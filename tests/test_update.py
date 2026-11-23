"""What an update run guarantees, checked against a fake upstream folder."""
import dataclasses
import os
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from conftest import GAMES, GAMES_FILE_ID, STATS, STATS_FILE, STATS_FILE_ID, catalog_of, stats_frame
from nfl_lake.pipeline import UpdateFailed, run_update, verify
from nfl_lake.source import LocalDirSource
from nfl_lake.store import LakeLocked


def clean(lake, dataset="stats", file=STATS_FILE) -> pl.DataFrame:
    return pl.read_parquet(lake.clean_path(dataset, file))


def published_targets(lake) -> int:
    """The value a test changes upstream, as the clean layer has it."""
    return clean(lake).filter(pl.col("player_id") == "00-001")["targets"].item()


def raw_files(lake, file_id=STATS_FILE_ID) -> list[Path]:
    return [lake.root / version["path"] for version in lake.raw_versions()[file_id]]


def test_first_run_publishes_and_an_identical_second_run_changes_nothing(upstream, lake):
    cat = catalog_of(stats=STATS)
    src = upstream.put("stats_player", STATS_FILE, stats_frame())

    dry = run_update(cat, upstream.source, lake, dry_run=True)
    assert dry.fetched == dry.built == [STATS_FILE_ID] and not lake.root.exists()  # a dry run writes nothing

    first = run_update(cat, upstream.source, lake)
    assert first.fetched == first.built == [STATS_FILE_ID]
    (raw,) = raw_files(lake)
    assert raw.read_bytes() == src.read_bytes()  # stored byte for byte
    assert_frame_equal(clean(lake), stats_frame().sort("player_id", "game_id"))  # same columns and types, sorted by key

    path = lake.clean_path("stats", STATS_FILE)
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    second = run_update(cat, upstream.source, lake)
    assert second.fetched == second.built == [] and second.unchanged == 1
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before and len(raw_files(lake)) == 1
    assert [run["status"] for run in lake.runs()] == ["ok", "ok"]


def test_correction_replaces_the_clean_file_and_history_decides_which_raw_versions_stay(upstream, lake):
    cat = catalog_of(stats=STATS, games={**GAMES, "history": "all"})  # stats keeps the default, latest
    games = lambda line: pl.DataFrame({"game_id": ["2024_01_BUF_MIA"], "spread_line": [line]})
    upstream.put("stats_player", STATS_FILE, stats_frame(targets=8))
    upstream.put("schedules", "games.parquet", games(3.0))
    run_update(cat, upstream.source, lake)
    (first_stats,) = raw_files(lake)

    upstream.put("stats_player", STATS_FILE, stats_frame(targets=10))
    upstream.put("schedules", "games.parquet", games(2.5))
    s = run_update(cat, upstream.source, lake)

    assert published_targets(lake) == 10 and clean(lake).height == 2  # replaced whole, not appended
    assert len(raw_files(lake)) == 1 and not first_stats.exists() and s.pruned == 1
    assert [pl.read_parquet(p)["spread_line"].item() for p in raw_files(lake, GAMES_FILE_ID)] == [3.0, 2.5]


def test_bad_file_fails_the_run_and_keeps_the_published_version(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame(8))
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(10), stats_frame(11)]))  # every key twice
    with pytest.raises(UpdateFailed) as failure:
        run_update(cat, upstream.source, lake)

    assert failure.value.published == 0 and published_targets(lake) == 8
    assert (lake.runs()[-1]["status"], lake.runs()[-1]["built"]) == ("failed", 0)
    assert not lake.lock_path.exists() and not lake.staging.exists()

    upstream.put("stats_player", STATS_FILE, stats_frame(10))  # upstream fixes it
    run_update(cat, upstream.source, lake)
    assert published_targets(lake) == 10


def test_one_bad_file_blocks_every_publish_in_the_run(upstream, lake):
    cat = catalog_of(stats=STATS, games=GAMES)
    games = lambda *ids: pl.DataFrame({"game_id": list(ids)})
    upstream.put("stats_player", STATS_FILE, stats_frame(8))
    upstream.put("schedules", "games.parquet", games("g1"))
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, stats_frame(10))        # a good change
    upstream.put("schedules", "games.parquet", games("g1", "g1"))    # a bad one
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)
    assert published_targets(lake) == 8

    upstream.put("schedules", "games.parquet", games("g1", "g2"))
    s = run_update(cat, upstream.source, lake)
    assert s.fetched == [GAMES_FILE_ID]  # the good file was already stored by the failed run
    assert sorted(s.built) == [GAMES_FILE_ID, STATS_FILE_ID] and published_targets(lake) == 10


def test_failure_while_publishing_is_counted_and_the_next_run_finishes(upstream, lake):
    cat = catalog_of(stats=STATS, games=GAMES)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    in_the_way = lake.clean_path("games", "games.parquet")
    in_the_way.mkdir(parents=True)  # a folder where the second clean file belongs, so that file cannot be moved in

    with pytest.raises(UpdateFailed) as failure:
        run_update(cat, upstream.source, lake)
    assert failure.value.published == 1 and clean(lake).height == 2
    assert list(lake.load_state()) == [STATS_FILE_ID]  # the record matches what is in clean/

    in_the_way.rmdir()
    s = run_update(cat, upstream.source, lake)
    assert s.fetched == [] and s.built == [GAMES_FILE_ID] and len(lake.load_state()) == 2


def test_rows_with_no_key_are_set_aside_only_when_the_catalog_allows_it(upstream, lake):
    def frame(*missing):  # 200 rows, of which `missing` have no player id
        ids = [*missing, *(f"p{i:03}" for i in range(200 - len(missing)))]
        return pl.DataFrame({"player_id": ids, "game_id": ["g1"] * 200})

    allowing = {**STATS, "reject_null_keys": True}
    rejects = lake.rejects_path("stats", STATS_FILE)
    upstream.put("stats_player", STATS_FILE, frame(None, ""))  # a null id and an empty one: 1% of the rows

    with pytest.raises(UpdateFailed):
        run_update(catalog_of(stats=STATS), upstream.source, lake)
    s = run_update(catalog_of(stats=allowing), upstream.source, lake)
    assert s.rejected == {STATS_FILE_ID: 2} and clean(lake).height == 198 and pl.read_parquet(rejects).height == 2

    upstream.put("stats_player", STATS_FILE, frame(None, "", None))  # 1.5%, above the default limit of 1%
    with pytest.raises(UpdateFailed):
        run_update(catalog_of(stats=allowing), upstream.source, lake)
    run_update(catalog_of(stats={**allowing, "max_reject_share": 0.02}), upstream.source, lake)
    assert clean(lake).height == 197

    upstream.put("stats_player", STATS_FILE, frame())  # upstream fills the ids in
    run_update(catalog_of(stats=allowing), upstream.source, lake)
    assert clean(lake).height == 200 and not rejects.exists()


def test_keep_last_by_resolves_a_repeated_key(upstream, lake):
    spec = {"release": "injuries", "file": "injuries_{season}.parquet", "min_season": 2009,
            "key": ["week", "gsis_id"], "keep_last_by": "date_modified"}
    df = pl.DataFrame({"week": [15, 15, 15], "gsis_id": ["a", "a", "b"],
                       "report_status": ["Out", "Questionable", "Out"],
                       "date_modified": ["2024-12-15T14:17", "2024-12-15T03:34", "2024-12-14T20:00"]})
    upstream.put("injuries", "injuries_2024.parquet", df)
    s = run_update(catalog_of(injuries=spec), upstream.source, lake)

    out = clean(lake, "injuries", "injuries_2024.parquet")
    assert s.deduped == {"injuries/injuries_2024.parquet": 1}
    assert out.filter(pl.col("gsis_id") == "a")["report_status"].item() == "Out"  # the later report

    # without the ordering column there is nothing to choose by, so the same repeat fails the run
    upstream.put("injuries", "injuries_2025.parquet", df.drop("date_modified"))
    with pytest.raises(UpdateFailed):
        run_update(catalog_of(injuries=spec), upstream.source, lake)


def test_file_that_lost_more_rows_than_the_limit_is_refused(upstream, lake):
    cat = catalog_of(stats=STATS)
    rows = lambda n: pl.DataFrame({"player_id": [f"p{i:03}" for i in range(n)], "game_id": ["g1"] * n})
    upstream.put("stats_player", STATS_FILE, rows(100))
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, rows(97))  # 3% fewer; the default limit is 2%
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)
    assert clean(lake).height == 100

    upstream.put("stats_player", STATS_FILE, rows(98))  # a stat correction can remove a row or two
    run_update(cat, upstream.source, lake)
    assert clean(lake).height == 98

    upstream.put("stats_player", STATS_FILE, rows(49))
    run_update(catalog_of(stats={**STATS, "max_row_drop": 0.5}), upstream.source, lake)  # the limit is set per dataset
    assert clean(lake).height == 49


@pytest.mark.parametrize("frame", [stats_frame().drop("game_id"), stats_frame().clear()], ids=["no key column", "no rows"])
def test_file_without_a_key_column_or_without_rows_is_refused(upstream, lake, frame):
    upstream.put("stats_player", STATS_FILE, frame)
    with pytest.raises(UpdateFailed):
        run_update(catalog_of(stats=STATS), upstream.source, lake)
    assert not lake.clean.exists()


def test_schema_change_is_refused_until_accepted(upstream, lake):
    cat = catalog_of(stats=STATS)
    with_epa = stats_frame().with_columns(pl.lit(1.5).alias("epa"))
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, with_epa)  # a new column is fine
    run_update(cat, upstream.source, lake)
    assert "epa" in clean(lake).columns

    upstream.put("stats_player", STATS_FILE, stats_frame())  # the column is gone again
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, with_epa.with_columns(pl.col("targets").cast(pl.Float64)))  # only a type changes
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)
    assert clean(lake).schema["targets"] == pl.Int64

    run_update(cat, upstream.source, lake, accept_schema_change=True)
    assert clean(lake).schema["targets"] == pl.Float64


def test_missing_or_deselected_upstream_files_are_errors_not_skips(upstream, lake):
    cat = catalog_of(stats=STATS)  # min_season 2020
    upstream.put("other", "x.parquet", stats_frame())  # the release is not there
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)

    upstream.put("stats_player", "renamed_2024.parquet", stats_frame())  # the release is, with no matching file
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)

    for season in (2020, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())
    run_update(cat, upstream.source, lake)

    with pytest.raises(UpdateFailed):  # the catalog stops selecting a file that is published
        run_update(catalog_of(stats={**STATS, "min_season": 2022}), upstream.source, lake)

    upstream.remove("stats_player", STATS_FILE)  # upstream stops listing one
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)


class Truncating(LocalDirSource):
    def fetch(self, asset, dest):
        data = (self.root / asset.release / asset.name).read_bytes()
        Path(dest).write_bytes(data[: len(data) // 2])


class BitFlipping(LocalDirSource):
    def fetch(self, asset, dest):
        data = bytearray((self.root / asset.release / asset.name).read_bytes())
        data[64] ^= 0xFF  # inside the first column's data; the size and the Parquet footer are untouched
        Path(dest).write_bytes(bytes(data))


class NoChecksums(LocalDirSource):
    """An upstream that publishes no checksum for its files."""

    def list_assets(self):
        return {release: {name: dataclasses.replace(asset, sha256=None) for name, asset in files.items()}
                for release, files in super().list_assets().items()}


@pytest.mark.parametrize("source", [Truncating, BitFlipping])
def test_damaged_download_is_refused(upstream, lake, source):
    rows = pl.DataFrame({"player_id": [f"p{i:04}" for i in range(2000)], "game_id": ["g1"] * 2000})
    upstream.put("stats_player", STATS_FILE, rows)
    with pytest.raises(UpdateFailed):
        run_update(catalog_of(stats=STATS), source(upstream.root), lake)
    assert lake.raw_versions() == {} and not lake.clean.exists()


def test_file_that_is_not_parquet_is_refused(upstream, lake):
    path = upstream.root / "stats_player" / STATS_FILE
    path.parent.mkdir(parents=True)
    path.write_text("<html>rate limited</html>")
    with pytest.raises(UpdateFailed):
        run_update(catalog_of(stats=STATS), upstream.source, lake)
    assert lake.raw_versions() == {}


def test_without_an_upstream_checksum_size_and_update_time_decide_what_is_fetched(upstream, lake):
    cat, source = catalog_of(stats=STATS), NoChecksums(upstream.root)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, source, lake)
    assert run_update(cat, source, lake).fetched == []

    more_rows = pl.concat([stats_frame(), stats_frame().with_columns(pl.lit("2024_02_BUF_NYJ").alias("game_id"))])
    path = upstream.put("stats_player", STATS_FILE, more_rows)  # a different size
    assert run_update(cat, source, lake).fetched == [STATS_FILE_ID]

    later = path.stat().st_mtime + 60
    os.utime(path, (later, later))  # the same bytes with a later upstream time
    assert run_update(cat, source, lake).fetched == [STATS_FILE_ID]


def test_earlier_file_put_back_upstream_keeps_the_raw_file_the_record_points_at(upstream, lake):
    cat = catalog_of(stats=STATS)
    path = upstream.put("stats_player", STATS_FILE, stats_frame(8))
    first = path.read_bytes()
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(10), stats_frame(11)]))  # refused
    with pytest.raises(UpdateFailed):
        run_update(cat, upstream.source, lake)

    path.write_bytes(first)
    later = path.stat().st_mtime + 60
    os.utime(path, (later, later))  # the same bytes as the published version, with a new upstream time
    run_update(cat, upstream.source, lake)
    assert verify(cat, lake, deep=True) == (1, [])


def test_seasons_before_min_season_or_outside_the_filter_are_not_built(upstream, lake):
    cat = catalog_of(stats=STATS)  # min_season 2020
    for season in (2019, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())

    assert run_update(cat, upstream.source, lake, seasons={2025}).built == ["stats/stats_player_week_2025.parquet"]
    assert run_update(cat, upstream.source, lake).built == [STATS_FILE_ID]  # 2025 is already published
    assert not lake.clean_path("stats", "stats_player_week_2019.parquet").exists()


def test_second_update_cannot_start_while_one_holds_the_lock(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    with lake.lock():
        with pytest.raises(LakeLocked):
            run_update(catalog_of(stats=STATS), upstream.source, lake)
    run_update(catalog_of(stats=STATS), upstream.source, lake)  # released on exit


def test_changing_a_dataset_rule_rebuilds_without_fetching(upstream, lake):
    df = pl.DataFrame({"player_id": ["a", "a"], "game_id": ["g1", "g2"], "team": ["BUF", "BUF"]})
    upstream.put("stats_player", STATS_FILE, df)
    run_update(catalog_of(stats=STATS), upstream.source, lake)

    rekeyed = catalog_of(stats={**STATS, "key": ["player_id", "game_id", "team"]})
    for dry_run in (True, False):
        s = run_update(rekeyed, upstream.source, lake, dry_run=dry_run)
        assert s.fetched == [] and s.built == [STATS_FILE_ID]
    assert run_update(rekeyed, upstream.source, lake).built == []


def test_verify_notices_damage_to_published_files(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, upstream.source, lake)
    assert verify(cat, lake, deep=True) == (1, [])

    path = lake.clean_path("stats", STATS_FILE)
    good = path.read_bytes()
    damaged_copies = [
        stats_frame().with_columns(pl.lit("00-001").alias("player_id")),         # the two rows share a key
        stats_frame().with_columns(pl.Series("player_id", ["00-002", ""])),      # an empty id is a missing id
        pl.concat([stats_frame(), stats_frame().with_columns(pl.lit("g2").alias("game_id"))]),  # more rows than published
    ]
    for frame in damaged_copies:
        frame.write_parquet(path)
        assert verify(cat, lake)[1] != []
    path.write_bytes(b"not parquet")
    assert verify(cat, lake)[1] != []
    path.unlink()
    assert verify(cat, lake)[1] != []

    path.write_bytes(good)
    (raw,) = raw_files(lake)
    raw.write_bytes(b"tampered")
    assert verify(cat, lake) == (1, []) and verify(cat, lake, deep=True)[1] != []  # only a deep check reads raw files
    raw.unlink()
    assert verify(cat, lake, deep=True)[1] != []


def test_verify_notices_a_changed_contract_and_a_dataset_dropped_from_the_catalog(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    run_update(catalog_of(stats=STATS, games=GAMES), upstream.source, lake)

    rekeyed = catalog_of(stats={**STATS, "key": ["player_id"]}, games=GAMES)
    checked, problems = verify(rekeyed, lake)
    assert checked == 2 and len(problems) == 1

    without_games = catalog_of(stats=STATS)
    checked, problems = verify(without_games, lake)
    assert checked == 1 and len(problems) == 1
    assert verify(without_games, lake, datasets=["stats"]) == (1, [])  # asked about stats only
