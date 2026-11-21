"""run_update, verify and status against a fake upstream folder: what a run guarantees."""
import dataclasses
import os
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from conftest import GAMES, GAMES_FILE_ID, STATS, STATS_FILE, STATS_FILE_ID, catalog_of, stats_frame
from nfl_lake.pipeline import UpdateFailed, run_update, status, verify
from nfl_lake.source import LocalDirSource, sha256_file
from nfl_lake.store import LakeLocked


def clean(lake, dataset="stats", file=STATS_FILE) -> pl.DataFrame:
    return pl.read_parquet(lake.clean_path(dataset, file))


def published_targets(lake) -> int:
    """The value a test changes upstream, as the clean layer has it."""
    return clean(lake).filter(pl.col("player_id") == "00-001")["targets"].item()


def test_first_run_stores_the_file_as_published_and_builds_the_clean_copy(upstream, lake):
    src = upstream.put("stats_player", STATS_FILE, stats_frame())
    s = run_update(catalog_of(stats=STATS), upstream.source, lake)

    assert s.fetched == [STATS_FILE_ID] and s.built == [STATS_FILE_ID]
    assert_frame_equal(clean(lake), stats_frame().sort("player_id", "game_id"))

    (version,) = lake.raw_versions()[STATS_FILE_ID]
    assert (lake.root / version["path"]).read_bytes() == src.read_bytes()  # byte for byte
    assert version["sha256"] == sha256_file(src) and version["rows"] == 2 and version["season"] == 2024

    run = lake.runs()[-1]
    assert run["status"] == "ok" and run["fetched"] == 1 and run["built"] == 1 and run["errors"] == []


def test_second_identical_run_fetches_and_rewrites_nothing(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    cat = catalog_of(stats=STATS)
    run_update(cat, upstream.source, lake)
    path = lake.clean_path("stats", STATS_FILE)
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    s = run_update(cat, upstream.source, lake)

    assert s.fetched == [] and s.built == [] and s.unchanged == 1
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert len(lake.raw_versions()[STATS_FILE_ID]) == 1
    assert [r["status"] for r in lake.runs()] == ["ok", "ok"]


def test_upstream_correction_appears_after_one_run(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame(targets=8))
    run_update(cat, upstream.source, lake)
    assert published_targets(lake) == 8

    upstream.put("stats_player", STATS_FILE, stats_frame(targets=10))
    s = run_update(cat, upstream.source, lake)

    assert s.fetched == [STATS_FILE_ID]
    assert published_targets(lake) == 10
    assert clean(lake).height == 2  # replaced, not appended


def test_history_setting_decides_how_many_raw_versions_stay(upstream, lake):
    cat = catalog_of(stats=STATS, games={**GAMES, "history": "all"})
    games = lambda line: pl.DataFrame({"game_id": ["2024_01_BUF_MIA"], "spread_line": [line]})
    upstream.put("stats_player", STATS_FILE, stats_frame(8))
    upstream.put("schedules", "games.parquet", games(3.0))
    run_update(cat, upstream.source, lake)
    first_stats = lake.root / lake.raw_versions()[STATS_FILE_ID][0]["path"]

    upstream.put("stats_player", STATS_FILE, stats_frame(10))
    upstream.put("schedules", "games.parquet", games(2.5))
    s = run_update(cat, upstream.source, lake)

    versions = lake.raw_versions()
    assert len(versions[STATS_FILE_ID]) == 1 and not first_stats.exists() and s.pruned == 1
    assert len(versions[GAMES_FILE_ID]) == 2
    assert all((lake.root / v["path"]).exists() for v in versions[GAMES_FILE_ID])
    lines = [pl.read_parquet(lake.root / v["path"])["spread_line"].item() for v in versions[GAMES_FILE_ID]]
    assert lines == [3.0, 2.5]  # the earlier line is still on disk


def test_repeated_key_fails_the_run_and_keeps_the_published_version(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame(8))
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(10), stats_frame(11)]))
    with pytest.raises(UpdateFailed) as failure:
        run_update(cat, upstream.source, lake)

    assert "repeat a key" in failure.value.errors[0] and STATS_FILE_ID in failure.value.errors[0]
    assert failure.value.published == 0
    assert published_targets(lake) == 8
    run = lake.runs()[-1]
    assert run["status"] == "failed" and run["built"] == 0 and "repeat a key" in run["errors"][0]
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
    assert sorted(s.built) == [GAMES_FILE_ID, STATS_FILE_ID]
    assert published_targets(lake) == 10


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


def test_failure_while_publishing_says_how_much_was_published_and_the_next_run_finishes(upstream, lake, monkeypatch, second_publish_fails):
    cat = catalog_of(stats=STATS, games=GAMES)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    with pytest.raises(UpdateFailed, match="PermissionError") as failure:
        run_update(cat, upstream.source, lake)

    assert failure.value.published == 1
    run = lake.runs()[-1]
    assert run["status"] == "failed" and run["built"] == 1
    assert len(lake.load_state()) == 1  # the record matches what is in clean/

    monkeypatch.undo()
    s = run_update(cat, upstream.source, lake)
    assert s.fetched == [] and len(s.built) == 1 and len(lake.load_state()) == 2


def test_failure_moving_the_rejects_file_leaves_the_clean_file_unpublished(upstream, lake, monkeypatch):
    cat = catalog_of(stats={**STATS, "reject_null_keys": True, "max_reject_share": 0.5})
    upstream.put("stats_player", STATS_FILE, pl.DataFrame({"player_id": ["a", None], "game_id": ["g1", "g1"], "targets": [1, 0]}))
    real_replace = os.replace

    def replace(src, dst):
        if lake.rejects in Path(dst).parents:
            raise PermissionError("the file is open in another program")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", replace)
    with pytest.raises(UpdateFailed, match="PermissionError") as failure:
        run_update(cat, upstream.source, lake)
    assert failure.value.published == 0 and not lake.clean_path("stats", STATS_FILE).exists()


def test_null_keys_are_set_aside_only_when_the_catalog_allows_it(upstream, lake):
    df = pl.DataFrame({"player_id": ["00-001", None, "", "00-002"], "game_id": ["g1"] * 4, "targets": [8, 0, 0, 3]})
    upstream.put("stats_player", STATS_FILE, df)

    with pytest.raises(UpdateFailed, match="null key"):
        run_update(catalog_of(stats=STATS), upstream.source, lake)
    with pytest.raises(UpdateFailed, match="above the 1.0% limit"):
        run_update(catalog_of(stats={**STATS, "reject_null_keys": True}), upstream.source, lake)

    s = run_update(catalog_of(stats={**STATS, "reject_null_keys": True, "max_reject_share": 0.5}), upstream.source, lake)
    assert s.rejected == {STATS_FILE_ID: 2}
    assert clean(lake)["player_id"].to_list() == ["00-001", "00-002"]
    assert pl.read_parquet(lake.rejects_path("stats", STATS_FILE)).height == 2


def test_rejects_file_is_removed_once_upstream_fills_in_the_key(upstream, lake):
    cat = catalog_of(stats={**STATS, "reject_null_keys": True, "max_reject_share": 0.5})
    rows = lambda second_id: pl.DataFrame({"player_id": ["a", second_id], "game_id": ["g1", "g1"], "targets": [1, 0]})
    upstream.put("stats_player", STATS_FILE, rows(None))
    run_update(cat, upstream.source, lake)
    rejects = lake.rejects_path("stats", STATS_FILE)
    assert rejects.exists()

    upstream.put("stats_player", STATS_FILE, rows("b"))
    run_update(cat, upstream.source, lake)
    assert not rejects.exists() and clean(lake).height == 2


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

    # without the ordering column there is nothing to choose by, so the same repeat is an error
    upstream.put("injuries", "injuries_2025.parquet", df.drop("date_modified"))
    with pytest.raises(UpdateFailed, match="repeat a key"):
        run_update(catalog_of(injuries=spec), upstream.source, lake)


def test_truncated_file_is_refused(upstream, lake):
    cat = catalog_of(stats=STATS)
    rows = lambda n: pl.DataFrame({"player_id": [f"p{i:03}" for i in range(n)], "game_id": ["g1"] * n})
    upstream.put("stats_player", STATS_FILE, rows(100))
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, rows(97))
    with pytest.raises(UpdateFailed, match="rows fell from 100 to 97"):
        run_update(cat, upstream.source, lake)
    assert clean(lake).height == 100

    upstream.put("stats_player", STATS_FILE, rows(98))  # a stat correction can remove a row
    run_update(cat, upstream.source, lake)
    assert clean(lake).height == 98

    upstream.put("stats_player", STATS_FILE, rows(49))
    run_update(catalog_of(stats={**STATS, "max_row_drop": 0.5}), upstream.source, lake)  # the limit is set per dataset
    assert clean(lake).height == 49


@pytest.mark.parametrize("frame, message", [
    (stats_frame().drop("game_id"), r"key column\(s\) missing from the file: game_id"),
    (stats_frame().clear(), "no rows left to publish"),
])
def test_file_without_a_key_column_or_without_rows_is_refused(upstream, lake, frame, message):
    upstream.put("stats_player", STATS_FILE, frame)
    with pytest.raises(UpdateFailed, match=message):
        run_update(catalog_of(stats=STATS), upstream.source, lake)


def test_schema_change_is_refused_until_accepted(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, stats_frame().with_columns(pl.lit(1.5).alias("epa")))
    s = run_update(cat, upstream.source, lake)  # a new column is fine
    assert s.notes == [f"{STATS_FILE_ID}: new column(s) epa"]

    upstream.put("stats_player", STATS_FILE, stats_frame())  # the new column is gone again
    with pytest.raises(UpdateFailed, match=r"schema changed \(removed: epa\)"):
        run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, stats_frame().with_columns(pl.col("targets").cast(pl.Float64)))
    with pytest.raises(UpdateFailed, match="schema changed") as failure:
        run_update(cat, upstream.source, lake)
    assert "removed: epa" in failure.value.errors[0] and "targets Int64 -> Float64" in failure.value.errors[0]
    assert clean(lake).schema["targets"] == pl.Int64

    s = run_update(cat, upstream.source, lake, accept_schema_change=True)
    assert clean(lake).schema["targets"] == pl.Float64 and any("accepted schema change" in n for n in s.notes)


def test_missing_upstream_files_are_errors_not_skips(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("other", "x.parquet", stats_frame())
    with pytest.raises(UpdateFailed, match="no release named 'stats_player'"):
        run_update(cat, upstream.source, lake)

    upstream.put("stats_player", "renamed_2024.parquet", stats_frame())
    with pytest.raises(UpdateFailed, match="no upstream file matches"):
        run_update(cat, upstream.source, lake)

    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("stats_player", "stats_player_week_2025.parquet", stats_frame())
    run_update(cat, upstream.source, lake)
    upstream.remove("stats_player", STATS_FILE)
    with pytest.raises(UpdateFailed, match=f"no longer lists {STATS_FILE}"):
        run_update(cat, upstream.source, lake)


def test_published_file_the_catalog_stops_selecting_is_an_error(upstream, lake):
    upstream.put("stats_player", "stats_player_week_2020.parquet", stats_frame())
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(catalog_of(stats=STATS), upstream.source, lake)  # min_season 2020

    with pytest.raises(UpdateFailed, match="stats_player_week_2020.parquet is published but the catalog no longer selects it") as failure:
        run_update(catalog_of(stats={**STATS, "min_season": 2022}), upstream.source, lake)
    assert "no longer lists" not in str(failure.value)  # upstream still has the file


def test_integer_columns_stay_integers(upstream, lake):
    df = pl.DataFrame({"player_id": ["a", "b"], "game_id": ["g1", "g1"],
                       "draft_number": pl.Series([12, None], dtype=pl.Int64), "week": pl.Series([1, 1], dtype=pl.Int32)})
    upstream.put("stats_player", STATS_FILE, df)
    run_update(catalog_of(stats=STATS), upstream.source, lake)
    assert clean(lake).schema == df.schema


class Truncating(LocalDirSource):
    def fetch(self, asset, dest):
        data = (self.root / asset.release / asset.name).read_bytes()
        Path(dest).write_bytes(data[: len(data) // 2])


class BitFlipping(LocalDirSource):
    def fetch(self, asset, dest):
        data = bytearray((self.root / asset.release / asset.name).read_bytes())
        data[len(data) // 2] ^= 0xFF
        Path(dest).write_bytes(bytes(data))


class NoChecksums(LocalDirSource):
    """An upstream that publishes no checksum, as GitHub does for some assets."""

    def list_assets(self):
        return {release: {name: dataclasses.replace(asset, sha256=None) for name, asset in files.items()}
                for release, files in super().list_assets().items()}


class Exploding(LocalDirSource):
    def list_assets(self):
        raise RuntimeError("boom")


@pytest.mark.parametrize("source, message", [(Truncating, "upstream lists"), (BitFlipping, "checksum differs")])
def test_damaged_download_is_refused(upstream, lake, source, message):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    with pytest.raises(UpdateFailed, match=message):
        run_update(catalog_of(stats=STATS), source(upstream.root), lake)
    assert lake.raw_versions() == {} and not lake.clean_path("stats", STATS_FILE).exists()


def test_file_that_is_not_parquet_is_refused(upstream, lake):
    path = upstream.root / "stats_player" / STATS_FILE
    path.parent.mkdir(parents=True)
    path.write_text("<html>rate limited</html>")
    with pytest.raises(UpdateFailed, match="not a readable Parquet file"):
        run_update(catalog_of(stats=STATS), upstream.source, lake)


def test_without_an_upstream_checksum_size_and_update_time_decide_what_is_fetched(upstream, lake):
    cat, source = catalog_of(stats=STATS), NoChecksums(upstream.root)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, source, lake)
    assert run_update(cat, source, lake).fetched == []

    more_rows = pl.concat([stats_frame(), stats_frame().with_columns(pl.lit("2024_02_BUF_NYJ").alias("game_id"))])
    path = upstream.put("stats_player", STATS_FILE, more_rows)  # a different size, whatever the clock says
    assert run_update(cat, source, lake).fetched == [STATS_FILE_ID]

    later = path.stat().st_mtime + 60
    os.utime(path, (later, later))  # the same bytes with a later upstream time
    assert run_update(cat, source, lake).fetched == [STATS_FILE_ID]


def test_raw_file_deleted_from_disk_is_fetched_again(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, upstream.source, lake)
    (lake.root / lake.raw_versions()[STATS_FILE_ID][0]["path"]).unlink()

    s = run_update(cat, upstream.source, lake)
    assert s.fetched == [STATS_FILE_ID] and len(lake.raw_versions()[STATS_FILE_ID]) == 1


def test_unexpected_error_is_recorded_as_a_failed_run(upstream, lake):
    with pytest.raises(UpdateFailed, match="RuntimeError: boom"):
        run_update(catalog_of(stats=STATS), Exploding(upstream.root), lake)
    assert lake.runs()[-1]["status"] == "failed" and not lake.lock_path.exists()


def test_seasons_before_min_season_or_outside_the_filter_are_not_built(upstream, lake):
    cat = catalog_of(stats=STATS)  # min_season 2020
    for season in (2019, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())

    s = run_update(cat, upstream.source, lake, seasons={2025})
    assert s.built == ["stats/stats_player_week_2025.parquet"]

    s = run_update(cat, upstream.source, lake)
    assert s.built == [STATS_FILE_ID]  # 2019 is before min_season, 2025 is already published
    assert not lake.clean_path("stats", "stats_player_week_2019.parquet").exists()


def test_dry_run_reports_and_writes_nothing(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    s = run_update(catalog_of(stats=STATS), upstream.source, lake, dry_run=True)
    assert s.fetched == [STATS_FILE_ID] and s.built == [STATS_FILE_ID] and s.bytes_fetched > 0
    assert not lake.root.exists()


def test_second_update_cannot_start_while_one_holds_the_lock(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    with lake.lock():
        with pytest.raises(LakeLocked, match=rf"\(pid {os.getpid()} since .*unlock"):
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


def test_verify_notices_damage_to_published_files(upstream, lake):
    cat = catalog_of(stats=STATS)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    run_update(cat, upstream.source, lake)
    assert verify(cat, lake, deep=True) == (1, [])

    path = lake.clean_path("stats", STATS_FILE)
    pl.concat([stats_frame(), stats_frame()]).write_parquet(path)
    _, problems = verify(cat, lake)
    assert any("4 rows but 2 were published" in p for p in problems) and any("repeating a key" in p for p in problems)

    path.write_bytes(b"not parquet")
    assert any("clean file cannot be read" in p for p in verify(cat, lake)[1])

    path.unlink()
    assert verify(cat, lake)[1] == [f"{STATS_FILE_ID}: clean file is missing"]

    raw = lake.root / lake.raw_versions()[STATS_FILE_ID][0]["path"]
    raw.write_bytes(b"tampered")
    assert any("no longer matches its recorded checksum" in p for p in verify(cat, lake, deep=True)[1])

    raw.unlink()
    assert any(f"raw file {lake.rel(raw)} is missing" in p for p in verify(cat, lake, deep=True)[1])


def test_verify_notices_a_changed_contract_and_a_dataset_dropped_from_the_catalog(upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    run_update(catalog_of(stats=STATS, games=GAMES), upstream.source, lake)

    rekeyed = catalog_of(stats={**STATS, "key": ["player_id"]}, games=GAMES)
    assert verify(rekeyed, lake) == (2, [f"{STATS_FILE_ID}: built under an older contract; run update to rebuild it"])

    checked, problems = verify(catalog_of(stats=STATS), lake)
    assert checked == 1 and len(problems) == 1 and "dataset games is not in the catalog" in problems[0]


def test_status_totals_what_is_published(upstream, lake):
    cat = catalog_of(stats={**STATS, "reject_null_keys": True, "max_reject_share": 0.5})
    assert status(cat, lake) == ([{"dataset": "stats", "files": 0, "seasons": "-", "rows": 0, "rejected": 0,
                                   "upstream": "-", "built": "-"}], None)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("stats_player", "stats_player_week_2025.parquet",
                 pl.DataFrame({"player_id": ["a", None], "game_id": ["g1", "g1"], "targets": [1, 0]}))
    run_update(cat, upstream.source, lake)

    (row,), last = status(cat, lake)
    assert (row["files"], row["seasons"], row["rows"], row["rejected"]) == (2, "2024-2025", 3, 1)
    assert last["status"] == "ok"
