"""What an update run guarantees, checked against a fake upstream folder."""

import dataclasses
import hashlib
import os
from pathlib import Path

import polars as pl
import pytest
from conftest import (
    GAMES_DATASET_SPEC,
    GAMES_FILE_ID,
    STATS_DATASET_SPEC,
    STATS_FILE_ID,
    STATS_FILENAME,
    build_catalog,
    make_stats_frame,
)
from polars.testing import assert_frame_equal

from nfl_lake.pipeline import UpdateFailed, run_update, verify_lake
from nfl_lake.source import FetchResult, LocalDirSource, SourceError
from nfl_lake.store import LakeLocked


def read_clean_frame(lake, dataset_name="stats", filename=STATS_FILENAME) -> pl.DataFrame:
    return pl.read_parquet(lake.clean_file_path(dataset_name, filename))


def published_targets(lake) -> int:
    """The value a test changes upstream, as the clean layer has it."""
    return read_clean_frame(lake).filter(pl.col("player_id") == "00-001")["targets"].item()


def stored_raw_paths(lake, dataset_name="stats", filename=STATS_FILENAME) -> list[Path]:
    raw_folder = lake.raw_dir / dataset_name / Path(filename).stem
    return sorted(raw_folder.iterdir()) if raw_folder.is_dir() else []


def test_first_run_publishes_and_an_identical_second_run_changes_nothing(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())

    dry_summary = run_update(catalog, upstream.source, lake, dry_run=True)
    assert dry_summary.fetched_file_ids == dry_summary.rebuilt_file_ids == [STATS_FILE_ID]
    assert not lake.root_dir.exists()  # a dry run writes nothing

    first_summary = run_update(catalog, upstream.source, lake)
    assert first_summary.fetched_file_ids == first_summary.rebuilt_file_ids == [STATS_FILE_ID]
    assert not lake.raw_dir.exists()  # default history is latest: no raw copy is kept on disk
    # same columns and types, sorted by key
    assert_frame_equal(read_clean_frame(lake), make_stats_frame().sort("player_id", "game_id"))

    clean_file_path = lake.clean_file_path("stats", STATS_FILENAME)
    clean_before = (clean_file_path.read_bytes(), clean_file_path.stat().st_mtime_ns)
    second_summary = run_update(catalog, upstream.source, lake)
    assert second_summary.fetched_file_ids == second_summary.rebuilt_file_ids == []
    assert second_summary.unchanged_file_count == 1
    assert (clean_file_path.read_bytes(), clean_file_path.stat().st_mtime_ns) == clean_before
    assert [run["status"] for run in lake.load_run_records()] == ["ok", "ok"]


def test_correction_replaces_the_clean_file_and_history_decides_which_raw_versions_stay(upstream, lake):
    # stats keeps the default history, latest; games keeps every raw snapshot
    catalog = build_catalog(stats=STATS_DATASET_SPEC, games={**GAMES_DATASET_SPEC, "history": "all"})

    def make_games_frame(spread):
        return pl.DataFrame({"game_id": ["2024_01_BUF_MIA"], "spread_line": [spread]})

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=8))
    first_games_upstream = upstream.put("schedules", "games.parquet", make_games_frame(3.0))
    first_games_bytes = first_games_upstream.read_bytes()
    run_update(catalog, upstream.source, lake)

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=10))
    second_games_upstream = upstream.put("schedules", "games.parquet", make_games_frame(2.5))
    run_update(catalog, upstream.source, lake)

    assert published_targets(lake) == 10 and read_clean_frame(lake).height == 2  # replaced whole, not appended
    assert stored_raw_paths(lake, "stats", STATS_FILENAME) == []  # latest keeps no raw copy
    # Both versions are kept byte for byte. A set, because snapshots stored in the same second sort by checksum.
    games_raw_bytes = {path.read_bytes() for path in stored_raw_paths(lake, "games", "games.parquet")}
    assert games_raw_bytes == {first_games_bytes, second_games_upstream.read_bytes()}


def test_bad_file_fails_the_run_and_keeps_the_published_version(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(8))
    run_update(catalog, upstream.source, lake)
    (lake.staging_dir / "run_killed" / "raw").mkdir(parents=True)  # what a killed run leaves; the next run clears it

    upstream.put(
        "stats_player", STATS_FILENAME, pl.concat([make_stats_frame(10), make_stats_frame(11)])
    )  # duplicate keys
    with pytest.raises(UpdateFailed) as failure:
        run_update(catalog, upstream.source, lake)

    assert failure.value.published_file_count == 0 and published_targets(lake) == 8
    last_run = lake.load_run_records()[-1]
    assert (last_run["status"], last_run["built"]) == ("failed", 0)
    assert not lake.lock_path.exists() and not lake.staging_dir.exists()

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(10))  # upstream fixes it
    run_update(catalog, upstream.source, lake)
    assert published_targets(lake) == 10


def test_one_bad_file_blocks_its_dataset_while_other_datasets_still_publish(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC, games={**GAMES_DATASET_SPEC, "history": "all"})
    stats_2025_filename = "stats_player_week_2025.parquet"

    def make_games_frame(*game_ids):
        return pl.DataFrame({"game_id": list(game_ids)})

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(8))
    upstream.put("stats_player", stats_2025_filename, make_stats_frame(8))
    upstream.put("schedules", "games.parquet", make_games_frame("g1"))
    run_update(catalog, upstream.source, lake)

    # One broken season in stats blocks all of stats (including the good 2024 update), while games still publishes.
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(10))
    upstream.put("stats_player", stats_2025_filename, pl.concat([make_stats_frame(10), make_stats_frame(11)]))
    upstream.put("schedules", "games.parquet", make_games_frame("g1", "g2"))
    with pytest.raises(UpdateFailed) as first_failure:
        run_update(catalog, upstream.source, lake)
    assert first_failure.value.published_file_count == 1
    assert published_targets(lake) == 8  # neither season of stats published
    assert lake.load_run_records()[-1]["unchanged"] == 0  # and neither is recorded as unchanged
    assert read_clean_frame(lake, "games", "games.parquet").height == 2
    assert len(stored_raw_paths(lake, "games", "games.parquet")) == 2

    # Fixing stats while breaking games publishes both stats seasons and writes no new raw file for games.
    upstream.put("stats_player", stats_2025_filename, make_stats_frame(12))
    upstream.put("schedules", "games.parquet", make_games_frame("g1", "g1"))
    with pytest.raises(UpdateFailed) as second_failure:
        run_update(catalog, upstream.source, lake)
    assert second_failure.value.published_file_count == 2
    assert published_targets(lake) == 10
    assert len(stored_raw_paths(lake, "games", "games.parquet")) == 2  # failed dataset writes no raw snapshot

    # Fixing games finishes the run without re-fetching or rebuilding stats.
    upstream.put("schedules", "games.parquet", make_games_frame("g1", "g2", "g3"))
    summary = run_update(catalog, upstream.source, lake)
    assert summary.fetched_file_ids == summary.rebuilt_file_ids == [GAMES_FILE_ID]


def test_metadata_only_upstream_rebuild_skips_rewriting_clean_and_raw(upstream, lake):
    catalog = build_catalog(stats={**STATS_DATASET_SPEC, "history": "all", "reject_null_keys": True})
    player_ids = [None, *(f"p{index:03}" for index in range(199))]
    frame = pl.DataFrame({"player_id": player_ids, "game_id": ["g1"] * 200})

    upstream.put("stats_player", STATS_FILENAME, frame, metadata={"nflverse_timestamp": "2026-11-21 10:00:00 UTC"})
    run_update(catalog, upstream.source, lake)

    clean_path = lake.clean_file_path("stats", STATS_FILENAME)
    rejects_path = lake.rejects_file_path("stats", STATS_FILENAME)
    clean_mtime_before = clean_path.stat().st_mtime_ns
    rejects_mtime_before = rejects_path.stat().st_mtime_ns
    raw_paths_before = stored_raw_paths(lake)

    # Identical rows with a newer Parquet timestamp change the raw checksum without changing clean or rejected rows.
    upstream.put("stats_player", STATS_FILENAME, frame, metadata={"nflverse_timestamp": "2026-11-22 10:00:00 UTC"})
    refresh_summary = run_update(catalog, upstream.source, lake)
    assert refresh_summary.fetched_file_ids == [STATS_FILE_ID]
    assert refresh_summary.rebuilt_file_ids == []
    assert refresh_summary.unchanged_file_count == 1
    assert clean_path.stat().st_mtime_ns == clean_mtime_before
    assert rejects_path.stat().st_mtime_ns == rejects_mtime_before
    assert stored_raw_paths(lake) == raw_paths_before
    assert lake.load_clean_state()[STATS_FILE_ID]["upstream_built_at"] == "2026-11-22 10:00:00 UTC"
    assert verify_lake(catalog, lake, deep=True) == (1, [])

    # Because manifest metadata was refreshed, the next run fetches nothing.
    followup_summary = run_update(catalog, upstream.source, lake)
    assert followup_summary.fetched_file_ids == followup_summary.rebuilt_file_ids == []


def test_failure_while_publishing_is_counted_and_the_next_run_finishes(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    blocking_dir = lake.clean_file_path("stats", STATS_FILENAME)
    blocking_dir.mkdir(parents=True)  # a folder where the first clean file belongs, so that file cannot be moved in

    with pytest.raises(UpdateFailed) as failure:
        run_update(catalog, upstream.source, lake)
    # the dataset after the one that failed is still published
    assert failure.value.published_file_count == 1 and read_clean_frame(lake, "games", "games.parquet").height == 1
    assert list(lake.load_clean_state()) == [GAMES_FILE_ID]  # the record matches what is in clean/

    blocking_dir.rmdir()
    summary = run_update(catalog, upstream.source, lake)
    assert summary.fetched_file_ids == [STATS_FILE_ID] and summary.rebuilt_file_ids == [STATS_FILE_ID]
    assert len(lake.load_clean_state()) == 2


def test_rows_with_no_key_are_set_aside_only_when_the_catalog_allows_it(upstream, lake):
    def make_frame_with_missing_ids(*missing_ids):  # 200 rows, of which `missing_ids` have no player id
        player_ids = [*missing_ids, *(f"p{index:03}" for index in range(200 - len(missing_ids)))]
        return pl.DataFrame({"player_id": player_ids, "game_id": ["g1"] * 200})

    allowing_null_keys = {**STATS_DATASET_SPEC, "reject_null_keys": True}
    rejects_file_path = lake.rejects_file_path("stats", STATS_FILENAME)
    upstream.put("stats_player", STATS_FILENAME, make_frame_with_missing_ids(None, ""))  # 1% of the rows

    with pytest.raises(UpdateFailed):
        run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)
    summary = run_update(build_catalog(stats=allowing_null_keys), upstream.source, lake)
    assert summary.rejected_rows_by_file == {STATS_FILE_ID: 2}
    assert read_clean_frame(lake).height == 198 and pl.read_parquet(rejects_file_path).height == 2

    # The set-aside file is published data too: a changed one is noticed, and a missing one is noticed and rebuilt.
    catalog = build_catalog(stats=allowing_null_keys)
    pl.read_parquet(rejects_file_path).head(1).write_parquet(rejects_file_path)
    assert verify_lake(catalog, lake) == (1, []) and verify_lake(catalog, lake, deep=True).errors != []
    rejects_file_path.unlink()
    assert verify_lake(catalog, lake).errors != []
    run_update(catalog, upstream.source, lake)
    assert pl.read_parquet(rejects_file_path).height == 2 and verify_lake(catalog, lake, deep=True) == (1, [])

    upstream.put("stats_player", STATS_FILENAME, make_frame_with_missing_ids(None, "", None))  # 1.5%, above 1% default
    with pytest.raises(UpdateFailed):
        run_update(build_catalog(stats=allowing_null_keys), upstream.source, lake)
    run_update(build_catalog(stats={**allowing_null_keys, "max_reject_share": 0.02}), upstream.source, lake)
    assert read_clean_frame(lake).height == 197

    upstream.put("stats_player", STATS_FILENAME, make_frame_with_missing_ids())  # upstream fills the ids in
    run_update(build_catalog(stats=allowing_null_keys), upstream.source, lake)
    assert read_clean_frame(lake).height == 200 and not rejects_file_path.exists()


def test_keep_last_by_resolves_a_repeated_key(upstream, lake):
    injuries_spec = {
        "release": "injuries",
        "file": "injuries_{season}.parquet",
        "min_season": 2009,
        "key": ["week", "gsis_id"],
        "keep_last_by": "date_modified",
        "history": "all",
    }
    injuries_frame = pl.DataFrame(
        {
            "week": [15, 15, 15],
            "gsis_id": ["a", "a", "b"],
            "report_status": ["Out", "Questionable", "Out"],
            "date_modified": ["2024-12-15T14:17", "2024-12-15T03:34", "2024-12-14T20:00"],
        }
    )
    upstream.put("injuries", "injuries_2024.parquet", injuries_frame)
    summary = run_update(build_catalog(injuries=injuries_spec), upstream.source, lake)

    clean_injuries = read_clean_frame(lake, "injuries", "injuries_2024.parquet")
    assert summary.deduped_rows_by_file == {"injuries/injuries_2024.parquet": 1}
    assert clean_injuries.filter(pl.col("gsis_id") == "a")["report_status"].item() == "Out"  # the later report

    # upstream drops the earlier report itself: the clean file comes out the same, but the raw file is a new version
    upstream.put("injuries", "injuries_2024.parquet", injuries_frame.filter(pl.col("report_status") != "Questionable"))
    run_update(build_catalog(injuries=injuries_spec), upstream.source, lake)
    assert len(stored_raw_paths(lake, "injuries", "injuries_2024.parquet")) == 2

    # without the ordering column there is nothing to choose by, so the same repeat fails the run
    upstream.put("injuries", "injuries_2025.parquet", injuries_frame.drop("date_modified"))
    with pytest.raises(UpdateFailed):
        run_update(build_catalog(injuries=injuries_spec), upstream.source, lake)


def test_file_that_lost_more_rows_than_the_limit_is_refused(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)

    def make_rows(row_count):
        return pl.DataFrame(
            {
                "player_id": [f"p{index:03}" for index in range(row_count)],
                "game_id": ["g1"] * row_count,
            }
        )

    upstream.put("stats_player", STATS_FILENAME, make_rows(100))
    run_update(catalog, upstream.source, lake)

    upstream.put("stats_player", STATS_FILENAME, make_rows(97))  # 3% fewer; the default limit is 2%
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)
    assert read_clean_frame(lake).height == 100

    upstream.put("stats_player", STATS_FILENAME, make_rows(98))  # a stat correction can remove a row or two
    run_update(catalog, upstream.source, lake)
    assert read_clean_frame(lake).height == 98

    upstream.put("stats_player", STATS_FILENAME, make_rows(49))  # half the rows: the limit is set per dataset
    run_update(build_catalog(stats={**STATS_DATASET_SPEC, "max_row_drop": 0.5}), upstream.source, lake)
    assert read_clean_frame(lake).height == 49


@pytest.mark.parametrize(
    "invalid_frame",
    [make_stats_frame().drop("game_id"), make_stats_frame().clear()],
    ids=["no key column", "no rows"],
)
def test_file_without_a_key_column_or_without_rows_is_refused(upstream, lake, invalid_frame):
    upstream.put("stats_player", STATS_FILENAME, invalid_frame)
    with pytest.raises(UpdateFailed):
        run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)
    assert not lake.clean_dir.exists()


def test_schema_change_is_refused_until_accepted(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)
    frame_with_epa = make_stats_frame().with_columns(pl.lit(1.5).alias("epa"))
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    run_update(catalog, upstream.source, lake)

    upstream.put("stats_player", STATS_FILENAME, frame_with_epa)  # a new column is fine
    run_update(catalog, upstream.source, lake)
    assert "epa" in read_clean_frame(lake).columns

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())  # the column is gone again
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)

    retyped_frame = frame_with_epa.with_columns(pl.col("targets").cast(pl.Float64))  # only a type changes
    upstream.put("stats_player", STATS_FILENAME, retyped_frame)
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)
    assert read_clean_frame(lake).schema["targets"] == pl.Int64

    run_update(catalog, upstream.source, lake, accept_schema_change=True)
    assert read_clean_frame(lake).schema["targets"] == pl.Float64


def test_missing_or_deselected_upstream_files_are_errors_not_skips(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)  # min_season 2020
    upstream.put("other", "x.parquet", make_stats_frame())  # the release is not there
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)

    # a dry run shows what the real run would still do for the other datasets, beside the error
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    dry_summary = run_update(
        build_catalog(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC), upstream.source, lake, dry_run=True
    )
    assert dry_summary.fetched_file_ids == [GAMES_FILE_ID] and len(dry_summary.plan_errors) == 1

    upstream.put("stats_player", "renamed_2024.parquet", make_stats_frame())  # the release is, with no matching file
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)

    for season in (2020, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", make_stats_frame())
    run_update(catalog, upstream.source, lake)

    with pytest.raises(UpdateFailed):  # the catalog stops selecting a file that is published
        run_update(build_catalog(stats={**STATS_DATASET_SPEC, "min_season": 2022}), upstream.source, lake)

    upstream.remove("stats_player", STATS_FILENAME)  # upstream stops listing one
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)


class TruncatingSource(LocalDirSource):
    def fetch(self, asset, dest_path):
        data = (self.root_dir / asset.release_tag / asset.filename).read_bytes()
        truncated = data[: len(data) // 2]
        Path(dest_path).write_bytes(truncated)
        return FetchResult(size_bytes=len(truncated), sha256=hashlib.sha256(truncated).hexdigest())


class BitFlippingSource(LocalDirSource):
    def fetch(self, asset, dest_path):
        data = bytearray((self.root_dir / asset.release_tag / asset.filename).read_bytes())
        data[64] ^= 0xFF  # inside the first column's data; the size and the Parquet footer are untouched
        corrupted = bytes(data)
        Path(dest_path).write_bytes(corrupted)
        return FetchResult(size_bytes=len(corrupted), sha256=hashlib.sha256(corrupted).hexdigest())


class NoChecksumsSource(LocalDirSource):
    """An upstream that publishes no checksum for its files."""

    def list_assets(self):
        return {
            release_tag: {
                filename: dataclasses.replace(asset, sha256=None) for filename, asset in release_assets.items()
            }
            for release_tag, release_assets in super().list_assets().items()
        }


@pytest.mark.parametrize("damaged_source_cls", [TruncatingSource, BitFlippingSource])
def test_damaged_download_is_refused(upstream, lake, damaged_source_cls):
    large_frame = pl.DataFrame({"player_id": [f"p{index:04}" for index in range(2000)], "game_id": ["g1"] * 2000})
    upstream.put("stats_player", STATS_FILENAME, large_frame)
    with pytest.raises(UpdateFailed):
        run_update(build_catalog(stats=STATS_DATASET_SPEC), damaged_source_cls(upstream.root_dir), lake)
    assert not lake.raw_dir.exists() and not lake.clean_dir.exists()


class StaleListingSource(LocalDirSource):
    """An upstream whose first `stale_listings` listings describe the files as they were when it was created."""

    def __init__(self, root_dir, stale_listings=1, listing_lag_seconds=0.0):
        super().__init__(root_dir)
        self.listing_at_creation = super().list_assets()
        self.stale_listings = stale_listings
        self.listing_lag_seconds = listing_lag_seconds
        self.listings_served = 0

    def list_assets(self):
        self.listings_served += 1
        return self.listing_at_creation if self.listings_served <= self.stale_listings else super().list_assets()


@pytest.mark.parametrize(
    "replacement, stale_listings, listing_lag_seconds",
    [
        (make_stats_frame(targets=10), 1, 0.0),
        (make_stats_frame(targets=10).head(1), 1, 0.0),
        (make_stats_frame(targets=10), 2, 0.01),
    ],
    ids=["other values", "another size", "listing catches up only after a wait"],
)
def test_file_replaced_upstream_after_the_listing_is_fetched_against_a_new_listing(
    upstream, lake, replacement, stale_listings, listing_lag_seconds
):
    catalog = build_catalog(stats={**STATS_DATASET_SPEC, "history": "all"})
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=8))
    source = StaleListingSource(upstream.root_dir, stale_listings, listing_lag_seconds)
    upstream_file_path = upstream.put("stats_player", STATS_FILENAME, replacement)  # replaced after the listing

    run_update(catalog, source, lake)

    assert source.listings_served == stale_listings + 1
    (raw_file_path,) = stored_raw_paths(lake)
    assert raw_file_path.read_bytes() == upstream_file_path.read_bytes()
    assert_frame_equal(read_clean_frame(lake), replacement.sort("player_id", "game_id"))


def test_listing_that_never_catches_up_costs_the_run_two_waits_in_all_not_two_per_dataset(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=8))
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    source = StaleListingSource(upstream.root_dir, stale_listings=99, listing_lag_seconds=0.01)
    upstream.put(
        "stats_player", STATS_FILENAME, make_stats_frame(targets=10).head(1)
    )  # both replaced after the listing
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g2"]}))

    with pytest.raises(UpdateFailed):
        run_update(catalog, source, lake)
    # stats lists again three times, waiting before the second and third; games once, because no wait is left
    assert source.listings_served == 1 + 3 + 1


def test_failed_second_listing_stops_only_its_own_dataset(upstream, lake):
    class SecondListingFails(StaleListingSource):
        def list_assets(self):
            if self.listings_served:
                raise SourceError("rate limit reached")
            return super().list_assets()

    catalog = build_catalog(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=8))
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    source = SecondListingFails(upstream.root_dir)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=10).head(1))  # replaced after the listing

    with pytest.raises(UpdateFailed):
        run_update(catalog, source, lake)
    assert list(lake.load_clean_state()) == [GAMES_FILE_ID]


def test_file_that_is_not_parquet_is_refused(upstream, lake):
    corrupt_path = upstream.root_dir / "stats_player" / STATS_FILENAME
    corrupt_path.parent.mkdir(parents=True)
    corrupt_path.write_text("<html>rate limited</html>")
    with pytest.raises(UpdateFailed):
        run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)
    assert not lake.raw_dir.exists() and not lake.clean_dir.exists()


def test_without_an_upstream_checksum_size_and_update_time_decide_what_is_fetched(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)
    source = NoChecksumsSource(upstream.root_dir)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    run_update(catalog, source, lake)
    assert run_update(catalog, source, lake).fetched_file_ids == []

    expanded_frame = pl.concat(
        [make_stats_frame(), make_stats_frame().with_columns(pl.lit("2024_02_BUF_NYJ").alias("game_id"))]
    )
    upstream_file_path = upstream.put("stats_player", STATS_FILENAME, expanded_frame)  # a different size
    assert run_update(catalog, source, lake).fetched_file_ids == [STATS_FILE_ID]

    later_mtime = upstream_file_path.stat().st_mtime + 60
    os.utime(upstream_file_path, (later_mtime, later_mtime))  # the same bytes with a later upstream time
    assert run_update(catalog, source, lake).fetched_file_ids == [STATS_FILE_ID]


def test_earlier_file_put_back_upstream_keeps_the_raw_file_the_record_points_at(upstream, lake):
    catalog = build_catalog(stats={**STATS_DATASET_SPEC, "history": "all"})
    upstream_file_path = upstream.put("stats_player", STATS_FILENAME, make_stats_frame(8))
    original_bytes = upstream_file_path.read_bytes()
    run_update(catalog, upstream.source, lake)

    upstream.put("stats_player", STATS_FILENAME, pl.concat([make_stats_frame(10), make_stats_frame(11)]))  # refused
    with pytest.raises(UpdateFailed):
        run_update(catalog, upstream.source, lake)

    upstream_file_path.write_bytes(original_bytes)
    later_mtime = upstream_file_path.stat().st_mtime + 60
    os.utime(upstream_file_path, (later_mtime, later_mtime))  # same bytes as published version, new upstream time
    run_update(catalog, upstream.source, lake)
    assert verify_lake(catalog, lake, deep=True) == (1, [])


def test_seasons_before_min_season_or_outside_the_filter_are_not_built(upstream, lake):
    catalog = build_catalog(stats=STATS_DATASET_SPEC)  # min_season 2020
    for season in (2019, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", make_stats_frame())

    assert run_update(catalog, upstream.source, lake, season_filter={2025}).rebuilt_file_ids == [
        "stats/stats_player_week_2025.parquet"
    ]
    assert run_update(catalog, upstream.source, lake).rebuilt_file_ids == [STATS_FILE_ID]  # 2025 is already published
    assert not lake.clean_file_path("stats", "stats_player_week_2019.parquet").exists()


def test_second_update_cannot_start_while_one_holds_the_lock(upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    with lake.acquire_lock(), pytest.raises(LakeLocked):
        run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)
    run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)  # released on exit


def test_changing_a_dataset_rule_rebuilds_from_raw_when_kept_and_refetches_otherwise(upstream, lake):
    frame = pl.DataFrame({"player_id": ["a", "a"], "game_id": ["g1", "g2"], "team": ["BUF", "BUF"]})
    games_frame = pl.DataFrame({"game_id": ["g1"], "season": [2024]})
    upstream.put("stats_player", STATS_FILENAME, frame)
    upstream.put("schedules", "games.parquet", games_frame)
    initial_catalog = build_catalog(stats=STATS_DATASET_SPEC, games={**GAMES_DATASET_SPEC, "history": "all"})
    run_update(initial_catalog, upstream.source, lake)

    rekeyed_catalog = build_catalog(
        stats={**STATS_DATASET_SPEC, "key": ["player_id", "game_id", "team"]},
        games={**GAMES_DATASET_SPEC, "history": "all", "key": ["game_id", "season"]},
    )
    for dry_run in (True, False):
        summary = run_update(rekeyed_catalog, upstream.source, lake, dry_run=dry_run)
        # games has a stored raw snapshot so it rebuilds without fetching; stats (history: latest) refetches
        assert summary.fetched_file_ids == [STATS_FILE_ID]
        assert sorted(summary.rebuilt_file_ids) == [GAMES_FILE_ID, STATS_FILE_ID]
    assert run_update(rekeyed_catalog, upstream.source, lake).rebuilt_file_ids == []


def test_verify_notices_damage_to_published_files(upstream, lake):
    catalog = build_catalog(stats={**STATS_DATASET_SPEC, "history": "all"})
    for targets in (5, 8):  # two versions, so two raw snapshots
        upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets))
        run_update(catalog, upstream.source, lake)
    assert verify_lake(catalog, lake, deep=True) == (1, [])

    clean_file_path = lake.clean_file_path("stats", STATS_FILENAME)
    valid_clean_bytes = clean_file_path.read_bytes()
    damaged_frames = [
        make_stats_frame().with_columns(pl.lit("00-001").alias("player_id")),  # the two rows share a key
        make_stats_frame().with_columns(pl.Series("player_id", ["00-002", ""])),  # an empty id is a missing id
        pl.concat([make_stats_frame(), make_stats_frame().with_columns(pl.lit("g2").alias("game_id"))]),  # extra rows
    ]
    for damaged_frame in damaged_frames:
        damaged_frame.write_parquet(clean_file_path)
        assert verify_lake(catalog, lake).errors != []
    clean_file_path.write_bytes(b"not parquet")
    assert verify_lake(catalog, lake).errors != []
    clean_file_path.unlink()
    assert verify_lake(catalog, lake).errors != []

    # Non-key column tampered: fast verify passes (key + rows match), --deep catches the clean checksum change.
    make_stats_frame(targets=99).sort("player_id", "game_id").write_parquet(clean_file_path)
    assert verify_lake(catalog, lake) == (1, []) and verify_lake(catalog, lake, deep=True).errors != []

    clean_file_path.write_bytes(valid_clean_bytes)
    for raw_file_path in stored_raw_paths(lake):  # only a deep check reads raw files, and it reads every snapshot
        intact_raw_bytes = raw_file_path.read_bytes()
        raw_file_path.write_bytes(b"tampered")
        assert verify_lake(catalog, lake) == (1, []) and verify_lake(catalog, lake, deep=True).errors != []
        raw_file_path.write_bytes(intact_raw_bytes)
    (raw_file_path.parent / "desktop.ini").write_text("a file the lake did not write is not a damaged snapshot")
    assert verify_lake(catalog, lake, deep=True) == (1, [])
    for raw_file_path in stored_raw_paths(lake):
        raw_file_path.unlink()
    assert verify_lake(catalog, lake, deep=True).errors != []  # the snapshot the clean file was built from is gone


def test_verify_notices_a_changed_contract_and_a_dataset_dropped_from_the_catalog(upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    run_update(build_catalog(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC), upstream.source, lake)

    rekeyed_catalog = build_catalog(stats={**STATS_DATASET_SPEC, "key": ["player_id"]}, games=GAMES_DATASET_SPEC)
    verification = verify_lake(rekeyed_catalog, lake)
    assert verification.checked_file_count == 2 and len(verification.errors) == 1

    catalog_without_games = build_catalog(stats=STATS_DATASET_SPEC)
    verification = verify_lake(catalog_without_games, lake)
    assert verification.checked_file_count == 1 and len(verification.errors) == 1
    assert verify_lake(catalog_without_games, lake, dataset_names=["stats"]) == (1, [])  # asked about stats only
