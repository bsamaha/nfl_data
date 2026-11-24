"""Properties of the parts that only compute: the key rules, the --seasons option and upstream file matching.

Hypothesis generates the inputs, so each test holds for every case it can build and not for a list someone
thought of. What a whole run does with these parts is in test_update.py and test_cli.py.
"""

import argparse

import polars as pl
import pytest
from conftest import build_catalog
from hypothesis import given
from hypothesis import strategies as st

from nfl_lake.cli import parse_season_filter
from nfl_lake.contract import ContractError, build_clean, count_key_violations

KEY_COLUMNS = ["player_id", "game_id"]
# Few distinct values, so that keys repeat. An empty string is a missing key, as a null is.
player_ids = st.sampled_from(["a", "b", "c", "", None])
game_ids = st.sampled_from(["g1", "g2", None])
modified_times = st.none() | st.integers(0, 3)


def raw_rows(max_size: int):
    return st.lists(st.tuples(player_ids, game_ids, modified_times), max_size=max_size)


def make_raw_frame(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": [row[0] for row in rows],
            "game_id": [row[1] for row in rows],
            "modified": [row[2] for row in rows],
            "row": list(range(len(rows))),  # which raw row a clean or set-aside row came from
        },
        schema={"player_id": pl.String, "game_id": pl.String, "modified": pl.Int64, "row": pl.Int64},
    )


def has_key(row: tuple) -> bool:
    return bool(row[0]) and bool(row[1])


def make_dataset(**rules):
    return build_catalog(stats={"release": "r", "file": "f.parquet", "key": KEY_COLUMNS, **rules}).get_dataset("stats")


@given(rows=raw_rows(max_size=25), max_reject_share=st.sampled_from([0.0, 0.2, 0.5, 1.0]))
def test_clean_file_has_each_key_once_and_every_raw_row_is_accounted_for(rows, max_reject_share):
    dataset = make_dataset(reject_null_keys=True, keep_last_by="modified", max_reject_share=max_reject_share)
    keyed = {index for index, row in enumerate(rows) if has_key(row)}
    without_key = len(rows) - len(keyed)
    if not keyed or without_key / len(rows) > max_reject_share:  # nothing to publish, or too much to set aside
        with pytest.raises(ContractError):
            build_clean(make_raw_frame(rows), dataset, None, False)
        return

    result = build_clean(make_raw_frame(rows), dataset, None, False)

    clean_rows = result.clean_frame.rows(named=True)
    clean_keys = [(clean_row["player_id"], clean_row["game_id"]) for clean_row in clean_rows]
    assert count_key_violations(result.clean_frame, KEY_COLUMNS) == (0, 0)
    assert clean_keys == sorted({rows[index][:2] for index in keyed})  # every key once, in key order
    set_aside = [] if result.rejected_frame is None else result.rejected_frame["row"].to_list()
    assert sorted(set_aside) == [index for index in range(len(rows)) if index not in keyed]
    assert len(clean_rows) + len(set_aside) + result.deduped_row_count == len(rows)
    for clean_row, clean_key in zip(clean_rows, clean_keys, strict=True):
        assert rows[clean_row["row"]] == (*clean_key, clean_row["modified"])  # a raw row, unchanged
        times_for_key = [rows[index][2] for index in keyed if rows[index][:2] == clean_key]
        assert clean_row["modified"] == max((time for time in times_for_key if time is not None), default=None)


@given(rows=raw_rows(max_size=8))
def test_with_no_rule_for_them_a_missing_or_repeated_key_refuses_the_file(rows):
    keys = [row[:2] for row in rows]
    publishable = bool(rows) and all(has_key(row) for row in rows) and len(set(keys)) == len(keys)
    if not publishable:
        with pytest.raises(ContractError):
            build_clean(make_raw_frame(rows), make_dataset(), None, False)
        return

    result = build_clean(make_raw_frame(rows), make_dataset(), None, False)

    assert result.rejected_frame is None and result.deduped_row_count == 0
    assert sorted(result.clean_frame["row"].to_list()) == list(range(len(rows)))  # the same rows
    assert result.clean_frame.select(KEY_COLUMNS).rows() == sorted(keys)


seasons = st.integers(1900, 2999)
season_ranges = st.tuples(seasons, st.integers(0, 40)).map(lambda pair: (pair[0], min(pair[0] + pair[1], 2999)))


@given(parts=st.lists(seasons | season_ranges, min_size=1, max_size=5))
def test_seasons_option_reads_any_list_of_seasons_and_ranges(parts):
    option_text = ",".join(str(part) if isinstance(part, int) else f"{part[0]}-{part[1]}" for part in parts)
    expected_seasons: set[int] = set()
    for part in parts:
        expected_seasons |= {part} if isinstance(part, int) else set(range(part[0], part[1] + 1))

    assert parse_season_filter(option_text) == expected_seasons


@given(option_text=st.text(alphabet="0123456789,- x", max_size=20))
def test_seasons_option_gives_seasons_or_a_usage_error_whatever_is_typed(option_text):
    try:
        parsed_seasons = parse_season_filter(option_text)
    except argparse.ArgumentTypeError:
        return
    assert parsed_seasons and all(1900 <= season <= 2999 for season in parsed_seasons)


# Includes the characters a regular expression would read as syntax: in a file name they are only characters.
name_parts = st.text(alphabet="abcxyz019_.-+()[]$^*?", max_size=8)
extra_characters = st.text(alphabet="abc019._", min_size=1, max_size=3)


@given(prefix=name_parts, suffix=name_parts, season=st.integers(1000, 9999), extra=extra_characters)
def test_season_file_pattern_matches_the_files_it_describes_and_no_other_name(prefix, suffix, season, extra):
    file_pattern = f"{prefix}{{season}}{suffix}"
    dataset = build_catalog(
        stats={"release": "r", "file": file_pattern, "min_season": 2000, "key": ["k"]},
    ).get_dataset("stats")
    filename = f"{prefix}{season}{suffix}"

    assert dataset.match_asset_filename(filename) == (True, season)
    assert dataset.match_asset_filename(extra + filename) == (False, None)
    assert dataset.match_asset_filename(filename + extra) == (False, None)
    if prefix:  # one character of the name replaced by one the pattern does not have
        assert dataset.match_asset_filename(f"!{prefix[1:]}{season}{suffix}") == (False, None)
    if suffix:
        assert dataset.match_asset_filename(f"{prefix}{season}{suffix[:-1]}!") == (False, None)


@given(filename=name_parts.filter(lambda name: name not in ("", ".", "..")), extra=extra_characters)
def test_single_file_dataset_matches_its_own_name_only(filename, extra):
    dataset = build_catalog(stats={"release": "r", "file": filename, "key": ["k"]}).get_dataset("stats")

    assert dataset.match_asset_filename(filename) == (True, None)
    assert dataset.match_asset_filename(filename + extra) == (False, None)
    assert dataset.match_asset_filename(f"!{filename[1:]}") == (False, None)
