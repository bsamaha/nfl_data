from pathlib import Path

import pytest
from conftest import STATS_DATASET_SPEC, build_catalog
from pydantic import ValidationError

from nfl_lake.catalog import load_catalog

SINGLE_FILE_SPEC = {"release": "r", "file": "f.parquet", "key": ["a"]}


def test_repo_catalog_loads_and_no_two_datasets_read_the_same_file():
    catalog = load_catalog(Path(__file__).parents[1] / "catalog" / "datasets.yml")
    upstream_sources = [(dataset.spec.release, dataset.spec.file) for dataset in catalog.select_datasets()]
    assert len(set(upstream_sources)) == len(upstream_sources), "two datasets read the same upstream file"


@pytest.mark.parametrize(
    "dataset_name, dataset_spec",
    [
        ("stats", {**STATS_DATASET_SPEC, "partitions": ["season"]}),
        ("stats", {**SINGLE_FILE_SPEC, "file": "f_{season}.parquet"}),
        ("stats", {**SINGLE_FILE_SPEC, "min_season": 2020}),
        ("stats", {**STATS_DATASET_SPEC, "file": "stats_{season}_{season}.parquet"}),
        ("stats", {**STATS_DATASET_SPEC, "key": []}),
        ("stats", {**STATS_DATASET_SPEC, "key": ["player_id", "player_id"]}),
        ("Player_stats", STATS_DATASET_SPEC),
        ("player-stats", STATS_DATASET_SPEC),
        ("2024_stats", STATS_DATASET_SPEC),
    ],
    ids=[
        "unknown setting",
        "season file without min_season",
        "min_season without a season file",
        "two season markers",
        "empty key",
        "key column twice",
        "upper case name",
        "hyphen in name",
        "name starts with a digit",
    ],
)
def test_invalid_entry_is_rejected(dataset_name, dataset_spec):
    build_catalog(stats=STATS_DATASET_SPEC, one=SINGLE_FILE_SPEC)  # the two entries the cases start from are valid
    with pytest.raises(ValidationError):
        build_catalog(**{dataset_name: dataset_spec})


def test_file_matching_is_exact():
    dataset = build_catalog(stats=STATS_DATASET_SPEC).get_dataset("stats")
    assert dataset.match_asset_filename("stats_player_week_2024.parquet") == (True, 2024)
    assert dataset.match_asset_filename("stats_player_week_2024.csv") == (False, None)
    assert dataset.match_asset_filename("old_stats_player_week_2024.parquet") == (False, None)


def test_select_takes_none_for_all_and_refuses_unknown_names():
    catalog = build_catalog(stats=STATS_DATASET_SPEC, one=SINGLE_FILE_SPEC)
    assert [dataset.name for dataset in catalog.select_datasets()] == ["stats", "one"]
    assert [dataset.name for dataset in catalog.select_datasets(["one"])] == ["one"]
    assert catalog.select_datasets([]) == []  # an empty list is no dataset, not every dataset
    with pytest.raises(ValueError):
        catalog.select_datasets(["nope"])
