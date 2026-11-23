from pathlib import Path

import pytest
from pydantic import ValidationError

from conftest import STATS, catalog_of
from nfl_lake.catalog import load_catalog

ONE_FILE = {"release": "r", "file": "f.parquet", "key": ["a"]}


def test_repo_catalog_loads_and_no_two_datasets_read_the_same_file():
    cat = load_catalog(Path(__file__).parents[1] / "catalog" / "datasets.yml")
    sources = [(d.spec.release, d.spec.file) for d in cat.select()]
    assert len(set(sources)) == len(sources), "two datasets read the same upstream file"


@pytest.mark.parametrize("name, spec", [
    ("stats", {**STATS, "partitions": ["season"]}),
    ("stats", {**ONE_FILE, "file": "f_{season}.parquet"}),
    ("stats", {**ONE_FILE, "min_season": 2020}),
    ("stats", {**STATS, "file": "stats_{season}_{season}.parquet"}),
    ("stats", {**STATS, "key": []}),
    ("stats", {**STATS, "key": ["player_id", "player_id"]}),
    ("Player_stats", STATS),
    ("player-stats", STATS),
    ("2024_stats", STATS),
], ids=["unknown setting", "season file without min_season", "min_season without a season file", "two season markers",
        "empty key", "key column twice", "upper case name", "hyphen in name", "name starts with a digit"])
def test_invalid_entry_is_rejected(name, spec):
    catalog_of(stats=STATS, one=ONE_FILE)  # the two entries the cases start from are valid
    with pytest.raises(ValidationError):
        catalog_of(**{name: spec})


def test_file_matching_is_exact():
    d = catalog_of(stats=STATS).dataset("stats")
    assert d.match("stats_player_week_2024.parquet") == (True, 2024)
    assert d.match("stats_player_week_2024.csv") == (False, None)
    assert d.match("old_stats_player_week_2024.parquet") == (False, None)


def test_select_takes_none_for_all_and_refuses_unknown_names():
    cat = catalog_of(stats=STATS, one=ONE_FILE)
    assert [d.name for d in cat.select()] == ["stats", "one"]
    assert [d.name for d in cat.select(["one"])] == ["one"]
    assert cat.select([]) == []  # an empty list is no dataset, not every dataset
    with pytest.raises(ValueError):
        cat.select(["nope"])
