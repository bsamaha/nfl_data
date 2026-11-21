from pathlib import Path

import pytest
from pydantic import ValidationError

from conftest import STATS, catalog_of
from nfl_lake.catalog import load_catalog


def test_repo_catalog_loads_and_no_two_datasets_read_the_same_file():
    cat = load_catalog(Path(__file__).parents[1] / "catalog" / "datasets.yml")
    sources = [(d.spec.release, d.spec.file) for d in cat.select()]
    assert len(set(sources)) == len(sources), "two datasets read the same upstream file"


def test_unknown_setting_is_rejected():
    with pytest.raises(ValidationError, match="partitions"):
        catalog_of(stats={**STATS, "partitions": ["season"]})


@pytest.mark.parametrize("name", ["Player_stats", "player-stats", "2024_stats"])
def test_dataset_name_must_be_lower_snake_case(name):
    with pytest.raises(ValidationError, match="lower_snake_case"):
        catalog_of(**{name: STATS})


def test_season_pattern_and_min_season_go_together():
    with pytest.raises(ValidationError, match="needs min_season"):
        catalog_of(stats={"release": "r", "file": "f_{season}.parquet", "key": ["a"]})
    with pytest.raises(ValidationError, match="only applies"):
        catalog_of(stats={"release": "r", "file": "f.parquet", "key": ["a"], "min_season": 2020})


def test_key_cannot_be_empty_or_repeat_a_column():
    with pytest.raises(ValidationError, match="too_short"):
        catalog_of(stats={**STATS, "key": []})
    with pytest.raises(ValidationError, match="twice"):
        catalog_of(stats={**STATS, "key": ["a", "a"]})


def test_file_matching_is_exact():
    d = catalog_of(stats=STATS).dataset("stats")
    assert d.match("stats_player_week_2024.parquet") == (True, 2024)
    assert d.match("stats_player_week_2024.csv") == (False, None)
    assert d.match("old_stats_player_week_2024.parquet") == (False, None)


def test_selecting_an_unknown_dataset_is_an_error():
    cat = catalog_of(stats=STATS)
    assert [d.name for d in cat.select()] == ["stats"]
    with pytest.raises(ValueError, match="not in the catalog: nope"):
        cat.select(["nope"])


def test_fingerprint_follows_the_rules_that_shape_the_clean_file():
    base = catalog_of(stats=STATS).dataset("stats").fingerprint
    assert catalog_of(stats={**STATS, "history": "all"}).dataset("stats").fingerprint == base
    assert catalog_of(stats={**STATS, "reject_null_keys": True}).dataset("stats").fingerprint != base
    assert catalog_of(stats={**STATS, "keep_last_by": "ts"}).dataset("stats").fingerprint != base
