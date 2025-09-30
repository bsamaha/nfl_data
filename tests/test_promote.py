from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from src.config import DatasetConfig
from src.promote import discover_changed_partitions, promote_to_silver, write_bronze_and_collect


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    base = tmp_path / "lake"
    base.mkdir()
    (base / "bronze").mkdir()
    (base / "silver").mkdir()
    return base


@pytest.fixture
def dataset_cfg() -> DatasetConfig:
    return DatasetConfig(
        name="weekly",
        importer="weekly",
        years=None,
        partitions=["season", "week"],
        key=["season", "week", "player_id"],
        options={},
        enabled=True,
        sort_by=None,
        max_rows_per_file=None,
    )


def test_discover_changed_partitions_returns_partition_strings():
    df = pd.DataFrame({"season": [2024, 2024], "week": [1, 2]})

    parts = discover_changed_partitions(df, ["season", "week"])

    assert sorted(parts) == ["season=2024/week=1", "season=2024/week=2"]


def test_write_bronze_and_collect_writes_to_disk(tmp_root: Path, dataset_cfg: DatasetConfig):
    df = pd.DataFrame({
        "season": [2024, 2024],
        "week": [1, 1],
        "player_id": ["00-001", "00-002"],
    })

    changed, stats = write_bronze_and_collect(str(tmp_root), dataset_cfg, df)

    assert changed == ["season=2024/week=1"]
    part_dir = tmp_root / "bronze" / "weekly" / "season=2024" / "week=1"
    assert part_dir.exists()
    assert stats["season=2024/week=1"].row_count == 2


def _read_silver_partition(root: Path, dataset: str, partition: str) -> pl.DataFrame:
    path = root / "silver" / dataset
    if partition:
        path = path / partition
    return pl.scan_parquet(str(path), hive_partitioning=True).collect()


def test_promote_to_silver_merges_existing_and_new_bronze(tmp_root: Path, dataset_cfg: DatasetConfig):
    df_initial = pd.DataFrame(
        {
            "season": [2024],
            "week": [1],
            "player_id": ["00-001"],
            "recent_team": ["BUF"],
            "team": ["BUF"],
            "targets": [8],
            "ingested_at": ["2024-09-01T00:00:00Z"],
        }
    )

    changed, _ = write_bronze_and_collect(str(tmp_root), dataset_cfg, df_initial)
    stats = promote_to_silver(str(tmp_root), dataset_cfg, changed, no_validate=True)

    assert stats["season=2024/week=1"].row_count == 1

    silver_initial = _read_silver_partition(tmp_root, "weekly", "season=2024/week=1")
    assert silver_initial.height == 1
    assert silver_initial.select("targets").item() == 8
    assert silver_initial.select("team").item() == "BUF"

    df_updated = pd.DataFrame(
        {
            "season": [2024],
            "week": [1],
            "player_id": ["00-001"],
            "recent_team": ["BUF"],
            "team": ["BUF"],
            "targets": [10],
            "ingested_at": ["2024-09-02T00:00:00Z"],
        }
    )

    changed, _ = write_bronze_and_collect(str(tmp_root), dataset_cfg, df_updated)
    stats = promote_to_silver(str(tmp_root), dataset_cfg, changed, no_validate=True)

    assert stats["season=2024/week=1"].row_count == 1

    silver_updated = _read_silver_partition(tmp_root, "weekly", "season=2024/week=1")
    assert silver_updated.height == 1
    assert silver_updated.select("targets").item() == 10

