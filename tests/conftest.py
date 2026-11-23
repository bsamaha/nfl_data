"""Shared fixtures: a fake nflverse on disk, an empty lake, and a catalog builder.

The tests check what a run did: which files are on disk, what was fetched and built, which exit code came back.
None of them matches the wording of a message, so each bad input has exactly one fault.
"""

from pathlib import Path

import polars as pl
import pytest

from nfl_lake.catalog import Catalog
from nfl_lake.source import LocalDirSource
from nfl_lake.store import LakeStore


class FakeUpstream:
    """A folder of <release>/<file> that a test writes to, changes and breaks."""

    def __init__(self, root_dir: Path):
        self.root_dir = root_dir

    def put(self, release_tag: str, filename: str, frame: pl.DataFrame) -> Path:
        asset_path = self.root_dir / release_tag / filename
        asset_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(asset_path)
        return asset_path

    def remove(self, release_tag: str, filename: str) -> None:
        (self.root_dir / release_tag / filename).unlink()

    @property
    def source(self) -> LocalDirSource:
        return LocalDirSource(self.root_dir)


@pytest.fixture
def upstream(tmp_path: Path) -> FakeUpstream:
    return FakeUpstream(tmp_path / "upstream")


@pytest.fixture
def lake(tmp_path: Path) -> LakeStore:
    return LakeStore(tmp_path / "lake")


def build_catalog(**datasets: dict) -> Catalog:
    return Catalog.model_validate({"source": {"repo": "example/repo"}, "datasets": datasets})


# A seasonal dataset shaped like nflverse player stats.
STATS_DATASET_SPEC = {
    "release": "stats_player",
    "file": "stats_player_week_{season}.parquet",
    "min_season": 2020,
    "key": ["player_id", "game_id"],
}
STATS_FILENAME = "stats_player_week_2024.parquet"
STATS_FILE_ID = f"stats/{STATS_FILENAME}"

# A single-file dataset shaped like the nflverse schedule.
GAMES_DATASET_SPEC = {"release": "schedules", "file": "games.parquet", "key": ["game_id"]}
GAMES_FILE_ID = "games/games.parquet"


def make_stats_frame(targets: int = 8) -> pl.DataFrame:
    """Two players in one game. The 32-bit and the nullable integer column are there so that comparing a clean
    file with this frame also checks that integer types survive."""
    return pl.DataFrame(
        {
            "player_id": ["00-002", "00-001"],
            "game_id": ["2024_01_BUF_MIA", "2024_01_BUF_MIA"],
            "targets": [3, targets],
            "week": pl.Series([1, 1], dtype=pl.Int32),
            "draft_number": pl.Series([12, None], dtype=pl.Int64),
        }
    )
