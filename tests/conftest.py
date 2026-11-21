"""Shared fixtures: a fake nflverse on disk, an empty lake, and a catalog builder."""
import os
from pathlib import Path

import polars as pl
import pytest

from nfl_lake.catalog import Catalog
from nfl_lake.source import LocalDirSource
from nfl_lake.store import Lake


class Upstream:
    """A folder of <release>/<file> that a test writes to, changes and breaks."""

    def __init__(self, root: Path):
        self.root = root

    def put(self, release: str, name: str, df: pl.DataFrame) -> Path:
        path = self.root / release / name
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
        return path

    def remove(self, release: str, name: str) -> None:
        (self.root / release / name).unlink()

    @property
    def source(self) -> LocalDirSource:
        return LocalDirSource(self.root)


@pytest.fixture
def upstream(tmp_path: Path) -> Upstream:
    return Upstream(tmp_path / "upstream")


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    return Lake(tmp_path / "lake")


@pytest.fixture
def second_publish_fails(lake: Lake, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the second move into clean/ fail, as it does on Windows when a clean file is open in another program."""
    real_replace = os.replace
    moved_into_clean = []

    def replace(src, dst):
        if lake.clean in Path(dst).parents:
            moved_into_clean.append(dst)
            if len(moved_into_clean) == 2:
                raise PermissionError("the file is open in another program")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", replace)


def catalog_of(**datasets: dict) -> Catalog:
    return Catalog.model_validate({"source": {"repo": "example/repo"}, "datasets": datasets})


# A seasonal dataset shaped like nflverse player stats.
STATS = {"release": "stats_player", "file": "stats_player_week_{season}.parquet", "min_season": 2020,
         "key": ["player_id", "game_id"]}
STATS_FILE = "stats_player_week_2024.parquet"
STATS_FILE_ID = f"stats/{STATS_FILE}"

# A single-file dataset shaped like the nflverse schedule.
GAMES = {"release": "schedules", "file": "games.parquet", "key": ["game_id"]}
GAMES_FILE_ID = "games/games.parquet"


def stats_frame(targets: int = 8) -> pl.DataFrame:
    return pl.DataFrame({
        "player_id": ["00-002", "00-001"],
        "game_id": ["2024_01_BUF_MIA", "2024_01_BUF_MIA"],
        "targets": [3, targets],
    })
