"""Shared fixtures: a fake nflverse on disk, an empty lake, and a catalog builder.

The tests check what a run did: which files are on disk, what was fetched and built, which exit code came back.
None of them matches the wording of a message, so each bad input has exactly one fault.
"""

import os
import sys
from pathlib import Path

import polars as pl
import pytest
from hypothesis import settings

from nfl_lake.catalog import Catalog
from nfl_lake.source import LocalDirSource
from nfl_lake.store import LakeStore

# Hypothesis fails an example that runs long. The first polars call in a process does, so the limit is off.
settings.register_profile("nfl_lake", deadline=None)
settings.load_profile("nfl_lake")

# Settings from the shell would point a test at the real lake or change what it logs. A test that needs one sets it.
for setting_name in ("NFL_LAKE_ROOT", "NFL_LAKE_SOURCE_DIR", "LOG_LEVEL", "GITHUB_TOKEN"):
    os.environ.pop(setting_name, None)

LOCAL_HOSTS = {None, "localhost", "127.0.0.1", "::1", b"localhost", b"127.0.0.1"}
network_attempts: list[str] = []


def refuse_network(event: str, args: tuple) -> None:
    """No test reaches the network: a test that needs HTTP answers passes GitHubReleasesSource an open_url.

    The refusal is a RuntimeError, not an OSError, so the source does not retry it as a network blip.
    """
    if event == "socket.getaddrinfo":
        host = args[0]
    elif event == "socket.connect":
        address = args[1]
        host = address[0] if isinstance(address, tuple) else address
    else:
        return
    if host not in LOCAL_HOSTS:
        network_attempts.append(str(host))
        raise RuntimeError(f"tests do not reach the network ({host})")


sys.addaudithook(refuse_network)


@pytest.fixture(autouse=True)
def no_network_reached():
    """Fails the test even when the code under test catches the refused connection and carries on."""
    network_attempts.clear()
    yield
    assert not network_attempts, f"the test tried to reach {sorted(set(network_attempts))}"


class FakeUpstream:
    """A folder of <release>/<file> that a test writes to, changes and breaks."""

    def __init__(self, root_dir: Path):
        self.root_dir = root_dir

    def put(
        self,
        release_tag: str,
        filename: str,
        frame: pl.DataFrame,
        metadata: dict[str, str] | None = None,
    ) -> Path:
        asset_path = self.root_dir / release_tag / filename
        asset_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(asset_path, metadata=metadata)
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
