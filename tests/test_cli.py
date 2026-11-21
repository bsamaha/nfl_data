"""The command line: exit codes and the messages a scheduled job or a person will see."""
import polars as pl
import pytest
import yaml
from typer.testing import CliRunner

from conftest import STATS, STATS_FILE, stats_frame
from nfl_lake.cli import app

runner = CliRunner()


@pytest.fixture
def cli(tmp_path, upstream, monkeypatch):
    catalog = tmp_path / "datasets.yml"
    catalog.write_text(yaml.safe_dump({"source": {"repo": "example/repo"}, "datasets": {"stats": STATS}}))
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", str(upstream.root))
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    return lambda *args: runner.invoke(app, ["--catalog", str(catalog), *args])


def test_update_status_and_verify_succeed(cli, upstream):
    upstream.put("stats_player", STATS_FILE, stats_frame())

    result = cli("update", "--dry-run")
    assert result.exit_code == 0 and "would fetch 1" in result.output and f"fetch stats/{STATS_FILE}" in result.output

    result = cli("update")
    assert result.exit_code == 0 and "fetched 1" in result.output and "rebuilt 1" in result.output

    result = cli("update")
    assert result.exit_code == 0 and "fetched 0" in result.output and "unchanged 1" in result.output

    result = cli("status")
    assert result.exit_code == 0 and "stats" in result.output and "2024-2024" in result.output and ": ok at" in result.output

    result = cli("verify", "--deep")
    assert result.exit_code == 0 and "verified 1 file(s): ok" in result.output


def test_failed_update_exits_non_zero_and_says_why(cli, upstream):
    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(), stats_frame()]))
    result = cli("update")
    assert result.exit_code == 1
    assert "repeat a key" in result.output and "nothing was published" in result.output

    result = cli("status")
    assert "failed" in result.output and "repeat a key" in result.output


def test_skip_publishes_the_rest_when_one_dataset_is_broken_upstream(tmp_path, upstream, monkeypatch):
    games = {"release": "schedules", "file": "games.parquet", "key": ["game_id"]}
    catalog = tmp_path / "two.yml"
    catalog.write_text(yaml.safe_dump({"source": {"repo": "example/repo"}, "datasets": {"stats": STATS, "games": games}}))
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", str(upstream.root))
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g1"]}))  # breaks its key
    run = lambda *args: runner.invoke(app, ["--catalog", str(catalog), "update", *args])

    assert run().exit_code == 1
    assert not (tmp_path / "lake" / "clean").exists()

    result = run("--skip", "games")
    assert result.exit_code == 0 and "1 files in 1 datasets" in result.output
    assert (tmp_path / "lake" / "clean" / "stats" / STATS_FILE).exists()

    assert run("--skip", "games,stats").exit_code == 1
    assert "not in the catalog: nope" in run("--skip", "nope").output


def test_unreachable_source_is_reported_without_a_traceback(cli, upstream):
    result = cli("update", "--dry-run")  # the source folder does not exist yet
    assert result.exit_code == 1 and "does not exist" in result.output and "Traceback" not in result.output


def test_bad_arguments_are_refused(cli, upstream):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    assert cli("update", "--seasons", "last-year").exit_code == 2
    result = cli("update", "--datasets", "nope")
    assert result.exit_code == 1 and "not in the catalog: nope" in result.output


def test_unlock_clears_a_stale_lock(cli, upstream, tmp_path):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    lock = tmp_path / "lake" / ".update.lock"
    lock.parent.mkdir()
    lock.write_text("{}")

    result = cli("update")
    assert result.exit_code == 1 and "unlock" in result.output
    assert cli("unlock").exit_code == 0 and not lock.exists()
    assert cli("update").exit_code == 0
