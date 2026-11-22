"""The command line: exit codes and the messages a scheduled job or a person will see."""
import io
import json

import polars as pl
import pytest
import yaml
from typer.testing import CliRunner

from conftest import GAMES, STATS, STATS_FILE, STATS_FILE_ID, catalog_of, stats_frame
from nfl_lake import source as source_module
from nfl_lake.cli import app

runner = CliRunner()


@pytest.fixture
def make_cli(tmp_path, upstream, lake, monkeypatch):
    """Build a CLI caller for a catalog of the given datasets, pointed at the test lake and upstream."""

    def make(**datasets):
        catalog = tmp_path / "datasets.yml"
        catalog.write_text(yaml.safe_dump(catalog_of(**datasets).model_dump(mode="json")))
        monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root))
        monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", str(upstream.root))
        return lambda *args: runner.invoke(app, ["--catalog", str(catalog), *args])

    return make


@pytest.fixture
def cli(make_cli):
    return make_cli(stats=STATS)


def test_update_status_and_verify_succeed(cli, upstream):
    upstream.put("stats_player", STATS_FILE, stats_frame())

    result = cli("update", "--dry-run")
    assert result.exit_code == 0, result.output
    assert "would fetch 1" in result.output and f"fetch {STATS_FILE_ID}" in result.output

    result = cli("update")
    assert result.exit_code == 0, result.output
    assert "fetched 1" in result.output and "rebuilt 1" in result.output

    result = cli("update")
    assert result.exit_code == 0, result.output
    assert "fetched 0" in result.output and "unchanged 1" in result.output

    result = cli("status")
    assert result.exit_code == 0, result.output
    assert "stats" in result.output and "2024-2024" in result.output and ": ok at" in result.output

    result = cli("verify", "--deep")
    assert result.exit_code == 0, result.output
    assert "verified 1 file(s): ok" in result.output


def test_failed_update_exits_non_zero_and_says_why(cli, upstream):
    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(), stats_frame()]))
    result = cli("update")
    assert result.exit_code == 1, result.output
    assert f"ERROR {STATS_FILE_ID}: 2 row(s) repeat a key" in result.output
    assert "ERROR nothing was published" in result.output

    result = cli("status")  # reports the failure, and exits 0 because status itself worked
    assert result.exit_code == 0, result.output
    assert "failed" in result.output and "repeat a key" in result.output


def test_failure_while_publishing_says_how_many_files_were_published(make_cli, upstream, second_publish_fails):
    cli = make_cli(stats=STATS, games=GAMES)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    result = cli("update")
    assert result.exit_code == 1, result.output
    assert "ERROR 1 file(s) were published before the failure; run update again" in result.output
    assert "nothing was published" not in result.output


def test_skip_publishes_the_rest_when_one_dataset_is_broken_upstream(make_cli, upstream, lake):
    cli = make_cli(stats=STATS, games=GAMES)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g1"]}))  # breaks its key

    result = cli("update")
    assert result.exit_code == 1, result.output
    assert not lake.clean.exists()

    result = cli("update", "--skip", "games")
    assert result.exit_code == 0, result.output
    assert "1 files in 1 datasets" in result.output
    assert (lake.root / "clean" / "stats" / STATS_FILE).exists()  # the path the README tells people to query

    result = cli("update", "--skip", "games,stats")
    assert result.exit_code == 1, result.output
    assert "--skip leaves no dataset to update" in result.output
    assert "not in the catalog: nope" in cli("update", "--skip", "nope").output


def test_missing_source_folder_is_reported(cli, lake):
    for args in (("update", "--dry-run"), ("update",)):  # the source folder does not exist yet
        result = cli(*args)
        assert result.exit_code == 1, result.output
        assert "does not exist" in result.output and "SourceError" not in result.output
    assert [run["status"] for run in lake.runs()] == ["failed"]  # the dry run is not recorded


def test_damaged_manifest_file_is_named(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    result = cli("update")
    assert result.exit_code == 0, result.output

    (lake.manifest / "clean_state.json").write_text("{not json")
    result = cli("status")
    assert result.exit_code == 1, result.output
    assert "clean_state.json is damaged" in result.output


def test_without_a_source_folder_the_catalog_repo_is_listed_on_github_with_the_token(cli, monkeypatch):
    seen = []

    def urlopen(req, timeout):
        seen.append(req)
        return io.BytesIO(json.dumps([{"tag_name": "stats_player", "assets": [
            {"name": STATS_FILE, "size": 10, "updated_at": "2026-11-21T16:46:06Z", "digest": None,
             "browser_download_url": "https://github.com/example/repo/releases/download/stats_player/" + STATS_FILE}]}]).encode())

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", "")  # empty rather than unset, so a .env file cannot fill it in
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    result = cli("update", "--dry-run")
    assert result.exit_code == 0, result.output
    assert f"fetch {STATS_FILE_ID}" in result.output
    (request,) = seen
    assert "/repos/example/repo/releases" in request.full_url and request.get_header("Authorization") == "Bearer t0ken"


def test_log_level_decides_whether_progress_lines_are_printed(cli, upstream, monkeypatch):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    result = cli("update")
    assert result.exit_code == 0, result.output
    assert "[info" not in result.output

    upstream.put("stats_player", STATS_FILE, stats_frame(targets=10))
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    assert "[info" in cli("update").output


def test_source_folder_can_be_a_relative_path(cli, upstream, tmp_path, monkeypatch):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", upstream.root.name)
    result = cli("update", "--dry-run")
    assert result.exit_code == 0, result.output


def test_bad_arguments_are_refused(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    for seasons in ("last-year", "2025-2024", ","):  # not a season, a reversed range, no season at all
        result = cli("update", "--seasons", seasons)
        assert result.exit_code == 2, result.output

    for args in (("update", "--datasets", ","), ("update", "--skip", " "), ("verify", "--datasets", ",")):
        result = cli(*args)  # an option with no name in it does not mean every dataset
        assert result.exit_code == 2, result.output

    result = cli("update", "--datasets", "nope")
    assert result.exit_code == 1, result.output
    assert "ERROR not in the catalog: nope" in result.output
    assert lake.runs() == []  # a typo is not a run


@pytest.mark.parametrize("seasons, files", [("2025", 1), ("2024-2025", 2), ("2023,2025", 2)])
def test_seasons_option_limits_the_run(cli, upstream, seasons, files):
    for season in (2023, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())
    result = cli("update", "--dry-run", "--seasons", seasons)
    assert result.exit_code == 0, result.output
    assert f"{files} files in 1 datasets" in result.output


def test_datasets_option_limits_the_run(make_cli, upstream):
    cli = make_cli(stats=STATS, games=GAMES)
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    result = cli("update", "--dry-run", "--datasets", "stats")
    assert result.exit_code == 0, result.output
    assert "1 files in 1 datasets" in result.output and f"fetch {STATS_FILE_ID}" in result.output


def test_schema_change_needs_the_accept_option(cli, upstream):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    result = cli("update")
    assert result.exit_code == 0, result.output

    upstream.put("stats_player", STATS_FILE, stats_frame().with_columns(pl.col("targets").cast(pl.Float64)))
    result = cli("update")
    assert result.exit_code == 1, result.output
    assert f"ERROR {STATS_FILE_ID}: schema changed" in result.output

    result = cli("update", "--accept-schema-change")
    assert result.exit_code == 0, result.output


def test_verify_exits_non_zero_on_damage(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    result = cli("update")
    assert result.exit_code == 0, result.output
    (lake.root / lake.raw_versions()[STATS_FILE_ID][0]["path"]).write_bytes(b"tampered")

    result = cli("verify")  # the clean file is intact; only --deep reads the raw one
    assert result.exit_code == 0, result.output

    result = cli("verify", "--deep")
    assert result.exit_code == 1, result.output
    assert "no longer matches its recorded checksum" in result.output

    result = cli("verify", "--datasets", "nope")
    assert result.exit_code == 1, result.output
    assert "not in the catalog: nope" in result.output


@pytest.mark.parametrize("damage", [
    lambda catalog: catalog.unlink(),  # no such file
    lambda catalog: catalog.write_text("datasets: [unclosed"),  # not YAML
    lambda catalog: catalog.write_text(catalog.read_text() + "root: elsewhere\n"),  # an unknown setting
])
def test_unreadable_catalog_is_reported_by_every_command_that_reads_it(make_cli, tmp_path, lake, damage):
    cli = make_cli(stats=STATS)
    damage(tmp_path / "datasets.yml")
    for command in ("update", "status", "verify"):
        result = cli(command)
        assert result.exit_code == 1, (command, result.output)
        assert "ERROR" in result.output

    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")
    result = cli("unlock")  # reads no catalog, so a broken one cannot keep a stale lock in place
    assert result.exit_code == 0, result.output
    assert not lake.lock_path.exists()


def test_status_marks_a_season_range_that_has_gaps(cli, upstream):
    for season in (2022, 2023, 2024):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())
    result = cli("update", "--seasons", "2022,2024")
    assert result.exit_code == 0, result.output
    result = cli("status")
    assert "2022-2024*" in result.output and "* not every season in the range is published" in result.output

    result = cli("update")
    assert result.exit_code == 0, result.output
    assert "*" not in cli("status").output


def test_unlock_clears_a_stale_lock(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")

    result = cli("update")
    assert result.exit_code == 1, result.output
    assert "unlock" in result.output

    result = cli("unlock")
    assert result.exit_code == 0, result.output
    assert not lake.lock_path.exists()

    result = cli("update")
    assert result.exit_code == 0, result.output
