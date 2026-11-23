"""The command line: exit codes and what ends up on disk. A scheduled job acts on the exit code."""

import polars as pl
import pytest
import yaml
from conftest import (
    GAMES_DATASET_SPEC,
    STATS_DATASET_SPEC,
    STATS_FILE_ID,
    STATS_FILENAME,
    build_catalog,
    make_stats_frame,
)
from typer.testing import CliRunner

from nfl_lake.cli import app
from nfl_lake.pipeline import run_update

cli_runner = CliRunner()


@pytest.fixture
def make_cli(tmp_path, upstream, lake, monkeypatch):
    """Build a CLI caller for a catalog of the given datasets, pointed at the test lake and upstream."""

    def create_caller(**datasets):
        catalog_path = tmp_path / "datasets.yml"
        catalog_path.write_text(yaml.safe_dump(build_catalog(**datasets).model_dump(mode="json")))
        monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root_dir))
        monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", str(upstream.root_dir))
        return lambda *args: cli_runner.invoke(app, ["--catalog", str(catalog_path), *args])

    return create_caller


@pytest.fixture
def cli(make_cli):
    return make_cli(stats=STATS_DATASET_SPEC)


def command_succeeded(result) -> bool:
    return result.exit_code == 0


def command_refused(result, exit_code: int = 1) -> bool:
    """The command stopped itself with this exit code. A crash also ends with code 1, but not through SystemExit."""
    return result.exit_code == exit_code and isinstance(result.exception, SystemExit)


def test_update_publishes_where_the_readme_says_and_a_dry_run_writes_nothing(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(cli("update", "--dry-run")) and not lake.root_dir.exists()

    for command in (("update",), ("update",), ("status",), ("verify", "--deep")):
        result = cli(*command)
        assert command_succeeded(result), result.output
    assert (lake.root_dir / "clean" / "stats" / STATS_FILENAME).exists()  # the path the README tells people to query
    assert [run["fetched"] for run in lake.load_run_records()] == [1, 0]


def test_progress_still_reaches_stderr_after_a_command_has_finished(cli, upstream, lake, capsys):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(
        cli("update", "--dry-run")
    )  # sets logging up while stderr is the runner's, which then closes

    run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)  # logs its progress

    progress = capsys.readouterr().err
    assert progress and "Traceback" not in progress


def test_failed_update_exits_1_and_status_still_exits_0(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, pl.concat([make_stats_frame(), make_stats_frame()]))  # every key twice
    result = cli("update")
    assert command_refused(result), result.output
    assert not lake.clean_dir.exists()

    result = cli("status")  # it reports the failed run; it did not fail itself
    assert command_succeeded(result), result.output


def test_bad_arguments_are_refused(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    invalid_invocations = [
        ("update", "--seasons", "last-year"),
        ("update", "--seasons", "2025-2024"),
        ("update", "--seasons", ","),
        ("update", "--datasets", ","),
        ("update", "--skip", " "),
        ("verify", "--datasets", ","),
    ]
    for args in invalid_invocations:  # not a season, a reversed range, and empty options: none means "everything"
        result = cli(*args)
        assert command_refused(result, exit_code=2), (args, result.output)

    result = cli("update", "--datasets", "nope")
    assert command_refused(result), result.output
    assert lake.load_run_records() == [] and not lake.clean_dir.exists()  # a typo is not a run


@pytest.mark.parametrize(
    "seasons_arg, expected_seasons",
    [("2025", {2025}), ("2024-2025", {2024, 2025}), ("2023,2025", {2023, 2025})],
)
def test_seasons_option_limits_the_run(cli, upstream, lake, seasons_arg, expected_seasons):
    for season in (2023, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", make_stats_frame())
    result = cli("update", "--seasons", seasons_arg)
    assert command_succeeded(result), result.output
    published_filenames = {path.name for path in (lake.clean_dir / "stats").iterdir()}
    assert published_filenames == {f"stats_player_week_{season}.parquet" for season in expected_seasons}


def test_datasets_and_skip_options_limit_the_run(make_cli, upstream, lake):
    cli = make_cli(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)

    def published_datasets():
        return sorted(path.name for path in lake.clean_dir.iterdir())

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g1"]}))  # breaks its key

    assert command_refused(cli("update")) and not lake.clean_dir.exists()
    assert command_refused(cli("update", "--skip", "games,stats"))  # nothing left to update
    result = cli("update", "--skip", "games")
    assert command_succeeded(result), result.output
    assert published_datasets() == ["stats"]

    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))  # games is fixed
    upstream.put(
        "stats_player", STATS_FILENAME, pl.concat([make_stats_frame(), make_stats_frame()])
    )  # and stats breaks
    result = cli("update", "--datasets", "games")
    assert command_succeeded(result), result.output
    assert published_datasets() == ["games", "stats"]


def test_schema_change_needs_the_accept_option(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(cli("update"))

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame().with_columns(pl.col("targets").cast(pl.Float64)))
    assert command_refused(cli("update"))
    result = cli("update", "--accept-schema-change")
    assert command_succeeded(result), result.output
    assert pl.read_parquet_schema(lake.clean_file_path("stats", STATS_FILENAME))["targets"] == pl.Float64


def test_verify_exits_1_on_damage(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(cli("update"))
    raw_file_path = lake.root_dir / lake.load_raw_versions()[STATS_FILE_ID][0]["path"]
    raw_file_path.write_bytes(b"tampered")

    result = cli("verify")  # the clean file is intact; only --deep reads the raw one
    assert command_succeeded(result), result.output
    assert command_refused(cli("verify", "--deep"))
    assert command_refused(cli("verify", "--datasets", "nope"))


@pytest.mark.parametrize(
    "corrupt_catalog",
    [
        lambda catalog_path: catalog_path.unlink(),
        lambda catalog_path: catalog_path.write_text("datasets: [unclosed"),
        lambda catalog_path: catalog_path.write_text(catalog_path.read_text() + "root: elsewhere\n"),
    ],
    ids=["no such file", "not YAML", "an unknown setting"],
)
def test_unreadable_catalog_stops_every_command_that_reads_it(make_cli, tmp_path, lake, corrupt_catalog):
    cli = make_cli(stats=STATS_DATASET_SPEC)
    corrupt_catalog(tmp_path / "datasets.yml")
    for command in ("update", "status", "verify"):
        result = cli(command)
        assert command_refused(result), (command, result.output)

    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")
    result = cli("unlock")  # reads no catalog, so a broken one cannot keep a stale lock in place
    assert command_succeeded(result), result.output
    assert not lake.lock_path.exists()


def test_stale_lock_stops_an_update_until_unlock(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")

    assert command_refused(cli("update")) and not lake.clean_dir.exists()
    assert command_succeeded(cli("unlock")) and not lake.lock_path.exists()
    result = cli("update")
    assert command_succeeded(result), result.output


def test_missing_source_folder_is_an_error_and_a_relative_one_works(cli, upstream, lake, tmp_path, monkeypatch):
    assert command_refused(cli("update", "--dry-run")) and command_refused(
        cli("update")
    )  # the folder does not exist yet
    assert [run["status"] for run in lake.load_run_records()] == ["failed"]  # only the real run is recorded

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", upstream.root_dir.name)
    result = cli("update", "--dry-run")
    assert command_succeeded(result), result.output
