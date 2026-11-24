"""The command line: exit codes and what ends up on disk. A scheduled job acts on the exit code."""

import contextlib
import io
from dataclasses import dataclass

import polars as pl
import pytest
import yaml
from conftest import (
    GAMES_DATASET_SPEC,
    STATS_DATASET_SPEC,
    STATS_FILENAME,
    build_catalog,
    make_stats_frame,
)

from nfl_lake.cli import main
from nfl_lake.pipeline import run_update


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        return self.stdout + self.stderr


@pytest.fixture
def make_cli(tmp_path, upstream, lake, monkeypatch, capsys):
    """Build a CLI caller for a catalog of the given datasets, pointed at the test lake and upstream."""

    def create_caller(**datasets):
        catalog_path = tmp_path / "datasets.yml"
        catalog_path.write_text(yaml.safe_dump(build_catalog(**datasets).model_dump(mode="json")))
        monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root_dir))
        monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", str(upstream.root_dir))
        monkeypatch.delenv("LOG_LEVEL", raising=False)  # the developer's own setting must not decide a test

        def invoke(*args: str) -> CliResult:
            capsys.readouterr()
            exit_code = 0
            try:
                main(["--catalog", str(catalog_path), *args])
            except SystemExit as exit_signal:
                exit_code = exit_signal.code if isinstance(exit_signal.code, int) else 1
            captured = capsys.readouterr()
            return CliResult(exit_code=exit_code, stdout=captured.out, stderr=captured.err)

        return invoke

    return create_caller


@pytest.fixture
def cli(make_cli):
    return make_cli(stats=STATS_DATASET_SPEC)


def command_succeeded(result: CliResult) -> bool:
    return result.exit_code == 0


def command_refused(result: CliResult, exit_code: int = 1) -> bool:
    return result.exit_code == exit_code


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
    # Configure logging while sys.stderr is a temporary stream that closes right after the command.
    temporary_stderr = io.StringIO()
    with contextlib.redirect_stderr(temporary_stderr):
        assert command_succeeded(cli("update", "--dry-run"))
    temporary_stderr.close()

    run_update(build_catalog(stats=STATS_DATASET_SPEC), upstream.source, lake)  # logs its progress

    progress = capsys.readouterr().err
    assert progress and "Traceback" not in progress


def test_failed_update_exits_1_and_status_still_exits_0(cli, upstream, lake, monkeypatch):
    upstream.put("stats_player", STATS_FILENAME, pl.concat([make_stats_frame(), make_stats_frame()]))  # every key twice
    result = cli("update")
    assert command_refused(result), result.output
    assert not lake.clean_dir.exists()

    monkeypatch.setenv("LOG_LEVEL", "BASIC_FORMAT")  # not a level, so the default is used
    result = cli("status")  # it reports the failed run; it did not fail itself
    assert command_succeeded(result), result.output


def test_run_log_line_cut_short_by_a_kill_costs_only_that_line(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(cli("update"))
    with open(lake.manifest_dir / "runs.jsonl", "a") as run_log:
        run_log.write('{"run_id": "run_cut_short", "status": "o')  # the write never finished

    assert command_succeeded(cli("update")) and command_succeeded(cli("status"))
    assert [run["fetched"] for run in lake.load_run_records()] == [1, 0]


@pytest.mark.parametrize("damaged_state", ["[]", '{"stats/a.parquet": {}}'], ids=["not entries", "an emptied entry"])
def test_clean_state_of_the_wrong_shape_is_refused_by_every_command_that_reads_it(cli, upstream, lake, damaged_state):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    assert command_succeeded(cli("update"))
    (lake.manifest_dir / "clean_state.json").write_text(damaged_state)  # valid JSON, as a slip in a hand edit leaves it
    for command in (("status",), ("verify",), ("update", "--dry-run"), ("update",)):
        result = cli(*command)
        assert command_refused(result), (command, result.output)


def test_selection_that_matches_nothing_is_an_error_not_a_success(make_cli, upstream, lake):
    cli = make_cli(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))
    assert command_refused(cli("verify"))  # nothing is published yet
    for dry_run_option in ((), ("--dry-run",)):
        assert command_refused(cli("update", "--seasons", "2062", *dry_run_option))  # no seasonal file matches 2062
        assert command_refused(cli("update", "--datasets", "games", "--seasons", "2024", *dry_run_option))
    assert not lake.clean_dir.exists()


def test_dry_run_exits_1_when_a_dataset_cannot_be_planned(make_cli, upstream, lake):
    cli = make_cli(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())  # the release games needs is not there
    assert command_refused(cli("update", "--dry-run")) and not lake.root_dir.exists()


def test_bad_arguments_are_refused(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame())
    invalid_invocations = [
        (),
        ("update", "--dry"),
        ("update", "--seasons", "last-year"),
        ("update", "--datasets", ","),
        ("update", "--skip", " "),
        ("update", "--workers", "0"),
        ("verify", "--datasets", ","),
    ]
    # no command, a shortened option, not a season, out-of-range workers, and empty options
    for args in invalid_invocations:
        result = cli(*args)
        assert command_refused(result, exit_code=2), (args, result.output)

    result = cli("update", "--datasets", "nope")
    assert command_refused(result), result.output
    assert lake.load_run_records() == [] and not lake.clean_dir.exists()  # a typo is not a run


def test_seasons_option_limits_the_run(cli, upstream, lake):
    for season in (2023, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", make_stats_frame())
    result = cli("update", "--seasons", "2023,2025")  # test_properties.py covers every way to write the option
    assert command_succeeded(result), result.output
    published_filenames = {path.name for path in (lake.clean_dir / "stats").iterdir()}
    assert published_filenames == {"stats_player_week_2023.parquet", "stats_player_week_2025.parquet"}


def test_datasets_and_skip_options_limit_the_run(make_cli, upstream, lake):
    cli = make_cli(stats=STATS_DATASET_SPEC, games=GAMES_DATASET_SPEC)

    def published_datasets():
        return sorted(path.name for path in lake.clean_dir.iterdir())

    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=8))
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g1"]}))  # breaks its key

    assert command_refused(cli("update", "--skip", "games,stats")) and not lake.clean_dir.exists()
    result = cli("update", "--skip", "games")
    assert command_succeeded(result), result.output
    assert published_datasets() == ["stats"]

    # Without --skip, the broken dataset causes exit 1 while the healthy dataset still publishes its update.
    upstream.put("stats_player", STATS_FILENAME, make_stats_frame(targets=10))
    assert command_refused(cli("update"))
    assert published_datasets() == ["stats"]
    clean_stats = pl.read_parquet(lake.clean_file_path("stats", STATS_FILENAME))
    assert clean_stats.filter(pl.col("player_id") == "00-001")["targets"].item() == 10

    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))  # games is fixed
    upstream.put("stats_player", STATS_FILENAME, pl.concat([make_stats_frame(), make_stats_frame()]))  # stats breaks
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
    # Tamper with a non-key column: key and row count stay valid, so only --deep catches the checksum change.
    make_stats_frame(targets=99).sort("player_id", "game_id").write_parquet(
        lake.clean_file_path("stats", STATS_FILENAME)
    )

    result = cli("verify")
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
