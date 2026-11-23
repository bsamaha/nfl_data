"""The command line: exit codes and what ends up on disk. A scheduled job acts on the exit code."""
import polars as pl
import pytest
import yaml
from typer.testing import CliRunner

from conftest import GAMES, STATS, STATS_FILE, STATS_FILE_ID, catalog_of, stats_frame
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


def ok(result) -> bool:
    return result.exit_code == 0


def refused(result, code: int = 1) -> bool:
    """The command stopped itself with this exit code. A crash also ends with code 1, but not through SystemExit."""
    return result.exit_code == code and isinstance(result.exception, SystemExit)


def test_update_publishes_where_the_readme_says_and_a_dry_run_writes_nothing(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    assert ok(cli("update", "--dry-run")) and not lake.root.exists()

    for command in (("update",), ("update",), ("status",), ("verify", "--deep")):
        result = cli(*command)
        assert ok(result), result.output
    assert (lake.root / "clean" / "stats" / STATS_FILE).exists()  # the path the README tells people to query
    assert [run["fetched"] for run in lake.runs()] == [1, 0]


def test_failed_update_exits_1_and_status_still_exits_0(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(), stats_frame()]))  # every key twice
    result = cli("update")
    assert refused(result), result.output
    assert not lake.clean.exists()

    result = cli("status")  # it reports the failed run; it did not fail itself
    assert ok(result), result.output


def test_bad_arguments_are_refused(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    bad = [("update", "--seasons", "last-year"), ("update", "--seasons", "2025-2024"), ("update", "--seasons", ","),
           ("update", "--datasets", ","), ("update", "--skip", " "), ("verify", "--datasets", ",")]
    for args in bad:  # not a season, a reversed range, and options with nothing in them: none means "everything"
        result = cli(*args)
        assert refused(result, code=2), (args, result.output)

    result = cli("update", "--datasets", "nope")
    assert refused(result), result.output
    assert lake.runs() == [] and not lake.clean.exists()  # a typo is not a run


@pytest.mark.parametrize("seasons, published", [("2025", {2025}), ("2024-2025", {2024, 2025}), ("2023,2025", {2023, 2025})])
def test_seasons_option_limits_the_run(cli, upstream, lake, seasons, published):
    for season in (2023, 2024, 2025):
        upstream.put("stats_player", f"stats_player_week_{season}.parquet", stats_frame())
    result = cli("update", "--seasons", seasons)
    assert ok(result), result.output
    assert {p.name for p in (lake.clean / "stats").iterdir()} == {f"stats_player_week_{s}.parquet" for s in published}


def test_datasets_and_skip_options_limit_the_run(make_cli, upstream, lake):
    cli = make_cli(stats=STATS, games=GAMES)
    published = lambda: sorted(p.name for p in lake.clean.iterdir())
    upstream.put("stats_player", STATS_FILE, stats_frame())
    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1", "g1"]}))  # breaks its key

    assert refused(cli("update")) and not lake.clean.exists()
    assert refused(cli("update", "--skip", "games,stats"))  # nothing left to update
    result = cli("update", "--skip", "games")
    assert ok(result), result.output
    assert published() == ["stats"]

    upstream.put("schedules", "games.parquet", pl.DataFrame({"game_id": ["g1"]}))                    # games is fixed
    upstream.put("stats_player", STATS_FILE, pl.concat([stats_frame(), stats_frame()]))  # and stats breaks
    result = cli("update", "--datasets", "games")
    assert ok(result), result.output
    assert published() == ["games", "stats"]


def test_schema_change_needs_the_accept_option(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    assert ok(cli("update"))

    upstream.put("stats_player", STATS_FILE, stats_frame().with_columns(pl.col("targets").cast(pl.Float64)))
    assert refused(cli("update"))
    result = cli("update", "--accept-schema-change")
    assert ok(result), result.output
    assert pl.read_parquet_schema(lake.clean_path("stats", STATS_FILE))["targets"] == pl.Float64


def test_verify_exits_1_on_damage(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    assert ok(cli("update"))
    (lake.root / lake.raw_versions()[STATS_FILE_ID][0]["path"]).write_bytes(b"tampered")

    result = cli("verify")  # the clean file is intact; only --deep reads the raw one
    assert ok(result), result.output
    assert refused(cli("verify", "--deep"))
    assert refused(cli("verify", "--datasets", "nope"))


@pytest.mark.parametrize("damage", [
    lambda catalog: catalog.unlink(),
    lambda catalog: catalog.write_text("datasets: [unclosed"),
    lambda catalog: catalog.write_text(catalog.read_text() + "root: elsewhere\n"),
], ids=["no such file", "not YAML", "an unknown setting"])
def test_unreadable_catalog_stops_every_command_that_reads_it(make_cli, tmp_path, lake, damage):
    cli = make_cli(stats=STATS)
    damage(tmp_path / "datasets.yml")
    for command in ("update", "status", "verify"):
        result = cli(command)
        assert refused(result), (command, result.output)

    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")
    result = cli("unlock")  # reads no catalog, so a broken one cannot keep a stale lock in place
    assert ok(result), result.output
    assert not lake.lock_path.exists()


def test_stale_lock_stops_an_update_until_unlock(cli, upstream, lake):
    upstream.put("stats_player", STATS_FILE, stats_frame())
    lake.lock_path.parent.mkdir()
    lake.lock_path.write_text("{}")

    assert refused(cli("update")) and not lake.clean.exists()
    assert ok(cli("unlock")) and not lake.lock_path.exists()
    result = cli("update")
    assert ok(result), result.output


def test_missing_source_folder_is_an_error_and_a_relative_one_works(cli, upstream, lake, tmp_path, monkeypatch):
    assert refused(cli("update", "--dry-run")) and refused(cli("update"))  # the folder does not exist yet
    assert [run["status"] for run in lake.runs()] == ["failed"]  # only the real run is recorded

    upstream.put("stats_player", STATS_FILE, stats_frame())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", upstream.root.name)
    result = cli("update", "--dry-run")
    assert ok(result), result.output
