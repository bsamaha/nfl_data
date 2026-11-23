"""Command line: `python -m nfl_lake <command>`."""

from __future__ import annotations

import logging
import os
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import NoReturn

import typer
import yaml
from dotenv import load_dotenv

from .catalog import Catalog, load_catalog
from .pipeline import UpdateFailed, get_lake_status, run_update, verify_lake
from .source import GitHubReleasesSource, LocalDirSource, SourceError, UpstreamSource
from .store import LakeLocked, LakeStore

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Ingest nflverse files into the local lake.")


class _CurrentStderrHandler(logging.StreamHandler):
    """Writes to whatever sys.stderr is when a record is emitted, not to the stream that existed at start-up."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value) -> None:
        pass  # StreamHandler.__init__ assigns one; there is nothing to keep


@app.callback()
def configure_cli(
    ctx: typer.Context,
    catalog: str = typer.Option("catalog/datasets.yml", help="Path to the dataset catalog."),
) -> None:
    ctx.obj = catalog
    load_dotenv()
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    handler = _CurrentStderrHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%SZ")
    formatter.converter = time.gmtime  # UTC, like run ids and the manifest
    handler.setFormatter(formatter)
    lake_logger = logging.getLogger("nfl_lake")
    lake_logger.handlers = [handler]
    lake_logger.setLevel(log_level)
    lake_logger.propagate = False


def _open_lake() -> LakeStore:
    """The lake folder: NFL_LAKE_ROOT, or `data` under the working directory."""
    return LakeStore(os.getenv("NFL_LAKE_ROOT") or "data")


def _open_source(catalog: Catalog) -> UpstreamSource:
    """Upstream source: NFL_LAKE_SOURCE_DIR reads a local mirror instead of GitHub."""
    local_source_dir = os.getenv("NFL_LAKE_SOURCE_DIR")
    if local_source_dir:
        return LocalDirSource(local_source_dir)
    return GitHubReleasesSource(catalog.source.repo, token=os.getenv("GITHUB_TOKEN") or None)


def _parse_dataset_names(raw_value: str | None) -> list[str] | None:
    """Dataset names from a comma-separated option. An option given with no name in it is refused, not read as 'all'."""
    if raw_value is None:
        return None
    dataset_names = [item.strip() for item in raw_value.split(",") if item.strip()]
    if not dataset_names:
        raise typer.BadParameter("give at least one dataset name, or leave the option out")
    return dataset_names


def _parse_season_filter(raw_value: str | None) -> set[int] | None:
    if not raw_value:
        return None
    seasons: set[int] = set()
    try:
        for token in raw_value.split(","):
            if "-" in token:
                start_season, end_season = (int(bound) for bound in token.split("-", 1))
                if start_season > end_season:
                    raise ValueError
                seasons.update(range(start_season, end_season + 1))
            elif token.strip():
                seasons.add(int(token))
        if not seasons:
            raise ValueError
    except ValueError:
        raise typer.BadParameter("use a season, a range or a list: 2026, 2015-2026 or 2024,2026") from None
    return seasons


def _exit_with_errors(error_lines: list[str]) -> NoReturn:
    for error_line in error_lines:
        typer.echo(f"ERROR {error_line}", err=True)
    raise typer.Exit(1)


@contextmanager
def _handle_cli_errors() -> Iterator[None]:
    """Turn a failure the user can act on into ERROR lines and exit code 1, with no traceback."""
    try:
        yield
    except UpdateFailed as error:
        if error.published_file_count:
            publish_outcome = (
                f"{error.published_file_count} file(s) were published before the failure; run update again"
            )
        else:
            publish_outcome = "nothing was published; the clean layer is unchanged"
        _exit_with_errors(error.errors + [publish_outcome])
    except (LakeLocked, SourceError, ValueError, OSError, yaml.YAMLError) as error:
        _exit_with_errors([str(error)])


@app.command()
def update(
    ctx: typer.Context,
    datasets: str | None = typer.Option(None, help="Comma-separated dataset names. Default: every dataset."),
    skip: str | None = typer.Option(
        None, help="Comma-separated dataset names to leave out, e.g. one that is broken upstream."
    ),
    seasons: str | None = typer.Option(None, help="Seasons to consider: 2026, 2015-2026 or 2024,2026. Default: all."),
    accept_schema_change: bool = typer.Option(
        False, "--accept-schema-change", help="Publish files whose columns were removed or changed type."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be fetched and rebuilt, and change nothing."
    ),
    workers: int = typer.Option(4, min=1, max=16, help="Parallel downloads."),
) -> None:
    """Fetch every file that changed upstream, rebuild the clean layer from it, and publish only if every file passed.

    The first run downloads everything. Later runs download only files whose upstream checksum changed.
    """
    with _handle_cli_errors():
        catalog = load_catalog(ctx.obj)
        lake = _open_lake()
        source = _open_source(catalog)
        selected_datasets = catalog.select_datasets(
            _parse_dataset_names(datasets),
            exclude_names=_parse_dataset_names(skip),
        )
        if skip is not None and not selected_datasets:
            raise ValueError("--skip leaves no dataset to update")
        selected_dataset_names = (
            None if datasets is None and skip is None else [dataset.name for dataset in selected_datasets]
        )
        summary = run_update(
            catalog,
            source,
            lake,
            dataset_names=selected_dataset_names,
            season_filter=_parse_season_filter(seasons),
            accept_schema_change=accept_schema_change,
            dry_run=dry_run,
            workers=workers,
        )
        fetched_megabytes = summary.bytes_fetched / 1e6
        if summary.dry_run:
            typer.echo(
                f"dry run: {summary.target_file_count} files in {summary.dataset_count} datasets; "
                f"would fetch {len(summary.fetched_file_ids)} ({fetched_megabytes:.1f} MB) "
                f"and rebuild {len(summary.rebuilt_file_ids)}"
            )
            for file_id in summary.fetched_file_ids:
                typer.echo(f"  fetch {file_id}")
            return
        typer.echo(f"{summary.run_id}: ok")
        typer.echo(
            f"  {summary.target_file_count} files in {summary.dataset_count} datasets: "
            f"fetched {len(summary.fetched_file_ids)} ({fetched_megabytes:.1f} MB), "
            f"rebuilt {len(summary.rebuilt_file_ids)}, unchanged {summary.unchanged_file_count}"
        )
        if summary.rejected_rows_by_file:
            typer.echo(
                f"  rows set aside for a null key: {sum(summary.rejected_rows_by_file.values())} "
                f"in {len(summary.rejected_rows_by_file)} file(s), see {lake.rejects_dir}"
            )
        if summary.deduped_rows_by_file:
            typer.echo(
                f"  repeated keys resolved: {sum(summary.deduped_rows_by_file.values())} row(s) "
                f"in {len(summary.deduped_rows_by_file)} file(s)"
            )
        if summary.pruned_raw_count:
            typer.echo(f"  old raw versions removed: {summary.pruned_raw_count}")
        for note in summary.notes:
            typer.echo(f"  note: {note}")


@app.command()
def status(ctx: typer.Context) -> None:
    """Show what is published per dataset, and how the last run ended."""
    with _handle_cli_errors():
        catalog = load_catalog(ctx.obj)
        lake = _open_lake()
        lake_status = get_lake_status(catalog, lake)
        typer.echo(f"lake: {lake.root_dir.resolve()}")
        typer.echo(
            f"{'dataset':<20}{'files':>6}  {'seasons':<10}{'rows':>11}{'set aside':>10}  {'upstream updated':<21}built"
        )
        for dataset_status in lake_status.datasets:
            typer.echo(
                f"{dataset_status.dataset_name:<20}{dataset_status.file_count:>6}  "
                f"{dataset_status.season_span:<10}{dataset_status.row_count:>11,}"
                f"{dataset_status.rejected_row_count:>10,}  "
                f"{dataset_status.latest_upstream_updated_at:<21}{dataset_status.latest_built_at}"
            )
        if any(dataset_status.season_span.endswith("*") for dataset_status in lake_status.datasets):
            typer.echo("* not every season in the range is published")
        last_run = lake_status.last_run
        if last_run is None:
            typer.echo("no runs recorded yet")
        else:
            typer.echo(
                f"last run {last_run['run_id']}: {last_run['status']} at {last_run['finished_at']}, "
                f"fetched {last_run['fetched']}, rebuilt {last_run['built']}"
            )
            for error_message in last_run["errors"]:
                typer.echo(f"  ERROR {error_message}")


@app.command()
def verify(
    ctx: typer.Context,
    datasets: str | None = typer.Option(None, help="Comma-separated dataset names. Default: every dataset."),
    deep: bool = typer.Option(False, "--deep", help="Also re-hash each raw file against the manifest."),
) -> None:
    """Re-check every published file: present, expected row count, key unique and non-null."""
    with _handle_cli_errors():
        catalog = load_catalog(ctx.obj)
        lake = _open_lake()
        verification = verify_lake(catalog, lake, _parse_dataset_names(datasets), deep)
        if verification.errors:
            _exit_with_errors(verification.errors)
        typer.echo(f"verified {verification.checked_file_count} file(s): ok")


@app.command()
def unlock() -> None:
    """Clear the lock left behind by an update that was killed. Only use it when no update is running."""
    with _handle_cli_errors():
        lake = _open_lake()  # the catalog is not read, so a broken one cannot keep a lock in place
        if lake.lock_path.exists():
            lake.lock_path.unlink()
            typer.echo(f"removed {lake.lock_path}")
        else:
            typer.echo("no lock to remove")
