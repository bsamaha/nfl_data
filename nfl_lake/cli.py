"""Command line: `python -m nfl_lake <command>`."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import NoReturn

import yaml

from .catalog import Catalog, load_catalog
from .checks import format_check_outcome
from .core import (
    CORE_TABLE_NAMES,
    CoreBuildFailed,
    build_core_tables,
    list_changed_clean_inputs,
    verify_core_tables,
)
from .pipeline import UpdateFailed, get_lake_status, run_update, verify_lake
from .source import GitHubReleasesSource, LocalDirSource, SourceError, UpstreamSource
from .store import LakeLocked, LakeStore


class _CurrentStderrHandler(logging.StreamHandler):
    """Writes to whatever sys.stderr is when a record is emitted, not to the stream that existed at start-up."""

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value) -> None:
        pass  # StreamHandler.__init__ assigns one; there is nothing to keep


def _configure_logging() -> None:
    log_level = logging.getLevelNamesMapping().get(os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
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


def _open_source(catalog: Catalog, lake: LakeStore, *, dry_run: bool = False) -> UpstreamSource:
    """Upstream source: NFL_LAKE_SOURCE_DIR reads a local mirror instead of GitHub."""
    local_source_dir = os.getenv("NFL_LAKE_SOURCE_DIR")
    if local_source_dir:
        return LocalDirSource(local_source_dir)
    # A token read from a file often ends in a newline. Sent as it is, the HTTP library refuses the header and
    # quotes the token in its error, so the value is cleaned here and never printed.
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not (token.isascii() and token.isprintable() and " " not in token):
        raise ValueError("GITHUB_TOKEN holds a space, a line break or another character a token cannot have")
    return GitHubReleasesSource(
        catalog.source.repo,
        token=token or None,
        cache_path=lake.manifest_dir / "listing_cache.json",
        write_cache=not dry_run,
    )


def _parse_dataset_names(raw_value: str) -> list[str]:
    """Dataset names from a comma-separated option. An option given with no name in it is refused, not read as 'all'."""
    dataset_names = [item.strip() for item in raw_value.split(",") if item.strip()]
    if not dataset_names:
        raise argparse.ArgumentTypeError("give at least one dataset name, or leave the option out")
    return dataset_names


def _parse_season_filter(raw_value: str) -> set[int]:
    seasons: set[int] = set()
    try:
        for token in raw_value.split(","):
            if "-" in token:
                start_season, end_season = (int(bound) for bound in token.split("-", 1))
                if not (1900 <= start_season <= end_season <= 2999):
                    raise ValueError
                seasons.update(range(start_season, end_season + 1))
            elif token.strip():
                season = int(token)
                if not (1900 <= season <= 2999):
                    raise ValueError
                seasons.add(season)
        if not seasons:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("use a season, a range or a list: 2026, 2015-2026 or 2024,2026") from None
    return seasons


def _parse_workers(raw_value: str) -> int:
    try:
        workers = int(raw_value)
        if not 1 <= workers <= 16:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("workers must be an integer between 1 and 16") from None
    return workers


def _exit_with_errors(error_lines: list[str]) -> NoReturn:
    for error_line in error_lines:
        print(f"ERROR {error_line}", file=sys.stderr)
    raise SystemExit(1)


@contextmanager
def _handle_cli_errors() -> Iterator[None]:
    """Turn a failure the user can act on into ERROR lines and exit code 1, with no traceback."""
    try:
        yield
    except UpdateFailed as error:
        if error.published_file_count:
            publish_outcome = f"{error.published_file_count} file(s) were published; run update again for the rest"
        else:
            publish_outcome = "nothing was published; the clean layer is unchanged"
        _exit_with_errors(error.errors + [publish_outcome])
    except CoreBuildFailed as error:
        _exit_with_errors(error.errors + ["nothing was published; the core layer is unchanged"])
    except (LakeLocked, SourceError, ValueError, OSError, yaml.YAMLError) as error:
        _exit_with_errors([str(error)])


def _cmd_update(args: argparse.Namespace) -> None:
    catalog = load_catalog(args.catalog)
    selected_datasets = catalog.select_datasets(args.datasets, exclude_names=args.skip)
    if args.skip is not None and not selected_datasets:
        raise ValueError("--skip leaves no dataset to update")
    selected_dataset_names = (
        None if args.datasets is None and args.skip is None else [dataset.name for dataset in selected_datasets]
    )
    lake = _open_lake()
    source = _open_source(catalog, lake, dry_run=args.dry_run)
    summary = run_update(
        catalog,
        source,
        lake,
        dataset_names=selected_dataset_names,
        season_filter=args.seasons,
        accept_schema_change=args.accept_schema_change,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    fetched_megabytes = summary.bytes_fetched / 1e6
    if summary.dry_run:
        if summary.target_file_count or not summary.plan_errors:
            print(
                f"dry run: {summary.target_file_count} files in {summary.dataset_count} datasets; "
                f"would fetch {len(summary.fetched_file_ids)} ({fetched_megabytes:.1f} MB) "
                f"and rebuild {len(summary.rebuilt_file_ids)}"
            )
            for file_id in summary.fetched_file_ids:
                print(f"  fetch {file_id}")
        if summary.plan_errors:  # a real run would do the above and then fail on these
            _exit_with_errors(summary.plan_errors)
        return
    print(f"{summary.run_id}: ok")
    print(
        f"  {summary.target_file_count} files in {summary.dataset_count} datasets: "
        f"fetched {len(summary.fetched_file_ids)} ({fetched_megabytes:.1f} MB), "
        f"rebuilt {len(summary.rebuilt_file_ids)}, unchanged {summary.unchanged_file_count}"
    )
    if summary.rejected_rows_by_file:
        print(
            f"  rows set aside for a null key: {sum(summary.rejected_rows_by_file.values())} "
            f"in {len(summary.rejected_rows_by_file)} file(s), see {lake.rejects_dir}"
        )
    if summary.deduped_rows_by_file:
        print(
            f"  repeated keys resolved: {sum(summary.deduped_rows_by_file.values())} row(s) "
            f"in {len(summary.deduped_rows_by_file)} file(s)"
        )
    for note in summary.notes:
        print(f"  note: {note}")


def _cmd_status(args: argparse.Namespace) -> None:
    catalog = load_catalog(args.catalog)
    lake = _open_lake()
    lake_status = get_lake_status(catalog, lake)
    print(f"lake: {lake.root_dir.resolve()}")
    print(f"{'dataset':<20}{'files':>6}  {'seasons':<10}{'rows':>11}{'set aside':>10}  {'upstream updated':<21}built")
    for dataset_status in lake_status.datasets:
        print(
            f"{dataset_status.dataset_name:<20}{dataset_status.file_count:>6}  "
            f"{dataset_status.season_span:<10}{dataset_status.row_count:>11,}"
            f"{dataset_status.rejected_row_count:>10,}  "
            f"{dataset_status.latest_upstream_updated_at:<21}{dataset_status.latest_built_at}"
        )
    if any(dataset_status.season_span.endswith("*") for dataset_status in lake_status.datasets):
        print("* not every season in the range is published")
    last_run = lake_status.last_run
    if last_run is None:
        print("no runs recorded yet")
    else:
        print(
            f"last run {last_run['run_id']}: {last_run['status']} at {last_run['finished_at']}, "
            f"fetched {last_run['fetched']}, rebuilt {last_run['built']}"
        )
        for error_message in last_run["errors"]:
            print(f"  ERROR {error_message}")
    core_state = lake.load_core_state()
    if core_state is not None:
        print()
        print(f"{'core table':<22}{'files':>6}{'rows':>12}")
        for table_name in CORE_TABLE_NAMES:
            table_record = core_state.tables.get(table_name)
            if table_record is None:
                print(f"{table_name:<22}{0:>6}{0:>12}  (missing)")
            else:
                print(f"{table_name:<22}{len(table_record.files):>6}{table_record.rows:>12,}")
        print(f"core built {core_state.built_at} by {core_state.run_id}")
        changed_inputs = list_changed_clean_inputs(lake, core_state)
        if changed_inputs:
            print(f"core is behind clean: {len(changed_inputs)} clean file(s) changed since; run build-core")


def _cmd_build_core(args: argparse.Namespace) -> None:
    del args
    lake = _open_lake()
    summary = build_core_tables(lake)
    print(f"{summary.run_id}: ok")
    for table_name, row_count in summary.row_counts_by_table.items():
        print(f"  core/{table_name:<22}{row_count:>11,} rows")
    print(f"  checks: {len(summary.report.outcomes)} run, 0 errors, {len(summary.report.warnings)} warning(s)")
    for warning in summary.report.warnings:
        print(f"  WARN {format_check_outcome(warning)}")


def _cmd_verify(args: argparse.Namespace) -> None:
    catalog = load_catalog(args.catalog)
    lake = _open_lake()
    verification = verify_lake(catalog, lake, args.datasets, args.deep)
    errors = list(verification.errors)
    checked_file_count = verification.checked_file_count
    if args.datasets is None and not errors:
        core_checked_count, core_errors = verify_core_tables(lake, deep=args.deep)
        checked_file_count += core_checked_count
        errors.extend(core_errors)
    if errors:
        _exit_with_errors(errors)
    print(f"verified {checked_file_count} file(s): ok")


def _cmd_unlock(args: argparse.Namespace) -> None:
    del args  # the catalog is not read, so a broken one cannot keep a lock in place
    lake = _open_lake()
    try:
        lake.lock_path.unlink()
    except FileNotFoundError:
        print("no lock to remove")
    else:
        print(f"removed {lake.lock_path}")


def _build_parser() -> argparse.ArgumentParser:
    # Option names must be written in full: a shortened one would change meaning when a new option is added.
    parser = argparse.ArgumentParser(
        prog="python -m nfl_lake",
        description="Ingest nflverse files into the local lake.",
        allow_abbrev=False,
    )
    parser.add_argument("--catalog", default="catalog/datasets.yml", help="Path to the dataset catalog.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_command(
        name: str, handler: Callable[[argparse.Namespace], None], help_text: str
    ) -> argparse.ArgumentParser:
        command_parser = subparsers.add_parser(name, help=help_text, allow_abbrev=False)
        command_parser.set_defaults(handler=handler)
        return command_parser

    update_parser = add_command(
        "update",
        _cmd_update,
        "Fetch what changed upstream, rebuild the clean layer, and publish each passing dataset.",
    )
    update_parser.add_argument(
        "--datasets", type=_parse_dataset_names, help="Comma-separated dataset names. Default: every dataset."
    )
    update_parser.add_argument(
        "--skip",
        type=_parse_dataset_names,
        help="Comma-separated dataset names to leave out, e.g. one that is broken upstream.",
    )
    update_parser.add_argument(
        "--seasons",
        type=_parse_season_filter,
        help="Seasons to consider: 2026, 2015-2026 or 2024,2026. Default: all.",
    )
    update_parser.add_argument(
        "--accept-schema-change",
        action="store_true",
        help="Publish files whose columns were removed or changed type.",
    )
    update_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched and rebuilt, and change nothing.",
    )
    update_parser.add_argument("--workers", type=_parse_workers, default=8, help="Parallel downloads (1-16).")

    add_command(
        "build-core",
        _cmd_build_core,
        "Build the 7 normalized core tables into data/core/ behind Strategy 1 checks.",
    )

    add_command(
        "status", _cmd_status, "Show what is published per dataset (and core table), and how the last run ended."
    )

    verify_parser = add_command(
        "verify",
        _cmd_verify,
        "Re-check every published clean file and (when built) core table: present, row count, keys, and Strategy 1 checks.",
    )
    verify_parser.add_argument(
        "--datasets", type=_parse_dataset_names, help="Comma-separated dataset names. Default: every dataset."
    )
    verify_parser.add_argument(
        "--deep",
        action="store_true",
        help="Also re-hash every clean file, set-aside file, raw snapshot, and published core file.",
    )

    add_command(
        "unlock",
        _cmd_unlock,
        "Clear the lock left behind by an update that was killed. Only use when no update is running.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    _configure_logging()
    with _handle_cli_errors():
        args.handler(args)
