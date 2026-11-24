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


def _open_source(catalog: Catalog, lake: LakeStore, *, dry_run: bool = False) -> UpstreamSource:
    """Upstream source: NFL_LAKE_SOURCE_DIR reads a local mirror instead of GitHub."""
    local_source_dir = os.getenv("NFL_LAKE_SOURCE_DIR")
    if local_source_dir:
        return LocalDirSource(local_source_dir)
    return GitHubReleasesSource(
        catalog.source.repo,
        token=os.getenv("GITHUB_TOKEN") or None,
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
                if start_season > end_season:
                    raise ValueError
                seasons.update(range(start_season, end_season + 1))
            elif token.strip():
                seasons.add(int(token))
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
            publish_outcome = (
                f"{error.published_file_count} file(s) were published; "
                "the failing dataset(s) were left unchanged — run update again"
            )
        else:
            publish_outcome = "nothing was published; the clean layer is unchanged"
        _exit_with_errors(error.errors + [publish_outcome])
    except (LakeLocked, SourceError, ValueError, OSError, yaml.YAMLError) as error:
        _exit_with_errors([str(error)])


def _cmd_update(args: argparse.Namespace) -> None:
    catalog = load_catalog(args.catalog)
    lake = _open_lake()
    source = _open_source(catalog, lake, dry_run=args.dry_run)
    selected_datasets = catalog.select_datasets(args.datasets, exclude_names=args.skip)
    if args.skip is not None and not selected_datasets:
        raise ValueError("--skip leaves no dataset to update")
    selected_dataset_names = (
        None if args.datasets is None and args.skip is None else [dataset.name for dataset in selected_datasets]
    )
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
        print(
            f"dry run: {summary.target_file_count} files in {summary.dataset_count} datasets; "
            f"would fetch {len(summary.fetched_file_ids)} ({fetched_megabytes:.1f} MB) "
            f"and rebuild {len(summary.rebuilt_file_ids)}"
        )
        for file_id in summary.fetched_file_ids:
            print(f"  fetch {file_id}")
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


def _cmd_verify(args: argparse.Namespace) -> None:
    catalog = load_catalog(args.catalog)
    lake = _open_lake()
    verification = verify_lake(catalog, lake, args.datasets, args.deep)
    if verification.errors:
        _exit_with_errors(verification.errors)
    print(f"verified {verification.checked_file_count} file(s): ok")


def _cmd_unlock(args: argparse.Namespace) -> None:
    del args  # the catalog is not read, so a broken one cannot keep a lock in place
    lake = _open_lake()
    if lake.lock_path.exists():
        lake.lock_path.unlink()
        print(f"removed {lake.lock_path}")
    else:
        print("no lock to remove")


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

    add_command("status", _cmd_status, "Show what is published per dataset, and how the last run ended.")

    verify_parser = add_command(
        "verify", _cmd_verify, "Re-check every published file: present, expected row count, key unique and non-null."
    )
    verify_parser.add_argument(
        "--datasets", type=_parse_dataset_names, help="Comma-separated dataset names. Default: every dataset."
    )
    verify_parser.add_argument(
        "--deep", action="store_true", help="Also re-hash every clean file and every raw snapshot."
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
