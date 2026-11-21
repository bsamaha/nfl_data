"""Command line: `python -m nfl_lake <command>`."""
from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from typing import Iterator, NoReturn

import structlog
import typer
import yaml
from dotenv import load_dotenv

from .catalog import Catalog, load_catalog
from .pipeline import UpdateFailed, run_update, status as lake_status, verify as lake_verify
from .source import GitHubReleasesSource, LocalDirSource, Source, SourceError
from .store import Lake, LakeLocked

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Ingest nflverse files into the local lake.")
_opts: dict[str, str] = {}


@app.callback()
def main(catalog: str = typer.Option("catalog/datasets.yml", help="Path to the dataset catalog.")) -> None:
    _opts["catalog"] = catalog
    load_dotenv()
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    structlog.configure(
        processors=[structlog.processors.add_log_level, structlog.processors.TimeStamper(fmt="%H:%M:%S"),
                    structlog.dev.ConsoleRenderer(colors=False)],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        # Looked up on every call, so progress goes to whatever stderr is at that moment.
        logger_factory=lambda *args: structlog.PrintLogger(file=sys.stderr),
    )


def _setup() -> tuple[Catalog, Lake, Source]:
    """Catalog, lake and source. NFL_LAKE_ROOT moves the lake; NFL_LAKE_SOURCE_DIR reads a local mirror instead of GitHub."""
    catalog = load_catalog(_opts["catalog"])
    lake = Lake(os.getenv("NFL_LAKE_ROOT") or "data")
    mirror = os.getenv("NFL_LAKE_SOURCE_DIR")
    source: Source
    if mirror:
        source = LocalDirSource(mirror)
    else:
        source = GitHubReleasesSource(catalog.source.repo, token=os.getenv("GITHUB_TOKEN") or None)
    return catalog, lake, source


def _names(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _seasons(value: str | None) -> set[int] | None:
    if not value:
        return None
    out: set[int] = set()
    try:
        for part in value.split(","):
            if "-" in part:
                lo, hi = (int(x) for x in part.split("-", 1))
                if lo > hi:
                    raise ValueError
                out.update(range(lo, hi + 1))
            elif part.strip():
                out.add(int(part))
        if not out:
            raise ValueError
    except ValueError:
        raise typer.BadParameter("use a season, a range or a list: 2026, 2015-2026 or 2024,2026") from None
    return out


def _fail(lines: list[str]) -> NoReturn:
    for line in lines:
        typer.echo(f"ERROR {line}", err=True)
    raise typer.Exit(1)


@contextmanager
def _reporting() -> Iterator[None]:
    """Turn a failure the user can act on into ERROR lines and exit code 1, with no traceback."""
    try:
        yield
    except UpdateFailed as e:
        if e.published:
            outcome = f"{e.published} file(s) were published before the failure; run update again"
        else:
            outcome = "nothing was published; the clean layer is unchanged"
        _fail(e.errors + [outcome])
    except (LakeLocked, SourceError, ValueError, OSError, yaml.YAMLError) as e:
        _fail([str(e)])


@app.command()
def update(
    datasets: str | None = typer.Option(None, help="Comma-separated dataset names. Default: every dataset."),
    skip: str | None = typer.Option(None, help="Comma-separated dataset names to leave out, e.g. one that is broken upstream."),
    seasons: str | None = typer.Option(None, help="Seasons to consider: 2026, 2015-2026 or 2024,2026. Default: all."),
    accept_schema_change: bool = typer.Option(False, "--accept-schema-change", help="Publish files whose columns were removed or changed type."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be fetched and rebuilt, and change nothing."),
    workers: int = typer.Option(4, min=1, max=16, help="Parallel downloads."),
) -> None:
    """Fetch every file that changed upstream, rebuild the clean layer from it, and publish only if every file passed.

    The first run downloads everything. Later runs download only files whose upstream checksum changed.
    """
    with _reporting():
        catalog, lake, source = _setup()
        chosen = _names(datasets)
        selected = catalog.select(chosen)  # an unknown name stops here, before upstream is listed or a run recorded
        if skip:
            left_out = {d.name for d in catalog.select(_names(skip))}
            chosen = [d.name for d in selected if d.name not in left_out]
            if not chosen:
                raise ValueError("--skip leaves no dataset to update")
        s = run_update(catalog, source, lake, datasets=chosen, seasons=_seasons(seasons),
                       accept_schema_change=accept_schema_change, dry_run=dry_run, workers=workers)
        mb = s.bytes_fetched / 1e6
        if s.dry_run:
            typer.echo(f"dry run: {s.listed} files in {s.dataset_count} datasets; would fetch {len(s.fetched)} ({mb:.1f} MB) and rebuild {len(s.built)}")
            for file_id in s.fetched:
                typer.echo(f"  fetch {file_id}")
            return
        typer.echo(f"{s.run_id}: ok")
        typer.echo(f"  {s.listed} files in {s.dataset_count} datasets: fetched {len(s.fetched)} ({mb:.1f} MB), rebuilt {len(s.built)}, unchanged {s.unchanged}")
        if s.rejected:
            typer.echo(f"  rows set aside for a null key: {sum(s.rejected.values())} in {len(s.rejected)} file(s), see {lake.rejects}")
        if s.deduped:
            typer.echo(f"  repeated keys resolved: {sum(s.deduped.values())} row(s) in {len(s.deduped)} file(s)")
        if s.pruned:
            typer.echo(f"  old raw versions removed: {s.pruned}")
        for note in s.notes:
            typer.echo(f"  note: {note}")


@app.command()
def status() -> None:
    """Show what is published per dataset, and how the last run ended."""
    with _reporting():
        catalog, lake, _ = _setup()
        rows, last = lake_status(catalog, lake)
        typer.echo(f"lake: {lake.root.resolve()}")
        typer.echo(f"{'dataset':<20}{'files':>6}  {'seasons':<10}{'rows':>11}{'set aside':>10}  {'upstream updated':<21}built")
        for r in rows:
            typer.echo(f"{r['dataset']:<20}{r['files']:>6}  {r['seasons']:<10}{r['rows']:>11,}{r['rejected']:>10,}  {r['upstream']:<21}{r['built']}")
        if last is None:
            typer.echo("no runs recorded yet")
        else:
            typer.echo(f"last run {last['run_id']}: {last['status']} at {last['finished_at']}, fetched {last['fetched']}, rebuilt {last['built']}")
            for err in last["errors"]:
                typer.echo(f"  ERROR {err}")


@app.command()
def verify(
    datasets: str | None = typer.Option(None, help="Comma-separated dataset names. Default: every dataset."),
    deep: bool = typer.Option(False, "--deep", help="Also re-hash each raw file against the manifest."),
) -> None:
    """Re-check every published file: present, expected row count, key unique and non-null."""
    with _reporting():
        catalog, lake, _ = _setup()
        checked, errors = lake_verify(catalog, lake, _names(datasets), deep)
        if errors:
            _fail(errors)
        typer.echo(f"verified {checked} file(s): ok")


@app.command()
def unlock() -> None:
    """Clear the lock left behind by an update that was killed. Only use it when no update is running."""
    with _reporting():
        _, lake, _ = _setup()
        if lake.lock_path.exists():
            lake.lock_path.unlink()
            typer.echo(f"removed {lake.lock_path}")
        else:
            typer.echo("no lock to remove")
