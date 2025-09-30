# pyright: reportMissingImports=false, reportMissingModuleSource=false
import os
from typing import List, Optional
from datetime import datetime, timezone

import typer
from dotenv import load_dotenv

from .config import load_dataset_catalog, DatasetConfig
from .logging_setup import configure_logging
from .lineage import load_lineage, save_lineage, update_dataset_lineage, record_partition_counts

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _resolve_root_from_env(default_root: str) -> str:
    env_root = os.getenv("LAKE_ROOT")
    return env_root or default_root


@app.callback()
def main() -> None:
    load_dotenv()
    configure_logging()


@app.command()
def bootstrap(
    years: str = typer.Option("1999-2024", help="Year range, e.g. 1999-2024 or comma list"),
    datasets: Optional[str] = typer.Option(None, help="Comma-separated dataset filter"),
    max_workers: int = typer.Option(2, help="Max parallel dataset workers"),
    no_validate: bool = typer.Option(False, help="Skip validation"),
) -> None:
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    # Lazy import to avoid heavy deps during --help
    from .orchestration import run_bootstrap
    run_bootstrap(root, catalog, years, datasets, max_workers, no_validate)


@app.command()
def update(
    season: int = typer.Option(..., help="Season to update"),
    datasets: Optional[str] = typer.Option(None, help="Comma-separated dataset filter"),
    max_workers: int = typer.Option(2, help="Max parallel dataset workers"),
    no_validate: bool = typer.Option(False, help="Skip validation"),
    since: Optional[str] = typer.Option(None, help="YYYY-MM-DD lower bound for fetching"),
) -> None:
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    from .orchestration import run_update
    run_update(root, catalog, season, datasets, max_workers, no_validate, since)


@app.command("recache-pbp")
def recache_pbp(
    season: int = typer.Option(..., help="Season to re-pull for corrections"),
) -> None:
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    from .orchestration import run_recache_pbp
    run_recache_pbp(root, catalog, season)


@app.command()
def profile(
    layer: str = typer.Option("silver", help="Layer to profile: bronze or silver"),
    datasets: Optional[str] = typer.Option(None, help="Comma-separated dataset filter"),
    values: Optional[str] = typer.Option(None, help="Limit to partition values (comma-separated), e.g. 1999,2000"),
) -> None:
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    from .profiling import run_profile

    limit_values = [v.strip() for v in values.split(",")] if values else None
    written = run_profile(root, catalog, datasets, layer, limit_values)
    for ds, lyr, part in written:
        typer.echo(f"profiled: {ds} {lyr} {part}")


@app.command()
def promote(
    datasets: Optional[str] = typer.Option(None, help="Comma-separated dataset filter"),
    values: Optional[str] = typer.Option(None, help="Limit to partition values (comma-separated), e.g. 1999,2000"),
    no_validate: bool = typer.Option(False, help="Skip validation"),
) -> None:
    """Promote existing bronze partitions to silver without re-fetching."""
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    # Lazy imports to avoid heavy deps during --help
    from .profiling import _iter_partitions
    from .promote import promote_to_silver

    allow = {x.strip() for x in datasets.split(",") if x.strip()} if datasets else None
    selected: List[DatasetConfig] = []
    for name, cfg in catalog.datasets.items():
        if not cfg.enabled:
            continue
        if allow and name not in allow:
            continue
        selected.append(cfg)

    limit_values = [v.strip() for v in values.split(",")] if values else None
    lineage = load_lineage()
    for cfg in selected:
        changed_parts = _iter_partitions(root, cfg.name, "bronze", cfg.partitions, limit_values)
        part_stats = promote_to_silver(root, cfg, changed_parts, no_validate=no_validate)
        # Update lineage with stats and counts
        lineage = update_dataset_lineage(
            lineage,
            dataset=cfg.name,
            last_ingest_utc=datetime.now(timezone.utc).isoformat(),
            rows_last_batch=sum(st.row_count for st in part_stats.values()) if isinstance(part_stats, dict) else 0,
            changed_partitions=changed_parts,
            partition_stats=part_stats if isinstance(part_stats, dict) else None,
        )
        for part in changed_parts:
            rc = 0
            if isinstance(part_stats, dict):
                st = part_stats.get(part)
                if st is not None and hasattr(st, "row_count"):
                    rc = int(getattr(st, "row_count"))
            lineage = record_partition_counts(lineage, cfg.name, part, rc)
        save_lineage(lineage)
        for part in changed_parts:
            rc = 0
            if isinstance(part_stats, dict):
                st = part_stats.get(part)
                if st is not None and hasattr(st, "row_count"):
                    rc = int(getattr(st, "row_count"))
            typer.echo(f"promoted: {cfg.name} silver <- bronze {part} rows={rc}")


@app.command()
def inseason(
    season: int = typer.Option(..., help="Season to update"),
    no_validate: bool = typer.Option(False, help="Skip validation for unstable feeds"),
    max_workers: int = typer.Option(2, help="Max parallel dataset workers"),
    retry_attempts: int = typer.Option(3, help="Retry attempts for 404-prone datasets"),
    retry_base_seconds: int = typer.Option(5, help="Base seconds for backoff"),
) -> None:
    """Convenience command: run safe datasets immediately, then 404-prone datasets with retry."""
    catalog = load_dataset_catalog()
    root = _resolve_root_from_env(catalog.root)
    from .orchestration import run_update
    # Safe now
    safe = "schedules,rosters,rosters_seasonal,players,ids,ngs_weekly,pbp"
    # Unstable early Monday
    unstable = "weekly,injuries,depth_charts,snap_counts"

    # Inject retry options into importer options (read by importers where supported)
    # We pass via environment variables to avoid changing many signatures
    import os
    os.environ["IMPORTER_RETRY_ATTEMPTS"] = str(retry_attempts)
    os.environ["IMPORTER_RETRY_BASE_SECONDS"] = str(retry_base_seconds)

    # First pass: safe datasets
    run_update(root, catalog, season, datasets=safe, max_workers=max_workers, no_validate=no_validate, since=None)
    # Second pass: unstable datasets (may still skip if upstream not ready)
    run_update(root, catalog, season, datasets=unstable, max_workers=max_workers, no_validate=no_validate, since=None)

if __name__ == "__main__":
    app()

