# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

import concurrent.futures
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from filelock import FileLock, Timeout, BaseFileLock
import structlog
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .config import DatasetCatalog, DatasetConfig
from .logging_setup import configure_logging, log_run_event
from .lineage import load_lineage, save_lineage, update_dataset_lineage, record_partition_counts
from . import version
from . import importers
from . import promote


logger = structlog.get_logger(__name__)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _select_datasets(catalog: DatasetCatalog, filter_csv: Optional[str]) -> List[DatasetConfig]:
    selected = []
    allow = None
    if filter_csv:
        allow = {x.strip() for x in filter_csv.split(",") if x.strip()}
    for name, cfg in catalog.datasets.items():
        if not cfg.enabled:
            continue
        if allow and name not in allow:
            continue
        selected.append(cfg)
    return selected


def _lock_guard(root: str) -> BaseFileLock:
    # filelock returns a BaseFileLock, which is compatible with FileLock usage
    return FileLock(str(Path(root).parent / ".lake.lock"))


def _run_dataset_bootstrap(root: str, cfg: DatasetConfig, years: str, no_validate: bool) -> tuple[int, list[str], dict]:
    df = importers.fetch_dataset_bootstrap(cfg, years)
    changed_parts, _partition_stats = promote.write_bronze_and_collect(root, cfg, df)
    part_stats = promote.promote_to_silver(root, cfg, changed_parts, no_validate=no_validate)
    return len(df), changed_parts, part_stats


def _run_dataset_update(
    root: str,
    cfg: DatasetConfig,
    season: int,
    no_validate: bool,
    since: Optional[str],
) -> tuple[int, list[str], dict]:
    df = importers.fetch_dataset_update(cfg, season=season, since=since)
    changed_parts, _partition_stats = promote.write_bronze_and_collect(root, cfg, df)
    part_stats = promote.promote_to_silver(root, cfg, changed_parts, no_validate=no_validate)
    return len(df), changed_parts, part_stats


def run_bootstrap(
    root: str,
    catalog: DatasetCatalog,
    years: str,
    datasets: Optional[str],
    max_workers: int,
    no_validate: bool,
) -> None:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    with _lock_guard(root):
        selected = _select_datasets(catalog, datasets)
        lineage = load_lineage()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for cfg in selected:
                try:
                    log_run_event(run_id, "submit", dataset=cfg.name, flow="bootstrap")
                    futures[pool.submit(_run_dataset_bootstrap, root, cfg, years, no_validate)] = cfg.name
                except Exception as exc:
                    logger.error("dataset_submit_failed", dataset=cfg.name, error=str(exc))
            for fut in concurrent.futures.as_completed(futures):
                name = futures[fut]
                try:
                    rows, parts, part_stats = fut.result()
                    log_run_event(run_id, "completed", dataset=name, rows=rows, parts=parts)
                except Exception as exc:
                    logger.error("dataset_run_failed", dataset=name, error=str(exc))
                    log_run_event(run_id, "failed", dataset=name, error=str(exc))
                    rows, parts, part_stats = 0, [], {}
                lineage = update_dataset_lineage(
                    lineage,
                    dataset=name,
                    last_ingest_utc=_now_utc_iso(),
                    rows_last_batch=rows,
                    changed_partitions=parts,
                    partition_stats=part_stats,
                )
                for part in parts:
                    # If we have per-partition stats, prefer those row counts; else fallback to rows
                    rc = None
                    if isinstance(part_stats, dict):
                        st = part_stats.get(part)
                        if st is not None and hasattr(st, "row_count"):
                            rc = int(getattr(st, "row_count"))
                    lineage = record_partition_counts(lineage, name, part, int(rc) if rc is not None else rows)
        save_lineage(lineage)


def run_update(
    root: str,
    catalog: DatasetCatalog,
    season: int,
    datasets: Optional[str],
    max_workers: int,
    no_validate: bool,
    since: Optional[str],
) -> None:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    with _lock_guard(root):
        selected = _select_datasets(catalog, datasets)
        lineage = load_lineage()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for cfg in selected:
                try:
                    log_run_event(run_id, "submit", dataset=cfg.name, flow="update", season=season)
                    futures[pool.submit(_run_dataset_update, root, cfg, season, no_validate, since)] = cfg.name
                except Exception as exc:
                    logger.error("dataset_submit_failed", dataset=cfg.name, error=str(exc))
            for fut in concurrent.futures.as_completed(futures):
                name = futures[fut]
                try:
                    rows, parts, part_stats = fut.result()
                    log_run_event(run_id, "completed", dataset=name, rows=rows, parts=parts)
                except Exception as exc:
                    logger.error("dataset_run_failed", dataset=name, error=str(exc))
                    log_run_event(run_id, "failed", dataset=name, error=str(exc))
                    rows, parts, part_stats = 0, [], {}
                lineage = update_dataset_lineage(
                    lineage,
                    dataset=name,
                    last_ingest_utc=_now_utc_iso(),
                    rows_last_batch=rows,
                    changed_partitions=parts,
                    partition_stats=part_stats,
                )
                for part in parts:
                    rc = None
                    if isinstance(part_stats, dict):
                        st = part_stats.get(part)
                        if st is not None and hasattr(st, "row_count"):
                            rc = int(getattr(st, "row_count"))
                    lineage = record_partition_counts(lineage, name, part, int(rc) if rc is not None else rows)
        save_lineage(lineage)


def run_recache_pbp(root: str, catalog: DatasetCatalog, season: int) -> None:
    cfg = catalog.datasets.get("pbp")
    if not cfg:
        logger.warning("pbp dataset not configured")
        return
    rows, parts, part_stats = _run_dataset_update(root, cfg, season, no_validate=False, since=None)
    lineage = load_lineage()
    lineage = update_dataset_lineage(
        lineage,
        dataset="pbp",
        last_ingest_utc=_now_utc_iso(),
        rows_last_batch=rows,
        changed_partitions=parts,
        partition_stats=part_stats,
    )
    save_lineage(lineage)

