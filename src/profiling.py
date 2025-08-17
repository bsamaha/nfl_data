# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import json
import polars as pl

from .config import DatasetCatalog, DatasetConfig


def _iter_partitions(root: str, dataset: str, layer: str, partition_keys: List[str], limit_values: Optional[List[str]] = None) -> List[str]:
    base = Path(root) / layer / dataset
    if not partition_keys:
        return [""]
    # Only support single key for now (year or season)
    key = partition_keys[0]
    parts = []
    if base.exists():
        for child in base.iterdir():
            if not child.is_dir():
                continue
            name = child.name
            if not name.startswith(f"{key}="):
                continue
            if limit_values and name.split("=", 1)[1] not in limit_values:
                continue
            parts.append(name)
    return sorted(parts)


def _read_partition_as_polars(root: str, dataset: str, layer: str, partition: str) -> pl.DataFrame:
    target = Path(root) / layer / dataset
    if partition:
        target = target / partition
    # Scan the partition directory; use hive partitioning and upcast Null dtypes
    lf = pl.scan_parquet(str(target), hive_partitioning=True)
    schema = lf.collect_schema()
    null_cols = [name for name, dtype in schema.items() if dtype == pl.Null]
    if null_cols:
        lf = lf.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    return lf.collect()


def _compute_metrics(df: pl.DataFrame, key_cols: List[str]) -> Dict[str, object]:
    rows = df.height
    cols = list(df.columns)
    dtypes = {name: str(dtype) for name, dtype in df.schema.items()}
    metrics: Dict[str, object] = {
        "rows": rows,
        "num_columns": len(cols),
        "columns": cols,
        "dtypes": dtypes,
    }
    # Key nulls and duplicates
    nulls = {}
    for k in key_cols:
        if k in df.columns:
            nulls[k] = int(df.select(pl.col(k).is_null().sum()).item())
    metrics["key_nulls"] = nulls
    if all(k in df.columns for k in key_cols):
        uniq = df.select(pl.concat_list([pl.col(k) for k in key_cols]).alias("__k")).unique().height
        metrics["key_unique_rows"] = int(uniq)
        metrics["key_duplicate_rows"] = int(rows - uniq)
        metrics["key_unique_ratio"] = float(uniq / rows) if rows else 1.0
    # Optional common fields
    for fld in ["season", "year", "week"]:
        if fld in df.columns:
            try:
                metrics[f"{fld}_min"] = int(df.select(pl.col(fld).min()).item())
                metrics[f"{fld}_max"] = int(df.select(pl.col(fld).max()).item())
            except Exception:
                pass
    return metrics


def run_profile(
    root: str,
    catalog: DatasetCatalog,
    datasets_filter: Optional[str],
    layer: str,
    limit_values: Optional[List[str]],
    output_dir: str = "catalog/quality",
) -> List[Tuple[str, str, str]]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    selected = []
    allow = None
    if datasets_filter:
        allow = {x.strip() for x in datasets_filter.split(",") if x.strip()}
    for name, cfg in catalog.datasets.items():
        if allow and name not in allow:
            continue
        selected.append(cfg)

    written: List[Tuple[str, str, str]] = []
    for cfg in selected:
        partitions = _iter_partitions(root, cfg.name, layer, cfg.partitions, limit_values)
        if not partitions:
            partitions = [""]
        for part in partitions:
            df = _read_partition_as_polars(root, cfg.name, layer, part)
            metrics = _compute_metrics(df, cfg.key)
            rec = {
                "dataset": cfg.name,
                "layer": layer,
                "partition": part or "all",
                "metrics": metrics,
            }
            ds_out = out / cfg.name
            ds_out.mkdir(parents=True, exist_ok=True)
            part_name = part.replace("/", "_") or "all"
            fpath = ds_out / f"{layer}_{part_name}.json"
            fpath.write_text(json.dumps(rec, indent=2))
            written.append((cfg.name, layer, part or "all"))
    return written


