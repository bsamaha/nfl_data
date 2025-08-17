# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import polars as pl

from .config import DatasetConfig
from .io import ensure_dir, write_parquet_dataset, remove_dir, move_replace
from .schemas import validate_bronze, validate_silver
from .transforms import to_silver
from .lineage import PartitionStats, compute_sha256_for_keys


def discover_changed_partitions(df: pd.DataFrame, partitions: List[str]) -> List[str]:
    if not partitions:
        return ["all"]
    keys = []
    for vals in df[partitions].drop_duplicates().itertuples(index=False, name=None):
        part = "".join([f"{col}={val}/" for col, val in zip(partitions, vals)])
        keys.append(part.rstrip("/"))
    return keys


def write_bronze_and_collect(
    root: str, cfg: DatasetConfig, df: pd.DataFrame, run_id: Optional[str] = None, ingested_at_iso: Optional[str] = None
) -> Tuple[List[str], Dict[str, PartitionStats]]:
    # Stamp minimal metadata columns
    # Vectorized assignment to avoid pandas fragmentation
    if True:
        new_cols: Dict[str, object] = {}
        if "source" not in df.columns:
            new_cols["source"] = "nflverse"
        if "pipeline_version" not in df.columns:
            new_cols["pipeline_version"] = "0.1.0"
        if run_id is not None and "run_id" not in df.columns:
            new_cols["run_id"] = run_id
        if ingested_at_iso is not None and "ingested_at" not in df.columns:
            new_cols["ingested_at"] = ingested_at_iso
        if new_cols:
            df = pd.concat([df, pd.DataFrame({k: [v] * len(df) for k, v in new_cols.items()})], axis=1)
    # Normalize partition columns to stable dtypes (avoid '2009.0' partition names)
    for part_col in cfg.partitions:
        if part_col in df.columns:
            try:
                df[part_col] = pd.to_numeric(df[part_col], errors="coerce").astype("Int64")
            except Exception:
                # Fallback to string if numeric cast fails
                df[part_col] = df[part_col].astype(str)
    changed = discover_changed_partitions(df, cfg.partitions)
    write_parquet_dataset(
        df,
        root=root,
        dataset=cfg.name,
        layer="bronze",
        partitions=cfg.partitions,
        max_rows_per_file=cfg.max_rows_per_file,
    )
    # Minimal partition stats (row counts only for now)
    part_stats = {part: PartitionStats(row_count=int(len(df)), sha256_fingerprint="") for part in changed}
    return changed, part_stats


def promote_to_silver(
    root: str,
    cfg: DatasetConfig,
    changed_partitions: List[str],
    no_validate: bool,
) -> Dict[str, PartitionStats]:
    bronze_root = Path(root) / "bronze" / cfg.name
    stats_by_part: Dict[str, PartitionStats] = {}
    for part in changed_partitions or [""]:
        part_path = bronze_root / part if part else bronze_root
        if not part_path.exists():
            continue
        # Read only the changed partition to avoid cross-partition schema conflicts
        lf_bronze = pl.scan_parquet(str(part_path), hive_partitioning=True)
        schema = lf_bronze.collect_schema()
        null_cols = [name for name, dtype in schema.items() if dtype == pl.Null]
        if null_cols:
            lf_bronze = lf_bronze.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
        df_bronze = lf_bronze.collect()
        # Ensure partition columns exist even if hive parsing did not materialize them
        if part:
            const_assignments = {}
            for seg in part.split("/"):
                if not seg:
                    continue
                if "=" not in seg:
                    continue
                k, v = seg.split("=", 1)
                # try cast numeric to int, else keep as str
                try:
                    v_cast = int(v)
                except ValueError:
                    v_cast = v
                const_assignments[k] = v_cast
            if const_assignments:
                df_bronze = df_bronze.with_columns(
                    [pl.lit(val).alias(key) for key, val in const_assignments.items() if key not in df_bronze.columns]
                )
        if not no_validate:
            validate_bronze(cfg.name, df_bronze)

        # Load existing silver partition if present and align schemas, then merge
        existing_path = Path(root) / "silver" / cfg.name / part
        if existing_path.exists():
            lf_existing = pl.scan_parquet(str(existing_path), hive_partitioning=True)
            schema_existing = lf_existing.collect_schema()
            # Upcast any Null-typed columns
            null_cols_e = [name for name, dtype in schema_existing.items() if dtype == pl.Null]
            if null_cols_e:
                lf_existing = lf_existing.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols_e])
            df_existing = lf_existing.collect()

            # Align schemas: add missing columns with appropriate dtypes
            cols_union = set(df_existing.columns) | set(df_bronze.columns)
            bronze_schema = df_bronze.schema
            existing_schema = df_existing.schema

            def align(df: pl.DataFrame, src_schema: dict, other_schema: dict) -> pl.DataFrame:
                missing = [c for c in cols_union if c not in df.columns]
                if missing:
                    df = df.with_columns(
                        [
                            pl.lit(None).cast(other_schema.get(c, pl.Utf8)).alias(c)
                            for c in missing
                        ]
                    )
                # Reorder to a stable union order
                return df.select([pl.col(c) for c in sorted(cols_union)])

            df_bronze_aligned = align(df_bronze, bronze_schema, existing_schema)
            df_existing_aligned = align(df_existing, existing_schema, bronze_schema)

            # Harmonize dtypes across both frames before concat to avoid SchemaError
            def is_int_dtype(dt: object) -> bool:
                return dt in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)

            def is_float_dtype(dt: object) -> bool:
                return dt in (pl.Float32, pl.Float64)

            def choose_common_dtype(a: Optional[object], b: Optional[object]) -> object:
                if a is None and b is None:
                    return pl.Utf8
                if a is None:
                    return b if b != pl.Null else pl.Utf8
                if b is None:
                    return a if a != pl.Null else pl.Utf8
                if a == b:
                    return a
                if a == pl.Null:
                    return b
                if b == pl.Null:
                    return a
                if a == pl.Utf8 or b == pl.Utf8:
                    return pl.Utf8
                if (is_int_dtype(a) and is_int_dtype(b)):
                    return pl.Int64
                if ((is_int_dtype(a) or is_float_dtype(a)) and (is_int_dtype(b) or is_float_dtype(b))):
                    return pl.Float64
                # Fallback to Utf8 for mixed/unknown types
                return pl.Utf8

            target_dtypes: Dict[str, Any] = {}
            for c in cols_union:
                target_dtypes[c] = choose_common_dtype(bronze_schema.get(c), existing_schema.get(c))

            def cast_to(df: pl.DataFrame, targets: Dict[str, Any]) -> pl.DataFrame:
                casts = []
                for c, dt in targets.items():
                    if c in df.columns:
                        cur = df.schema.get(c)
                        if cur != dt:
                            casts.append(pl.col(c).cast(dt, strict=False).alias(c))
                return df.with_columns(casts) if casts else df

            df_bronze_casted = cast_to(df_bronze_aligned, target_dtypes)
            df_existing_casted = cast_to(df_existing_aligned, target_dtypes)
            df_merged_raw = pl.concat([df_existing_casted, df_bronze_casted], how="vertical", rechunk=True)
        else:
            df_merged_raw = df_bronze

        df_silver = to_silver(cfg.name, df_merged_raw)
        if not no_validate:
            validate_silver(cfg.name, df_silver)

        # Compute lineage stats from silver frame
        part_key = part or "all"
        # Row count
        row_count = int(df_silver.height)
        # Fingerprint on keys
        keys = [k for k in cfg.key if k in df_silver.columns]
        if keys:
            key_series = (
                df_silver.select(
                    pl.concat_str([pl.col(k).cast(pl.Utf8) for k in keys], separator="|").alias("__k")
                )
                .to_series()
                .to_list()
            )
            fp = compute_sha256_for_keys(key_series)
        else:
            fp = ""
        # Min/max ingested_at if present
        min_ing: Optional[str] = None
        max_ing: Optional[str] = None
        if "ingested_at" in df_silver.columns:
            try:
                min_ing = str(df_silver.select(pl.col("ingested_at").min()).item())
                max_ing = str(df_silver.select(pl.col("ingested_at").max()).item())
            except Exception:
                pass
        stats_by_part[part_key] = PartitionStats(
            row_count=row_count,
            sha256_fingerprint=fp,
            max_ingested_at=max_ing,
            min_ingested_at=min_ing,
        )

        # Atomic staging: write into _staging then move/replace only the changed partition
        staging_dir = Path(root) / "silver" / "_staging" / cfg.name
        remove_dir(staging_dir)
        write_parquet_dataset(
            df_silver.to_pandas(),
            root=str(Path(root) / "silver" / "_staging"),
            dataset=cfg.name,
            layer="",
            partitions=cfg.partitions,
            sort_by=cfg.sort_by,
            max_rows_per_file=cfg.max_rows_per_file,
        )
        # Move only the partition directory to avoid clobbering other partitions
        if part:
            staging_part_dir = staging_dir / part
            target_part_dir = Path(root) / "silver" / cfg.name / part
            if staging_part_dir.exists():
                move_replace(staging_part_dir, target_part_dir)
            else:
                # Fallback: move entire staging dataset dir contents (should only include this partition)
                for child in staging_dir.iterdir() if staging_dir.exists() else []:
                    if child.is_dir():
                        move_replace(child, Path(root) / "silver" / cfg.name / child.name)
        else:
            # No explicit partition: replace entire dataset (initial bulk write)
            target_dir = Path(root) / "silver" / cfg.name
            move_replace(staging_dir, target_dir)

    return stats_by_part

