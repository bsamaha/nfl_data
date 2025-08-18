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
            # Try numeric, but only keep it if conversion didn't null-out non-null values.
            series = df[part_col]
            numeric = pd.to_numeric(series, errors="coerce")
            orig_non_null = series.notna().sum()
            numeric_non_null = numeric.notna().sum()
            if orig_non_null > 0 and numeric_non_null == orig_non_null:
                df[part_col] = numeric.astype("Int64")
            else:
                df[part_col] = series.astype(str)
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

        # Dataset-specific enrichments that may require reading other silver tables
        if cfg.name == "weekly":
            # If player_name missing, attempt to enrich from silver/rosters for this season/partition
            # Determine season value from partition path (e.g., season=2020)
            season_val = None
            if part:
                for seg in part.split("/"):
                    if seg.startswith("season="):
                        try:
                            season_val = int(seg.split("=", 1)[1])
                        except Exception:
                            season_val = seg.split("=", 1)[1]
                        break
            if season_val is not None:
                # Prefer seasonal_rosters for stable full_name; fallback to rosters
                rost_dir_seasonal = Path(root) / "silver" / "rosters_seasonal" / f"season={season_val}"
                rost_dir_weekly = Path(root) / "silver" / "rosters" / f"season={season_val}"
                if rost_dir_seasonal.exists() or rost_dir_weekly.exists():
                    try:
                        use_seasonal = rost_dir_seasonal.exists()
                        scan_path = str(rost_dir_seasonal if use_seasonal else rost_dir_weekly)
                        lf_rost = pl.scan_parquet(scan_path, hive_partitioning=True)
                        # Build best-available name from rosters
                        schema_rost = lf_rost.collect_schema()
                        cols_needed = [
                            c for c in [
                                "season",
                                "week",
                                "team",
                                "player_id",
                                "full_name",
                                "first_name",
                                "last_name",
                                "player_name",
                            ] if c in schema_rost
                        ]
                        lf_rost = lf_rost.select([pl.col(c) for c in cols_needed])
                        # Compute roster_name = coalesce(full_name, first_name||' '||last_name, player_name)
                        name_sources = []
                        if "full_name" in cols_needed:
                            name_sources.append(pl.col("full_name"))
                        if "first_name" in cols_needed and "last_name" in cols_needed:
                            name_sources.append(pl.col("first_name") + pl.lit(" ") + pl.col("last_name"))
                        if "player_name" in cols_needed:
                            name_sources.append(pl.col("player_name"))
                        if name_sources:
                            lf_rost = lf_rost.with_columns(pl.coalesce(name_sources).alias("__rost_name"))
                        else:
                            lf_rost = lf_rost.with_columns(pl.lit(None).cast(pl.Utf8).alias("__rost_name"))
                        df_rost = lf_rost.collect()
                        # For seasonal rosters, avoid joining on week (often null). Use season+player_id (+team) only.
                        if use_seasonal:
                            join_keys = [k for k in ["season", "player_id"] if k in df_silver.columns and k in df_rost.columns]
                        else:
                            join_keys = [k for k in ["season", "week", "player_id"] if k in df_silver.columns and k in df_rost.columns]
                        if "team" in df_silver.columns and "team" in df_rost.columns:
                            join_keys.append("team")
                        if join_keys and "__rost_name" in df_rost.columns:
                            df_silver = df_silver.join(df_rost.select(join_keys + ["__rost_name"]), on=join_keys, how="left")
                            # Fill player_name via coalesce: player_name, player_display_name, __rost_name
                            name_sources = []
                            if "player_name" in df_silver.columns:
                                name_sources.append(pl.col("player_name"))
                            if "player_display_name" in df_silver.columns:
                                name_sources.append(pl.col("player_display_name"))
                            name_sources.append(pl.col("__rost_name"))
                            df_silver = df_silver.with_columns(pl.coalesce(name_sources).alias("player_name"))
                            # Drop helper column
                            if "__rost_name" in df_silver.columns:
                                df_silver = df_silver.drop(["__rost_name"])
                    except Exception:
                        # Best-effort enrichment; ignore failures
                        pass
            # Fallback: join players table by gsis_id to get display_name
            if "player_name" in df_silver.columns and df_silver.select(pl.col("player_name").is_null().any()).item():
                players_dir = Path(root) / "silver" / "players"
                if players_dir.exists():
                    try:
                        df_players = pl.scan_parquet(str(players_dir)).select(
                            [c for c in ["gsis_id", "display_name", "full_name", "first_name", "last_name"] if c in pl.scan_parquet(str(players_dir)).collect_schema()]
                        ).collect()
                        if "gsis_id" in df_players.columns:
                            name_expr = None
                            if "display_name" in df_players.columns:
                                name_expr = pl.col("display_name")
                            elif "full_name" in df_players.columns:
                                name_expr = pl.col("full_name")
                            elif "first_name" in df_players.columns and "last_name" in df_players.columns:
                                name_expr = pl.col("first_name") + pl.lit(" ") + pl.col("last_name")
                            else:
                                name_expr = pl.lit(None).cast(pl.Utf8)
                            df_players = df_players.with_columns(name_expr.alias("__pl_name"))
                            if "player_id" in df_silver.columns:
                                df_silver = df_silver.join(
                                    df_players.select(["gsis_id", "__pl_name"]).rename({"gsis_id": "player_id"}),
                                    on=["player_id"], how="left",
                                )
                                df_silver = df_silver.with_columns(
                                    pl.coalesce([pl.col("player_name"), pl.col("player_display_name"), pl.col("__pl_name")]).alias("player_name")
                                ).drop(["__pl_name"])
                    except Exception:
                        pass
            # Final fallback: derive name from PBP per-season mode of names across rusher/receiver/passer
            if "player_name" in df_silver.columns and df_silver.select(pl.col("player_name").is_null().any()).item():
                season_val2 = None
                if part:
                    for seg in part.split("/"):
                        if seg.startswith("season="):
                            try:
                                season_val2 = int(seg.split("=", 1)[1])
                            except Exception:
                                season_val2 = seg.split("=", 1)[1]
                            break
                if season_val2 is not None:
                    pbp_dir = Path(root) / "silver" / "pbp" / f"year={season_val2}"
                    if pbp_dir.exists():
                        try:
                            lf_pbp = pl.scan_parquet(str(pbp_dir), hive_partitioning=True)
                            schema_pbp = lf_pbp.collect_schema()
                            selects = []
                            if {"year","rusher_player_id","rusher_player_name"}.issubset(schema_pbp.keys()):
                                selects.append(
                                    lf_pbp.select([
                                        pl.col("year").alias("season"),
                                        pl.col("rusher_player_id").cast(pl.Utf8).alias("player_id"),
                                        pl.col("rusher_player_name").alias("__pbp_name"),
                                    ])
                                )
                            if {"year","receiver_player_id","receiver_player_name"}.issubset(schema_pbp.keys()):
                                selects.append(
                                    lf_pbp.select([
                                        pl.col("year").alias("season"),
                                        pl.col("receiver_player_id").cast(pl.Utf8).alias("player_id"),
                                        pl.col("receiver_player_name").alias("__pbp_name"),
                                    ])
                                )
                            if {"year","passer_player_id","passer_player_name"}.issubset(schema_pbp.keys()):
                                selects.append(
                                    lf_pbp.select([
                                        pl.col("year").alias("season"),
                                        pl.col("passer_player_id").cast(pl.Utf8).alias("player_id"),
                                        pl.col("passer_player_name").alias("__pbp_name"),
                                    ])
                                )
                            if selects:
                                lf_pairs = pl.concat(selects)
                                lf_pairs = lf_pairs.filter(pl.col("player_id").is_not_null() & pl.col("__pbp_name").is_not_null())
                                # count occurrences per (season, player_id, name) and take top-1 name
                                lf_counts = (
                                    lf_pairs
                                    .group_by(["season","player_id","__pbp_name"]) 
                                    .agg(pl.len().alias("__cnt"))
                                )
                                lf_ranked = lf_counts.with_columns(
                                    pl.col("__cnt").rank("dense", descending=True).over(["season","player_id"]).alias("__rnk")
                                )
                                df_mode = lf_ranked.filter(pl.col("__rnk") == 1).select(["season","player_id","__pbp_name"]).unique(subset=["season","player_id"], keep="first").collect()
                                join_keys = [k for k in ["season","player_id"] if k in df_silver.columns and k in df_mode.columns]
                                if join_keys and "__pbp_name" in df_mode.columns:
                                    df_silver = df_silver.join(df_mode, on=join_keys, how="left")
                                    df_silver = df_silver.with_columns(
                                        pl.coalesce([pl.col("player_name")] + ([pl.col("player_display_name")] if "player_display_name" in df_silver.columns else []) + [pl.col("__pbp_name")]).alias("player_name")
                                    )
                                    if "__pbp_name" in df_silver.columns:
                                        df_silver = df_silver.drop(["__pbp_name"])
                        except Exception:
                            pass
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

