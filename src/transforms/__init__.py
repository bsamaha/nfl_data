from __future__ import annotations

import polars as pl


def _normalize_common(df: pl.DataFrame) -> pl.DataFrame:
    # Enforce snake_case is upstream default; ensure IDs are strings
    for col in df.columns:
        if col.endswith("_id") and col not in ("play_id",):
            df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
    # Coerce known integer keys with nullable ints
    if "play_id" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("play_id").is_not_null())
            .then(
                pl.col("play_id")
                .cast(pl.Float64, strict=False)
                .round(0)
                .cast(pl.Int64, strict=False)
            )
            .otherwise(None)
            .alias("play_id")
        )
    if "year" in df.columns:
        df = df.with_columns(pl.col("year").cast(pl.Int64, strict=False))
    if "season" in df.columns:
        df = df.with_columns(pl.col("season").cast(pl.Int64, strict=False))
    # Upcast columns that are entirely Null to Utf8 to stabilize schemas across seasons
    null_cols = [name for name, dtype in df.schema.items() if dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    return df


def to_silver(dataset: str, df: pl.DataFrame) -> pl.DataFrame:
    df = _normalize_common(df)
    if dataset == "pbp":
        key_cols = ["game_id", "play_id"]
    elif dataset == "schedules":
        key_cols = ["game_id"]
    elif dataset == "weekly":
        # Ensure team column present if only recent_team exists
        if "team" not in df.columns and "recent_team" in df.columns:
            df = df.rename({"recent_team": "team"})
        # Use base keys that must be present; treat team as optional (older seasons may have null team)
        base_keys = [c for c in ["season", "week", "player_id"] if c in df.columns]
        key_cols = base_keys + (["team"] if "team" in df.columns else [])
    elif dataset == "rosters":
        # weekly rosters keyed by season/week/player/team
        if "team" not in df.columns and "recent_team" in df.columns:
            df = df.rename({"recent_team": "team"})
        key_cols = [c for c in ["season", "week", "player_id", "team"] if c in df.columns]
    elif dataset == "injuries":
        if "player_id" not in df.columns and "gsis_id" in df.columns:
            df = df.rename({"gsis_id": "player_id"})
        if "week" in df.columns:
            df = df.with_columns(pl.col("week").cast(pl.Int64, strict=False))
        key_cols = [c for c in ["season", "week", "team", "player_id", "report_date"] if c in df.columns]
    elif dataset == "depth_charts":
        if "player_id" not in df.columns and "gsis_id" in df.columns:
            df = df.rename({"gsis_id": "player_id"})
        key_cols = [c for c in ["season", "week", "team", "position", "player_id"] if c in df.columns]
    elif dataset == "snap_counts":
        if "player_id" not in df.columns and "gsis_id" in df.columns:
            df = df.rename({"gsis_id": "player_id"})
        key_cols = [c for c in ["season", "week", "team", "player_id"] if c in df.columns]
    elif dataset == "dk_bestball":
        # Simple static table; enforce keys
        key_cols = [c for c in ["section", "id"] if c in df.columns]
    elif dataset == "ngs_weekly":
        # Partition by season, stat_type; ensure player_id string if present
        if "player_id" in df.columns:
            df = df.with_columns(pl.col("player_id").cast(pl.Utf8))
        key_cols = [c for c in ["season", "week", "player_id", "stat_type"] if c in df.columns]
    elif dataset == "pfr_weekly":
        if "player_id" in df.columns:
            df = df.with_columns(pl.col("player_id").cast(pl.Utf8))
        key_cols = [c for c in ["season", "week", "player_id", "stat_type"] if c in df.columns]
    elif dataset == "pfr_seasonal":
        if "player_id" in df.columns:
            df = df.with_columns(pl.col("player_id").cast(pl.Utf8))
        key_cols = [c for c in ["season", "player_id", "stat_type"] if c in df.columns]
    elif dataset == "ids":
        # Deduplicate by primary ids
        # Keep the most complete row (heuristic: prefer rows with more non-null fields)
        cols = df.columns
        df = df.with_columns([pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int8) for c in cols]).alias("__nn")])
        key_cols = [c for c in ["gsis_id", "pfr_id"] if c in df.columns]
        df = df.sort("__nn", descending=True).unique(subset=key_cols, keep="first").drop(["__nn"]) if key_cols else df
    elif dataset == "seasonal_rosters":
        # Ensure player_id and name strings
        if "player_id" in df.columns:
            df = df.with_columns(pl.col("player_id").cast(pl.Utf8))
        for c in ("full_name", "first_name", "last_name"):
            if c in df.columns:
                df = df.with_columns(pl.col(c).cast(pl.Utf8))
        key_cols = [c for c in ["season","player_id"] if c in df.columns]
    else:
        return df

    # Dedupe by keys, prefer newer if ingested_at exists; otherwise stable first occurrence
    # Drop rows missing required base keys only (do not require optional keys like team)
    required_keys = [c for c in key_cols if c in ("season", "week", "player_id")] or key_cols
    df = df.drop_nulls(subset=required_keys)
    # Upcast any remaining Null-typed columns to Utf8 to avoid schema merge issues across files
    null_cols = [name for name, dtype in df.schema.items() if dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    if "ingested_at" in df.columns:
        df = df.sort("ingested_at").unique(subset=key_cols, keep="last")
    else:
        df = df.unique(subset=key_cols, keep="first")
    return df

