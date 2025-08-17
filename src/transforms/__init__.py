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
        key_cols = [c for c in ["season", "week", "player_id", "team"] if c in df.columns]
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
    else:
        return df

    # Dedupe by keys, prefer newer if ingested_at exists; otherwise stable first occurrence
    # Drop rows missing key columns
    df = df.drop_nulls(subset=key_cols)
    # Upcast any remaining Null-typed columns to Utf8 to avoid schema merge issues across files
    null_cols = [name for name, dtype in df.schema.items() if dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])
    if "ingested_at" in df.columns:
        df = df.sort("ingested_at").unique(subset=key_cols, keep="last")
    else:
        df = df.unique(subset=key_cols, keep="first")
    return df

