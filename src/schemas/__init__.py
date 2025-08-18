# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

import polars as pl
import pandera.pandas as pa


PBP_SCHEMA_BRONZE = pa.DataFrameSchema(
    {
        "game_id": pa.Column(str, nullable=True),
        "play_id": pa.Column("Int64", nullable=True),
        "year": pa.Column("Int64", nullable=True),
    },
    coerce=True,
)

PBP_SCHEMA_SILVER = pa.DataFrameSchema(
    {
        "game_id": pa.Column(str, nullable=False),
        "play_id": pa.Column("Int64", pa.Check.ge(1), nullable=False),
        "year": pa.Column("Int64", nullable=False),
    },
    coerce=True,
)

SCHEDULES_SCHEMA_BRONZE = pa.DataFrameSchema(
    {
        "game_id": pa.Column(str, nullable=True),
        # season may be missing in older schedules; allow missing in bronze
        # and add it from partition later if needed
    },
    coerce=True,
)

SCHEDULES_SCHEMA_SILVER = pa.DataFrameSchema(
    {
        "game_id": pa.Column(str, nullable=False),
        # season remains optional in silver for legacy backfills; we partition on season
    },
    coerce=True,
)

WEEKLY_SCHEMA_BRONZE = pa.DataFrameSchema(
    {
        "season": pa.Column("Int64", nullable=True),
        "week": pa.Column("Int64", nullable=True),
        "player_id": pa.Column(str, nullable=True),
        # team not guaranteed in bronze; may be `recent_team`. Silver enforces `team`.
    },
    coerce=True,
)

WEEKLY_SCHEMA_SILVER = pa.DataFrameSchema(
    {
        "season": pa.Column("Int64", nullable=False),
        "week": pa.Column("Int64", nullable=False),
        "player_id": pa.Column(str, nullable=False),
        "team": pa.Column(str, nullable=False),
    },
    coerce=True,
)


def validate_bronze(dataset: str, df: pl.DataFrame) -> None:
    if dataset == "pbp":
        PBP_SCHEMA_BRONZE.validate(df.to_pandas(), lazy=True)
    elif dataset == "schedules":
        SCHEDULES_SCHEMA_BRONZE.validate(df.to_pandas(), lazy=True)
    elif dataset == "weekly":
        WEEKLY_SCHEMA_BRONZE.validate(df.to_pandas(), lazy=True)
    elif dataset == "rosters":
        # minimal: season/week/player_id/team optional in bronze
        pass
    elif dataset == "injuries":
        pass
    elif dataset == "depth_charts":
        pass
    elif dataset == "snap_counts":
        pass
    elif dataset == "dk_bestball":
        # Static config; minimal checks
        pass
    elif dataset == "ngs_weekly":
        pass
    elif dataset == "pfr_weekly":
        pass
    elif dataset == "pfr_seasonal":
        pass
    elif dataset == "ids":
        pass
    elif dataset == "seasonal_rosters":
        pass


def validate_silver(dataset: str, df: pl.DataFrame) -> None:
    if dataset == "pbp":
        PBP_SCHEMA_SILVER.validate(df.to_pandas(), lazy=True)
    elif dataset == "schedules":
        SCHEDULES_SCHEMA_SILVER.validate(df.to_pandas(), lazy=True)
    elif dataset == "weekly":
        WEEKLY_SCHEMA_SILVER.validate(df.to_pandas(), lazy=True)
    elif dataset == "rosters":
        # Expect core keys present
        required = ["season", "week", "player_id", "team"]
        pdf = df.select([c for c in required if c in df.columns]).to_pandas()
        assert all(c in df.columns for c in required), "rosters silver missing required key columns"
    elif dataset == "injuries":
        required = ["season", "week", "team", "player_id"]
        # Allow missing week for non-regular updates; cast if present
        assert all(c in df.columns for c in required), "injuries silver missing required key columns"
    elif dataset == "depth_charts":
        required = ["season", "week", "team", "position", "player_id"]
        assert all(c in df.columns for c in required), "depth_charts silver missing required key columns"
    elif dataset == "snap_counts":
        required = ["season", "week", "team", "player_id"]
        assert all(c in df.columns for c in required), "snap_counts silver missing required key columns"
    elif dataset == "dk_bestball":
        # Ensure partition and key columns exist
        assert all(c in df.columns for c in ["section", "id"]), "dk_bestball silver missing key columns"
    elif dataset == "ngs_weekly":
        required = ["season", "week", "stat_type"]
        assert all(c in df.columns for c in required), "ngs_weekly silver missing required columns"
    elif dataset == "pfr_weekly":
        required = ["season", "week", "stat_type"]
        assert all(c in df.columns for c in required), "pfr_weekly silver missing required columns"
    elif dataset == "pfr_seasonal":
        required = ["season", "stat_type"]
        assert all(c in df.columns for c in required), "pfr_seasonal silver missing required columns"
    elif dataset == "ids":
        required_any = ["gsis_id", "pfr_id"]
        assert any(c in df.columns for c in required_any), "ids silver missing gsis_id/pfr_id columns"
    elif dataset == "seasonal_rosters":
        required = ["season","player_id"]
        assert all(c in df.columns for c in required), "seasonal_rosters silver missing required key columns"

