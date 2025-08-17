# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from typing import Optional, Dict, Any, List
import pandas as pd
import nfl_data_py as nfl
import structlog


def _parse_years_arg(years: str) -> list[int]:
    years = years.strip()
    if "-" in years:
        start, end = years.split("-")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in years.split(",") if x.strip()]


def fetch_pbp(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    opts = options or {}
    downcast = bool(opts.get("downcast", True))
    default_cache = bool(opts.get("cache", False))
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        cache = default_cache
        try:
            df_y = nfl.import_pbp_data([yr], downcast=downcast, cache=cache)
        except Exception as exc:
            msg = str(exc)
            if cache and "cache file does not exist" in msg:
                try:
                    df_y = nfl.import_pbp_data([yr], downcast=downcast, cache=False)
                except Exception as exc2:
                    # Fallback to direct read from release asset via DuckDB
                    try:
                        import duckdb
                        url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{yr}.parquet"
                        df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url}')").to_df()
                    except Exception as exc3:
                        logger.error("pbp_fetch_failed", year=yr, error=str(exc3))
                        continue
            else:
                # Retry once without cache for any unexpected error
                try:
                    df_y = nfl.import_pbp_data([yr], downcast=downcast, cache=False)
                except Exception as exc2:
                    try:
                        import duckdb
                        url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{yr}.parquet"
                        df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url}')").to_df()
                    except Exception as exc3:
                        logger.error("pbp_fetch_failed", year=yr, error=str(exc3))
                        continue
        if "year" not in df_y.columns:
            df_y["year"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No PBP data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_schedules(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_schedules([yr])
        except Exception as exc:
            logger.error("schedules_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No schedules data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_weekly(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_weekly_data([yr])
        except Exception as exc:
            logger.error("weekly_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No weekly data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_rosters(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            # nfl_data_py rosters function name differs by version; try common variants
            if hasattr(nfl, "import_weekly_rosters"):
                df_y = nfl.import_weekly_rosters([yr])
            elif hasattr(nfl, "import_rosters"):
                df_y = nfl.import_rosters([yr])
            else:
                raise AttributeError("nfl_data_py missing rosters import function")
        except Exception as exc:
            logger.error("rosters_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        # Normalize key and problematic dtypes for stable parquet writing
        if "week" in df_y.columns:
            df_y["week"] = pd.to_numeric(df_y["week"], errors="coerce").astype("Int64")
        # Cast common numeric-like columns that may be mixed types
        for col in [
            "jersey_number",
            "height",
            "weight",
            "draft_year",
            "draft_round",
            "draft_number",
        ]:
            if col in df_y.columns:
                df_y[col] = pd.to_numeric(df_y[col], errors="coerce").astype("Int64")
        if "player_id" not in df_y.columns and "gsis_id" in df_y.columns:
            df_y["player_id"] = df_y["gsis_id"].astype(str)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No rosters data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_injuries(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            if hasattr(nfl, "import_injuries"):
                df_y = nfl.import_injuries([yr])
            else:
                # Legacy or alternative name not available; mark as unavailable before 2009
                raise RuntimeError("injuries not available in this nfl_data_py version")
        except Exception as exc:
            logger.error("injuries_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No injuries data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_depth_charts(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            if hasattr(nfl, "import_depth_charts"):
                df_y = nfl.import_depth_charts([yr])
            else:
                raise RuntimeError("depth_charts not available in this nfl_data_py version")
        except Exception as exc:
            logger.error("depth_charts_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No depth_charts data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_snap_counts(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            if hasattr(nfl, "import_snap_counts"):
                df_y = nfl.import_snap_counts([yr])
            else:
                raise RuntimeError("snap_counts not available in this nfl_data_py version")
        except Exception as exc:
            logger.error("snap_counts_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No snap_counts data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_officials(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_officials([yr])
        except Exception as exc:
            logger.error("officials_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No officials data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_win_totals(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_win_totals([yr])
        except Exception as exc:
            logger.error("win_totals_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No win_totals data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_scoring_lines(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_sc_lines([yr])
        except Exception as exc:
            logger.error("scoring_lines_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No scoring_lines data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_draft_picks(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_draft_picks([yr])
        except Exception as exc:
            logger.error("draft_picks_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No draft_picks data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_combine(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_combine_data([yr])
        except Exception as exc:
            logger.error("combine_fetch_failed", year=yr, error=str(exc))
            continue
        if "season" not in df_y.columns:
            df_y["season"] = yr
        if "player_id" not in df_y.columns and "gsis_id" in df_y.columns:
            df_y["player_id"] = df_y["gsis_id"].astype(str)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No combine data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)