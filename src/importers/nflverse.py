# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from typing import Optional, Dict, Any, List
import time
import pandas as pd
import nfl_data_py as nfl
import structlog


def _parse_years_arg(years: str) -> list[int]:
    years = years.strip()
    if "-" in years:
        start, end = years.split("-")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in years.split(",") if x.strip()]


def _with_const_col(df: pd.DataFrame, col: str, val: Any) -> pd.DataFrame:
    """Add a constant column without causing pandas frame fragmentation."""
    if col in df.columns:
        # If the column already exists, fill any missing values with the constant
        # Use a mask to avoid chained assignment warnings and preserve dtypes
        try:
            mask = df[col].isna()
        except Exception:
            # If isna() is not applicable (e.g., non-standard dtype), leave as-is
            mask = None
        if mask is not None and getattr(mask, "any", lambda: False)():
            df = df.copy()
            df.loc[mask, col] = val
        return df
    # Build a single-column frame and concat to avoid repeated insert operations
    return pd.concat([df, pd.DataFrame({col: [val] * len(df)})], axis=1)


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
        df_y = _with_const_col(df_y, "year", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No schedules data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_weekly(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        df_y = None
        attempts = int((options or {}).get("retry_attempts", 3))
        base_sleep = int((options or {}).get("retry_base_seconds", 5))
        for attempt in range(1, attempts + 1):
            try:
                df_y = nfl.import_weekly_data([yr])
                break
            except Exception as exc:
                msg = str(exc)
                if "404" in msg or "Not Found" in msg:
                    logger.warning("weekly_fetch_retry", year=yr, attempt=attempt, error=msg)
                    if attempt < attempts:
                        time.sleep(base_sleep * attempt)
                    continue
                logger.error("weekly_fetch_failed", year=yr, error=msg)
                df_y = None
                break
        if df_y is None:
            # Fallback: try direct DuckDB read from github release known path for weekly (stats_player)
            try:
                import duckdb
                # nflverse reorganized: attempt both legacy and new naming
                url_new = f"https://github.com/nflverse/nflverse-data/releases/download/weekly_stats/weekly_stats_{yr}.parquet"
                url_legacy = f"https://github.com/nflverse/nflverse-data/releases/download/weekly/player_stats_{yr}.parquet"
                try:
                    df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url_new}')").to_df()
                except Exception:
                    df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url_legacy}')").to_df()
            except Exception as exc4:
                logger.error("weekly_fetch_fallback_failed", year=yr, error=str(exc4))
                continue
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = None
        attempts = int((options or {}).get("retry_attempts", 3))
        base_sleep = int((options or {}).get("retry_base_seconds", 5))
        for attempt in range(1, attempts + 1):
            try:
                if hasattr(nfl, "import_injuries"):
                    df_y = nfl.import_injuries([yr])
                else:
                    # Legacy or alternative name not available; mark as unavailable before 2009
                    raise RuntimeError("injuries not available in this nfl_data_py version")
                break
            except Exception as exc:
                msg = str(exc)
                if "404" in msg or "Not Found" in msg:
                    logger.warning("injuries_fetch_retry", year=yr, attempt=attempt, error=msg)
                    if attempt < attempts:
                        time.sleep(base_sleep * attempt)
                    continue
                logger.error("injuries_fetch_failed", year=yr, error=msg)
                df_y = None
                break
        if df_y is None:
            continue
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No snap_counts data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_ngs_weekly(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    stat_types: List[str] = list((options or {}).get("stat_types", ["passing", "rushing", "receiving"]))
    frames: List[pd.DataFrame] = []
    for s_type in stat_types:
        for yr in year_list:
            try:
                df_y = nfl.import_ngs_data(s_type, years=[yr])
            except Exception as exc:
                logger.error("ngs_fetch_failed", stat_type=s_type, year=yr, error=str(exc))
                continue
            df_y = _with_const_col(df_y, "season", yr)
            df_y = _with_const_col(df_y, "stat_type", s_type)
            frames.append(df_y)
    if not frames:
        raise RuntimeError("No NGS data fetched for any requested year/stat_type")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_pfr_weekly(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    stat_types: List[str] = list((options or {}).get("stat_types", ["pass", "rush", "rec"]))
    frames: List[pd.DataFrame] = []
    for s_type in stat_types:
        for yr in year_list:
            try:
                df_y = nfl.import_weekly_pfr(s_type, years=[yr])
            except Exception as exc:
                logger.error("pfr_weekly_fetch_failed", stat_type=s_type, year=yr, error=str(exc))
                continue
            df_y = _with_const_col(df_y, "season", yr)
            df_y = _with_const_col(df_y, "stat_type", s_type)
            frames.append(df_y)
    if not frames:
        raise RuntimeError("No PFR weekly data fetched for any requested year/stat_type")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_pfr_seasonal(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    stat_types: List[str] = list((options or {}).get("stat_types", ["pass", "rush", "rec"]))
    frames: List[pd.DataFrame] = []
    for s_type in stat_types:
        for yr in year_list:
            try:
                df_y = nfl.import_seasonal_pfr(s_type, years=[yr])
            except Exception as exc:
                logger.error("pfr_seasonal_fetch_failed", stat_type=s_type, year=yr, error=str(exc))
                continue
            df_y = _with_const_col(df_y, "season", yr)
            df_y = _with_const_col(df_y, "stat_type", s_type)
            frames.append(df_y)
    if not frames:
        raise RuntimeError("No PFR seasonal data fetched for any requested year/stat_type")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_ids(years: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    try:
        df = nfl.import_ids()
    except Exception as exc:
        logger.error("ids_fetch_failed", error=str(exc))
        raise
    # Normalize common id columns to string for stable parquet schemas
    for c in [
        "gsis_id",
        "pfr_id",
        "pff_id",
        "espn_id",
        "sportradar_id",
        "yahoo_id",
        "rotowire_id",
    ]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df


def fetch_seasonal_rosters(years: str, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    year_list = _parse_years_arg(years)
    frames: List[pd.DataFrame] = []
    for yr in year_list:
        try:
            df_y = nfl.import_seasonal_rosters([yr])
        except Exception as exc:
            logger.error("seasonal_rosters_fetch_failed", year=yr, error=str(exc))
            continue
        df_y = _with_const_col(df_y, "season", yr)
        # Normalize id/name fields
        if "player_id" not in df_y.columns and "gsis_id" in df_y.columns:
            df_y["player_id"] = df_y["gsis_id"].astype(str)
        # Cast jersey_number and numeric-like fields to strings to avoid dtype collisions across seasons
        for c in ("full_name", "first_name", "last_name", "jersey_number"):
            if c in df_y.columns:
                df_y[c] = df_y[c].astype(str)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No seasonal_rosters data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)


def fetch_players(options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    try:
        df = nfl.import_players()
    except Exception as exc:
        logger.error("players_fetch_failed", error=str(exc))
        raise
    # Normalize key/id and name fields
    if "gsis_id" in df.columns:
        df["gsis_id"] = df["gsis_id"].astype(str)
    for c in ("display_name", "first_name", "last_name", "full_name"):
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
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
        df_y = _with_const_col(df_y, "season", yr)
        if "player_id" not in df_y.columns and "gsis_id" in df_y.columns:
            df_y["player_id"] = df_y["gsis_id"].astype(str)
        frames.append(df_y)
    if not frames:
        raise RuntimeError("No combine data fetched for any requested year")
    return pd.concat(frames, ignore_index=True, copy=False)