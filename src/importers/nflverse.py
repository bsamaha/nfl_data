# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from typing import Optional, Dict, Any, List
from functools import lru_cache
import time
import pandas as pd
import nfl_data_py as nfl
import structlog
import os
def _retry_params(options: Optional[Dict[str, Any]] = None) -> tuple[int, int]:
    opts = options or {}
    env_attempts = os.getenv("IMPORTER_RETRY_ATTEMPTS")
    env_base = os.getenv("IMPORTER_RETRY_BASE_SECONDS")
    attempts = opts.get("retry_attempts")
    base = opts.get("retry_base_seconds")
    if attempts is None and env_attempts is not None:
        try:
            attempts = int(env_attempts)
        except ValueError:
            attempts = None
    if base is None and env_base is not None:
        try:
            base = int(env_base)
        except ValueError:
            base = None
    attempts = int(attempts) if attempts is not None else 3
    base = int(base) if base is not None else 5
    return attempts, base


def _parse_years_arg(years: str) -> list[int]:
    years = years.strip()
    if "-" in years:
        start, end = years.split("-")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in years.split(",") if x.strip()]


@lru_cache(maxsize=1)
def _load_ids_lookup() -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    try:
        ids_df = nfl.import_ids()
    except Exception as exc:
        logger.warning("ids_lookup_fetch_failed", error=str(exc))
        return pd.DataFrame(columns=["gsis_id"])
    # Include Player IDs from the broader players dataset for rookies/new entries
    try:
        players_df = nfl.import_players()
        players_df = players_df[[c for c in ["gsis_id", "display_name", "esb_id"] if c in players_df.columns]]
        ids_df = ids_df.merge(players_df, how="outer", on="gsis_id")
    except Exception as exc:
        logger.warning("players_lookup_fetch_failed", error=str(exc))
    for col in ids_df.columns:
        try:
            ids_df[col] = ids_df[col].astype("string")
        except Exception:
            ids_df[col] = ids_df[col]
    if "gsis_id" in ids_df.columns:
        ids_df["gsis_id"] = ids_df["gsis_id"].str.strip()
    return ids_df


@lru_cache(maxsize=None)
def _load_schedule_lookup(year: int) -> pd.DataFrame:
    logger = structlog.get_logger(__name__)
    try:
        sched = nfl.import_schedules([year])
    except Exception as exc:
        logger.warning("schedule_lookup_fetch_failed", year=year, error=str(exc))
        return pd.DataFrame(columns=["team", "game_date", "week"])
    sched = sched.copy()
    sched["gameday"] = pd.to_datetime(sched.get("gameday"), errors="coerce")
    long_rows = []
    for _, row in sched.iterrows():
        game_date = row.get("gameday")
        week = row.get("week")
        for col in ("home_team", "away_team"):
            team = row.get(col)
            if pd.isna(team) or pd.isna(game_date):
                continue
            long_rows.append({
                "team": str(team),
                "game_date": game_date.normalize(),
                "week": week,
            })
    if not long_rows:
        return pd.DataFrame(columns=["team", "game_date", "week"])
    out = pd.DataFrame(long_rows)
    out["team"] = out["team"].astype("string")
    out["week"] = pd.to_numeric(out["week"], errors="coerce").astype("Int64")
    return out.drop_duplicates(subset=["team", "game_date"])


def _assign_weeks_from_schedule(df: pd.DataFrame, year: int) -> pd.Series:
    if df.empty or "dt" not in df.columns or "team" not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    schedule = _load_schedule_lookup(year).copy()
    if schedule.empty:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    schedule["game_datetime"] = pd.to_datetime(schedule["game_date"], errors="coerce")
    schedule = schedule.dropna(subset=["team", "game_datetime"])
    if schedule.empty:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    left = df[["team", "dt"]].copy()
    left = left.dropna(subset=["team", "dt"])
    if left.empty:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    left = left.reset_index().rename(columns={"index": "__idx"})
    left["dt_local"] = pd.to_datetime(left["dt"], errors="coerce", utc=True)
    left["dt_local"] = left["dt_local"].dt.tz_localize(None)
    left = left.dropna(subset=["dt_local"])
    if left.empty:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    left = left.sort_values(["team", "dt_local"])
    schedule = schedule.sort_values(["team", "game_datetime"])
    merged = pd.merge_asof(
        left,
        schedule,
        left_on="dt_local",
        right_on="game_datetime",
        by="team",
        direction="nearest",
        tolerance=pd.Timedelta(days=6),
    )
    week_series = pd.Series(pd.NA, index=df.index, dtype="Int64")
    valid = merged.dropna(subset=["week", "__idx"])
    if not valid.empty:
        week_values = pd.to_numeric(valid["week"], errors="coerce").astype("Int64")
        week_series.loc[valid["__idx"].astype(int)] = week_values
    return week_series


def _fill_player_id_fallbacks(df: pd.DataFrame, fallbacks: List[tuple[str, str]]) -> pd.Series:
    if "player_id" not in df.columns:
        df["player_id"] = pd.NA
    df["player_id"] = df["player_id"].astype("string")
    for col, prefix in fallbacks:
        if col not in df.columns:
            continue
        values = df[col].astype("string")
        values = values.where(values.notna())
        if values is None:
            continue
        if prefix:
            values = prefix + values
        df["player_id"] = df["player_id"].fillna(values)
    df["player_id"] = df["player_id"].astype("string")
    return df["player_id"]


def _resolve_player_ids(df: pd.DataFrame, candidates: List[tuple[str, str]]) -> pd.Series:
    ids_df = _load_ids_lookup()
    if df.empty:
        return pd.Series(dtype="string")
    result = pd.Series(pd.NA, index=df.index, dtype="string")
    for src_col, lookup_col in candidates:
        if src_col not in df.columns:
            continue
        src_values = df[src_col].astype("string", copy=False)
        src_values = src_values.where(src_values.notna())
        if lookup_col == "gsis_id":
            resolved = src_values
        else:
            if lookup_col not in ids_df.columns:
                continue
            mapping_df = ids_df[[lookup_col, "gsis_id"]].dropna()
            if mapping_df.empty:
                continue
            mapping_df = mapping_df.astype("string")
            mapping_df = mapping_df[mapping_df[lookup_col].str.len() > 0]
            mapping_df = mapping_df[mapping_df["gsis_id"].str.len() > 0]
            if mapping_df.empty:
                continue
            mapping = mapping_df.set_index(lookup_col)["gsis_id"].to_dict()
            resolved = src_values.map(mapping)
        result = result.fillna(resolved.astype("string"))
    return result.astype("string")


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
                except Exception:
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
                except Exception:
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
        df_y["season"] = pd.to_numeric(df_y["season"], errors="coerce").astype("Int64")
        if "dt" in df_y.columns:
            df_y["dt"] = pd.to_datetime(df_y["dt"], errors="coerce", utc=True)
        if "pos_slot" in df_y.columns:
            df_y["pos_slot"] = pd.to_numeric(df_y["pos_slot"], errors="coerce").astype("Int64")
        if "pos_abb" in df_y.columns:
            df_y["position"] = df_y["pos_abb"].astype("string")
        elif "pos_name" in df_y.columns:
            df_y["position"] = df_y["pos_name"].astype("string")
        else:
            df_y["position"] = pd.NA
        df_y["player_id"] = _resolve_player_ids(
            df_y,
            [
                ("gsis_id", "gsis_id"),
                ("espn_id", "espn_id"),
                ("pfr_player_id", "pfr_id"),
            ],
        )
        if "dt" in df_y.columns:
            schedule_lookup = _load_schedule_lookup(yr).copy()
            if not schedule_lookup.empty:
                df_y["team"] = df_y.get("team", pd.Series(dtype="string")).astype("string")
                try:
                    dt_naive = df_y["dt"].dt.tz_convert(None)
                except TypeError:
                    dt_naive = df_y["dt"].dt.tz_localize(None)
                df_y["dt_date"] = dt_naive.dt.normalize()
                schedule_lookup["game_date_normalized"] = pd.to_datetime(schedule_lookup["game_date"], errors="coerce").dt.normalize()
                merged = df_y.merge(
                    schedule_lookup,
                    how="left",
                    left_on=["team", "dt_date"],
                    right_on=["team", "game_date_normalized"],
                )
                df_y["week"] = pd.to_numeric(merged["week"], errors="coerce").astype("Int64")
                df_y.drop(columns=["dt_date"], inplace=True)
            else:
                df_y["week"] = pd.NA
        missing = int(df_y["player_id"].isna().sum())
        if missing:
            logger.warning(
                "depth_charts_player_id_missing",
                year=yr,
                rows_missing=missing,
            )
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
        attempts, base_sleep = _retry_params(options)
        for attempt in range(1, attempts + 1):
            try:
                df_y = nfl.import_weekly_data([yr])
                break
            except Exception as exc:
                msg = str(exc)
                if "404" in msg or "Not Found" in msg:
                    # On 404s, immediately try the nflverse release parquet for weekly stats
                    logger.warning("weekly_fetch_retry", year=yr, attempt=attempt, error=msg)
                    try:
                        import duckdb
                        url = f"https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{yr}.parquet"
                        df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url}')").to_df()
                        break
                    except Exception as exc_fallback:
                        logger.warning("weekly_release_fallback_failed", year=yr, attempt=attempt, error=str(exc_fallback))
                        if attempt < attempts:
                            time.sleep(base_sleep * attempt)
                        continue
                # For non-404 errors, attempt the nflverse release once as a fallback too
                try:
                    import duckdb
                    url = f"https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{yr}.parquet"
                    df_y = duckdb.sql(f"SELECT * FROM read_parquet('{url}')").to_df()
                    break
                except Exception as exc_fallback:
                    logger.error("weekly_release_fallback_failed", year=yr, error=str(exc_fallback))
                    df_y = None
                    break
        if df_y is None:
            # Fallback: derive minimal weekly from PBP + rosters for the season
            try:
                # Pull PBP using our resilient fetch
                pbp_df = fetch_pbp(str(yr), options={"downcast": True, "cache": False})
                pbp_df = pbp_df[pbp_df["year"] == yr]
                # Receiving targets and yards per player-week-team
                # Only keep pass plays with a receiver
                m = (pbp_df.get("pass", 0) == 1) & (pbp_df["receiver_player_id"].notna())
                rec = pbp_df.loc[m, [
                    "year", "week", "season_type", "posteam", "receiver_player_id",
                    "receiving_yards", "air_yards", "yards_gained"
                ]].copy()
                rec["targets"] = 1
                # receiving_yards may be null in some rows; fall back to yards_gained on passes
                rec["receiving_yards"] = rec["receiving_yards"].fillna(rec["yards_gained"]).fillna(0)
                rec["air_yards"] = rec["air_yards"].fillna(0)
                grp = rec.groupby(["year","week","season_type","posteam","receiver_player_id"], as_index=False).agg({
                    "targets":"sum",
                    "receiving_yards":"sum",
                    "air_yards":"sum"
                })
                grp.rename(columns={
                    "year":"season",
                    "posteam":"team",
                    "receiver_player_id":"player_id",
                    "air_yards":"receiving_air_yards"
                }, inplace=True)
                # Names/positions from rosters
                try:
                    rost = fetch_rosters(str(yr))
                    name_cols = [c for c in ["player_name","football_name","first_name","last_name"] if c in rost.columns]
                    if "first_name" in rost.columns and "last_name" in rost.columns:
                        rost["__name_fnln"] = rost["first_name"].astype(str) + " " + rost["last_name"].astype(str)
                        name_cols.append("__name_fnln")
                    if name_cols:
                        # prefer player_name, then football_name, then first+last
                        for col in ["player_name","football_name","__name_fnln"]:
                            if col in rost.columns:
                                rost["__joined_name"] = rost[col]
                                break
                    else:
                        rost["__joined_name"] = None
                    keep = [c for c in ["season","week","team","player_id","__joined_name","position"] if c in rost.columns]
                    rost_small = rost[keep].drop_duplicates()
                    df_y = grp.merge(rost_small, on=[c for c in ["season","week","team","player_id"] if c in grp.columns and c in rost_small.columns], how="left")
                    if "__joined_name" in df_y.columns:
                        df_y.rename(columns={"__joined_name":"player_name"}, inplace=True)
                except Exception as exc_ro:
                    logger.warning("weekly_fallback_rosters_join_failed", year=yr, error=str(exc_ro))
                    df_y = grp
                # Ensure required columns
                df_y["season"] = yr
                # Market-share-style columns left NaN for now
            except Exception as exc_fb:
                logger.error("weekly_fallback_failed", year=yr, error=str(exc_fb))
                continue
        df_y = _with_const_col(df_y, "season", yr)
        if "player" in df_y.columns and "player_name" not in df_y.columns:
            df_y = df_y.rename(columns={"player": "player_name"})
        df_y["player_id"] = _resolve_player_ids(
            df_y,
            [
                ("gsis_id", "gsis_id"),
                ("pfr_player_id", "pfr_id"),
                ("espn_id", "espn_id"),
            ],
        )
        missing = int(df_y["player_id"].isna().sum())
        if missing:
            logger.warning(
                "snap_counts_player_id_missing",
                year=yr,
                rows_missing=missing,
            )
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
        attempts, base_sleep = _retry_params(options)
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
        df_y["season"] = pd.to_numeric(df_y["season"], errors="coerce").astype("Int64")
        if "dt" in df_y.columns:
            df_y["dt"] = pd.to_datetime(df_y["dt"], errors="coerce", utc=True)
        if "pos_slot" in df_y.columns:
            df_y["pos_slot"] = pd.to_numeric(df_y["pos_slot"], errors="coerce").astype("Int64")
        if "pos_abb" in df_y.columns:
            df_y["position"] = df_y["pos_abb"].astype("string")
        elif "pos_name" in df_y.columns:
            df_y["position"] = df_y["pos_name"].astype("string")
        else:
            df_y["position"] = pd.NA
        df_y["player_id"] = _resolve_player_ids(
            df_y,
            [
                ("gsis_id", "gsis_id"),
                ("espn_id", "espn_id"),
                ("pfr_player_id", "pfr_id"),
            ],
        )
        if "dt" in df_y.columns:
            schedule_lookup = _load_schedule_lookup(yr)
            if not schedule_lookup.empty:
                df_y["team"] = df_y.get("team", pd.Series(dtype="string")).astype("string")
                dt_naive = df_y["dt"].dt.tz_localize(None)
                df_y["dt_date"] = dt_naive.dt.normalize()
                merged = df_y.merge(
                    schedule_lookup,
                    how="left",
                    left_on=["team", "dt_date"],
                    right_on=["team", "game_date"],
                )
                df_y["week"] = pd.to_numeric(merged["week"], errors="coerce").astype("Int64")
                df_y.drop(columns=["dt_date"], inplace=True)
            else:
                df_y["week"] = pd.NA
        else:
            df_y["week"] = pd.NA
        missing = int(df_y["player_id"].isna().sum())
        if missing:
            logger.warning(
                "depth_charts_player_id_missing",
                year=yr,
                rows_missing=missing,
            )
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
        if "player" in df_y.columns and "player_name" not in df_y.columns:
            df_y = df_y.rename(columns={"player": "player_name"})
        df_y["player_id"] = _resolve_player_ids(
            df_y,
            [
                ("gsis_id", "gsis_id"),
                ("pfr_player_id", "pfr_id"),
                ("espn_id", "espn_id"),
            ],
        )
        missing = int(df_y["player_id"].isna().sum())
        if missing:
            logger.warning(
                "snap_counts_player_id_missing",
                year=yr,
                rows_missing=missing,
            )
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