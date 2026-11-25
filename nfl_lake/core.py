"""Stage 4: build the 7 normalized core tables into `data/core/` behind Strategy 1 publish-time checks.

Tables built from `data/clean/`:
- `game.parquet`                  key: game_id
- `team_game.parquet`             key: game_id, team
- `player.parquet`                key: gsis_id
- `player_game.parquet`           key: game_id, player_id (every player with a stat line or a snap in the game)
- `play/play_<season>.parquet`    key: game_id, play_id (partitioned by season for low RAM and fast reads)
- `player_week_status.parquet`    key: season, week, team, gsis_id (2016+)
- `depth_chart_snapshot.parquet`  key: dt, team, pos_id, pos_rank (2025+)

Every table is built into `_staging/<run_id>/core/` first. Only if every Strategy 1 `error` check passes is that
folder swapped in for `core/` as a whole and recorded in `manifest/core_state.json`, together with the checksum of
every clean file the build read.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .changes import TableChange, compare_table
from .checks import (
    CORE_TABLE_KEYS,
    NGS_WEEK_AS_SCHEDULE_WEEK,
    CheckReport,
    build_clean_health_checks,
    build_core_checks,
    format_check_outcome,
    run_checks,
    sql_path,
)
from .source import compute_file_sha256
from .store import CoreState, CoreTableRecord, LakeStore, format_utc_iso, utc_now

logger = logging.getLogger("nfl_lake")

# Joined in where a season has a file; a season without one gets empty columns.
OPTIONAL_CLEAN_DATASETS = ("pbp_participation", "ftn_charting")
REQUIRED_CLEAN_DATASETS = (
    "schedules",
    "pbp",
    "stats_player_week",
    "stats_team_week",
    "players",
    "weekly_rosters",
    "snap_counts",
    "injuries",
    "depth_charts",
    "ngs_passing",
    "ngs_rushing",
    "ngs_receiving",
    "pfr_week_pass",
    "pfr_week_rush",
    "pfr_week_rec",
    "pfr_week_def",
)

# Columns of stats_player_week that say which player and game a row is about; player_game builds its own.
_STAT_KEY_COLUMNS = frozenset({"game_id", "player_id", "season", "week", "season_type", "team", "opponent_team"})
# Columns of stats_player_week that describe the player, and where `player` holds the same fact.
_STAT_PLAYER_COLUMNS = {
    "player_name": "short_name",
    "player_display_name": "display_name",
    "position": "position",
    "position_group": "position_group",
    "headshot_url": "headshot",
}
# Numeric stat columns upstream leaves empty when a player has no attempt of that kind. It writes 0 in every other
# one, so a player who played without recording a stat gets the same: 0 in the counts and shares, empty here.
_STAT_COLUMNS_EMPTY_WITHOUT_AN_ATTEMPT = frozenset(
    {
        "passing_epa",
        "passing_cpoe",
        "pacr",
        "rushing_epa",
        "receiving_epa",
        "racr",
        "fg_long",
        "fg_pct",
        "pat_pct",
        "pt_long",
    }
)
_NUMERIC_COLUMN_TYPES = frozenset({"INTEGER", "BIGINT", "DOUBLE"})
_FLOAT_COLUMN_TYPES = frozenset({"FLOAT", "DOUBLE"})
# The last season whose injury report times upstream holds as US Pacific clock time; section 2.7 of
# docs/DATA_VERIFICATION.md gives the measurements.
_LAST_SEASON_WITH_PACIFIC_REPORT_TIMES = 2020


class CoreBuildFailed(Exception):
    def __init__(self, errors: list[str], report: CheckReport | None = None):
        super().__init__("; ".join(errors))
        self.errors = errors
        self.report = report


@dataclass(frozen=True)
class CoreBuildSummary:
    run_id: str
    row_counts_by_table: dict[str, int]
    report: CheckReport
    published: bool  # False after a dry run
    # Each table against the one published before the build, or None when there was nothing to compare with.
    changes: list[TableChange] | None


def _norm_team(column_expr: str) -> str:
    """Normalize every historical/NGS NFL team code to the 32 current codes, turning blanks into NULL."""
    return (
        f"CASE NULLIF({column_expr}, '') "
        "WHEN 'OAK' THEN 'LV' "
        "WHEN 'SD' THEN 'LAC' "
        "WHEN 'STL' THEN 'LA' "
        "WHEN 'LAR' THEN 'LA' "
        f"ELSE NULLIF({column_expr}, '') END"
    )


def _open_utc_duckdb() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    return connection


def _copy_query_to_parquet(connection: duckdb.DuckDBPyConnection, query: str, dest_path: Path) -> int:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(f"COPY ({query}) TO '{sql_path(dest_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return int(connection.sql(f"SELECT count(*) FROM read_parquet('{sql_path(dest_path)}')").fetchall()[0][0])


def _require_clean_inputs(clean_dir: Path) -> None:
    missing = [name for name in REQUIRED_CLEAN_DATASETS if not any((clean_dir / name).glob("*.parquet"))]
    if missing:
        raise CoreBuildFailed(
            [f"missing clean dataset(s) needed to build core tables: {', '.join(missing)}; run update first"]
        )


def _build_game_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    schedules = sql_path(clean_dir / "schedules" / "games.parquet")
    query = f"""
        SELECT
            game_id,
            season,
            game_type,
            week,
            gameday,
            weekday,
            gametime,
            CASE
                WHEN gametime IS NOT NULL AND gametime != ''
                THEN timezone('America/New_York', strptime(gameday || ' ' || gametime, '%Y-%m-%d %H:%M'))
                ELSE NULL
            END AS kickoff_utc,
            {_norm_team("home_team")} AS home_team,
            {_norm_team("away_team")} AS away_team,
            home_score,
            away_score,
            result,
            total,
            overtime,
            location,
            div_game,
            spread_line,
            total_line,
            home_moneyline,
            away_moneyline,
            home_spread_odds,
            away_spread_odds,
            over_odds,
            under_odds,
            home_rest,
            away_rest,
            NULLIF(roof, '') AS roof,
            NULLIF(surface, '') AS surface,
            temp,
            wind,
            home_qb_id,
            away_qb_id,
            home_qb_name,
            away_qb_name,
            home_coach,
            away_coach,
            referee,
            stadium_id,
            stadium,
            old_game_id,
            pfr,
            espn
        FROM read_parquet('{schedules}')
        ORDER BY game_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "game.parquet")


def _build_team_game_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    team_stats = sql_path(clean_dir / "stats_team_week" / "*.parquet")
    game_path = sql_path(staged_core_dir / "game.parquet")
    norm_team = _norm_team("t.team")
    norm_opp = _norm_team("t.opponent_team")
    query = f"""
        SELECT
            t.game_id,
            {norm_team} AS team,
            {norm_opp} AS opponent,
            t.season,
            t.week,
            g.game_type,
            t.season_type,
            g.kickoff_utc,
            ({norm_team} = g.home_team) AS is_home,
            CASE WHEN {norm_team} = g.home_team THEN g.home_score ELSE g.away_score END AS points_scored,
            CASE WHEN {norm_team} = g.home_team THEN g.away_score ELSE g.home_score END AS points_allowed,
            CASE WHEN {norm_team} = g.home_team THEN g.spread_line ELSE -g.spread_line END AS favored_by,
            g.total_line,
            CASE WHEN {norm_team} = g.home_team THEN g.home_moneyline ELSE g.away_moneyline END AS team_moneyline,
            CASE WHEN {norm_team} = g.home_team THEN g.home_rest ELSE g.away_rest END AS team_rest,
            CASE WHEN {norm_team} = g.home_team THEN g.away_rest ELSE g.home_rest END AS opponent_rest,
            g.div_game,
            g.roof,
            g.surface,
            g.temp,
            g.wind,
            t.* EXCLUDE (game_id, team, opponent_team, season, week, season_type)
        FROM read_parquet('{team_stats}') t
        JOIN read_parquet('{game_path}') g USING (game_id)
        ORDER BY t.game_id, team
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "team_game.parquet")


def _build_player_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    players = sql_path(clean_dir / "players" / "players.parquet")
    rosters = sql_path(clean_dir / "weekly_rosters" / "*.parquet")
    query = f"""
        WITH roster_ids AS (
            SELECT
                gsis_id,
                arg_max(NULLIF(CAST(sleeper_id AS VARCHAR), ''), CAST(season AS INTEGER) * 100 + CAST(week AS INTEGER))
                    FILTER (WHERE NULLIF(CAST(sleeper_id AS VARCHAR), '') IS NOT NULL) AS sleeper_id,
                arg_max(NULLIF(CAST(yahoo_id AS VARCHAR), ''), CAST(season AS INTEGER) * 100 + CAST(week AS INTEGER))
                    FILTER (WHERE NULLIF(CAST(yahoo_id AS VARCHAR), '') IS NOT NULL) AS yahoo_id
            FROM read_parquet('{rosters}', union_by_name=true)
            GROUP BY gsis_id
        )
        SELECT
            p.* EXCLUDE (latest_team, draft_team),
            {_norm_team("p.latest_team")} AS latest_team,
            {_norm_team("p.draft_team")} AS draft_team,
            r.sleeper_id,
            r.yahoo_id
        FROM read_parquet('{players}') p
        LEFT JOIN roster_ids r USING (gsis_id)
        ORDER BY p.gsis_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "player.parquet")


def _player_game_stat_selects(connection: duckdb.DuckDBPyConnection, player_stats: str) -> str:
    """The select list that carries every stats_player_week column into player_game.

    A player who played without recording a stat has no stat line upstream. That row is filled the way upstream
    fills a stat line with nothing in it, and takes the player's name and position from `player`.

    Upstream writes NaN or infinity in a share whose divisor is zero. Either one spoils every sum and average it
    is part of, so both become empty.
    """
    selects: list[str] = []
    for column_name, column_type, *_ in connection.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{player_stats}')"
    ).fetchall():
        if column_name in _STAT_KEY_COLUMNS:
            continue
        stat_value = f's."{column_name}"'
        if column_type in _FLOAT_COLUMN_TYPES:
            stat_value = f"CASE WHEN isfinite({stat_value}) THEN {stat_value} END"
        if column_name in _STAT_PLAYER_COLUMNS:
            expression = f"coalesce({stat_value}, p.{_STAT_PLAYER_COLUMNS[column_name]})"
        elif column_type in _NUMERIC_COLUMN_TYPES and column_name not in _STAT_COLUMNS_EMPTY_WITHOUT_AN_ATTEMPT:
            expression = f"CASE WHEN s.player_id IS NULL THEN 0 ELSE {stat_value} END"
        else:
            expression = stat_value
        selects.append(f'{expression} AS "{column_name}"')
    return ",\n            ".join(selects)


def _build_player_game_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    """One row per player per game in which the player has a stat line or played a snap.

    stats_player_week only lists players who recorded a stat, so a receiver who played 60 snaps and was never
    thrown to is missing from it. snap_counts (2013 on) has that game, and a table without it would overstate
    every per-game average built from it.

    snap_counts lists every player of a few games under the other team. The stat lines of the same game show
    which games, and those are turned back, so a player who is only in snap_counts gets the right team.
    """
    player_stats = sql_path(clean_dir / "stats_player_week" / "*.parquet")
    game_path = sql_path(staged_core_dir / "game.parquet")
    player_path = sql_path(staged_core_dir / "player.parquet")
    snaps = sql_path(clean_dir / "snap_counts" / "*.parquet")
    ngs_pass = sql_path(clean_dir / "ngs_passing" / "ngs_passing.parquet")
    ngs_rush = sql_path(clean_dir / "ngs_rushing" / "ngs_rushing.parquet")
    ngs_rec = sql_path(clean_dir / "ngs_receiving" / "ngs_receiving.parquet")
    pfr_pass = sql_path(clean_dir / "pfr_week_pass" / "*.parquet")
    pfr_rush = sql_path(clean_dir / "pfr_week_rush" / "*.parquet")
    pfr_rec = sql_path(clean_dir / "pfr_week_rec" / "*.parquet")
    pfr_def = sql_path(clean_dir / "pfr_week_def" / "*.parquet")
    game_id = "coalesce(s.game_id, snap.game_id)"
    player_id = "coalesce(s.player_id, snap.player_id)"
    season = "coalesce(s.season, g.season)"
    week = "coalesce(s.week, g.week)"
    team = f"coalesce({_norm_team('s.team')}, snap.team)"
    opponent = f"coalesce({_norm_team('s.opponent_team')}, snap.opponent)"
    query = f"""
        WITH listed_snap AS (
            SELECT sn.game_id, sp.gsis_id AS player_id,
                   {_norm_team("sn.team")} AS team,
                   {_norm_team("sn.opponent")} AS opponent,
                   sn.offense_snaps, sn.offense_pct, sn.defense_snaps, sn.defense_pct, sn.st_snaps, sn.st_pct
            FROM read_parquet('{snaps}') sn
            JOIN read_parquet('{player_path}') sp ON NULLIF(sn.pfr_player_id, '') = NULLIF(sp.pfr_id, '')
        ),
        reversed_game AS (
            SELECT game_id
            FROM listed_snap
            JOIN read_parquet('{player_stats}') s USING (game_id, player_id)
            GROUP BY game_id
            HAVING count(*) FILTER (WHERE {_norm_team("s.team")} = listed_snap.opponent)
                 > count(*) FILTER (WHERE {_norm_team("s.team")} = listed_snap.team)
        ),
        snap AS (
            SELECT listed_snap.* EXCLUDE (team, opponent),
                   CASE WHEN reversed_game.game_id IS NULL THEN team ELSE opponent END AS team,
                   CASE WHEN reversed_game.game_id IS NULL THEN opponent ELSE team END AS opponent
            FROM listed_snap
            LEFT JOIN reversed_game USING (game_id)
        ),
        ngs_p AS (
            SELECT season, {NGS_WEEK_AS_SCHEDULE_WEEK} AS week, player_gsis_id AS player_id,
                   attempts AS ngs_pass_attempts,
                   avg_time_to_throw AS ngs_pass_avg_time_to_throw,
                   avg_completed_air_yards AS ngs_pass_avg_completed_air_yards,
                   avg_intended_air_yards AS ngs_pass_avg_intended_air_yards,
                   avg_air_yards_differential AS ngs_pass_avg_air_yards_differential,
                   aggressiveness AS ngs_pass_aggressiveness,
                   max_completed_air_distance AS ngs_pass_max_completed_air_distance,
                   avg_air_yards_to_sticks AS ngs_pass_avg_air_yards_to_sticks,
                   expected_completion_percentage AS ngs_pass_expected_completion_pct,
                   completion_percentage_above_expectation AS ngs_pass_cpoe,
                   avg_air_distance AS ngs_pass_avg_air_distance,
                   max_air_distance AS ngs_pass_max_air_distance
            FROM read_parquet('{ngs_pass}') WHERE week > 0
        ),
        ngs_ru AS (
            SELECT season, {NGS_WEEK_AS_SCHEDULE_WEEK} AS week, player_gsis_id AS player_id,
                   efficiency AS ngs_rush_efficiency,
                   percent_attempts_gte_eight_defenders AS ngs_rush_pct_8_box,
                   avg_time_to_los AS ngs_rush_avg_time_to_los,
                   expected_rush_yards AS ngs_rush_expected_yards,
                   rush_yards_over_expected AS ngs_rush_ryoe,
                   rush_yards_over_expected_per_att AS ngs_rush_ryoe_per_att,
                   rush_pct_over_expected AS ngs_rush_pct_over_expected
            FROM read_parquet('{ngs_rush}') WHERE week > 0
        ),
        ngs_re AS (
            SELECT season, {NGS_WEEK_AS_SCHEDULE_WEEK} AS week, player_gsis_id AS player_id,
                   avg_cushion AS ngs_rec_avg_cushion,
                   avg_separation AS ngs_rec_avg_separation,
                   avg_intended_air_yards AS ngs_rec_avg_intended_air_yards,
                   percent_share_of_intended_air_yards AS ngs_rec_intended_air_yards_share,
                   avg_yac AS ngs_rec_avg_yac,
                   avg_expected_yac AS ngs_rec_avg_expected_yac,
                   avg_yac_above_expectation AS ngs_rec_avg_yac_above_expectation
            FROM read_parquet('{ngs_rec}') WHERE week > 0
        )
        SELECT
            {game_id} AS game_id,
            {player_id} AS player_id,
            {season} AS season,
            {week} AS week,
            g.game_type,
            coalesce(s.season_type, CASE WHEN g.game_type = 'REG' THEN 'REG' ELSE 'POST' END) AS season_type,
            g.kickoff_utc,
            {team} AS team,
            {opponent} AS opponent,
            ({team} = g.home_team) AS is_home,
            (s.player_id IS NOT NULL) AS has_stat_line,
            {_player_game_stat_selects(connection, player_stats)},
            snap.offense_snaps,
            snap.offense_pct,
            snap.defense_snaps,
            snap.defense_pct,
            snap.st_snaps,
            snap.st_pct,
            ngs_p.* EXCLUDE (season, week, player_id),
            ngs_ru.* EXCLUDE (season, week, player_id),
            ngs_re.* EXCLUDE (season, week, player_id),
            pp.passing_drops AS pfr_pass_drops,
            pp.passing_drop_pct AS pfr_pass_drop_pct,
            pp.passing_bad_throws AS pfr_pass_bad_throws,
            pp.passing_bad_throw_pct AS pfr_pass_bad_throw_pct,
            pp.times_sacked AS pfr_pass_times_sacked,
            pp.times_blitzed AS pfr_pass_times_blitzed,
            pp.times_hurried AS pfr_pass_times_hurried,
            pp.times_hit AS pfr_pass_times_hit,
            pp.times_pressured AS pfr_pass_times_pressured,
            pp.times_pressured_pct AS pfr_pass_times_pressured_pct,
            pru.rushing_yards_before_contact AS pfr_rush_yards_before_contact,
            pru.rushing_yards_before_contact_avg AS pfr_rush_yards_before_contact_avg,
            pru.rushing_yards_after_contact AS pfr_rush_yards_after_contact,
            pru.rushing_yards_after_contact_avg AS pfr_rush_yards_after_contact_avg,
            pru.rushing_broken_tackles AS pfr_rush_broken_tackles,
            pre.receiving_broken_tackles AS pfr_rec_broken_tackles,
            pre.receiving_drop AS pfr_rec_drops,
            pre.receiving_drop_pct AS pfr_rec_drop_pct,
            pre.receiving_int AS pfr_rec_int,
            pre.receiving_rat AS pfr_rec_rat,
            pd.def_ints AS pfr_def_ints,
            pd.def_targets AS pfr_def_targets,
            pd.def_completions_allowed AS pfr_def_completions_allowed,
            pd.def_completion_pct AS pfr_def_completion_pct,
            pd.def_yards_allowed AS pfr_def_yards_allowed,
            pd.def_receiving_td_allowed AS pfr_def_receiving_td_allowed,
            pd.def_passer_rating_allowed AS pfr_def_passer_rating_allowed,
            pd.def_adot AS pfr_def_adot,
            pd.def_times_blitzed AS pfr_def_times_blitzed,
            pd.def_times_hurried AS pfr_def_times_hurried,
            pd.def_times_hitqb AS pfr_def_times_hitqb,
            pd.def_sacks AS pfr_def_sacks,
            pd.def_pressures AS pfr_def_pressures,
            pd.def_tackles_combined AS pfr_def_tackles_combined,
            pd.def_missed_tackles AS pfr_def_missed_tackles,
            pd.def_missed_tackle_pct AS pfr_def_missed_tackle_pct
        FROM read_parquet('{player_stats}') s
        FULL OUTER JOIN snap ON s.game_id = snap.game_id AND s.player_id = snap.player_id
        JOIN read_parquet('{game_path}') g ON g.game_id = {game_id}
        LEFT JOIN read_parquet('{player_path}') p ON p.gsis_id = {player_id}
        LEFT JOIN ngs_p ON ngs_p.season = {season} AND ngs_p.week = {week} AND ngs_p.player_id = {player_id}
        LEFT JOIN ngs_ru ON ngs_ru.season = {season} AND ngs_ru.week = {week} AND ngs_ru.player_id = {player_id}
        LEFT JOIN ngs_re ON ngs_re.season = {season} AND ngs_re.week = {week} AND ngs_re.player_id = {player_id}
        LEFT JOIN read_parquet('{pfr_pass}') pp
            ON pp.game_id = g.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pp.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_rush}') pru
            ON pru.game_id = g.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pru.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_rec}') pre
            ON pre.game_id = g.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pre.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_def}') pd
            ON pd.game_id = g.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pd.pfr_player_id, '')
        ORDER BY game_id, player_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "player_game.parquet")


def _build_play_tables(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    """Build per-season `core/play/play_<season>.parquet` so peak RAM stays low."""
    total_rows = 0
    for pbp_path in sorted((clean_dir / "pbp").glob("*.parquet")):
        season_match = re.fullmatch(r"play_by_play_(\d{4})", pbp_path.stem)
        if season_match is None:  # the checks read every file in the folder, so a stray copy would count twice
            raise CoreBuildFailed(
                [f"{pbp_path} is not a play-by-play season file; move it out of clean/pbp/ and run build-core again"]
            )
        season = int(season_match.group(1))
        part_path = clean_dir / "pbp_participation" / f"pbp_participation_{season}.parquet"
        ftn_path = clean_dir / "ftn_charting" / f"ftn_charting_{season}.parquet"

        if part_path.exists():
            part_select = """
                part.offense_formation,
                part.offense_personnel,
                part.defenders_in_box,
                part.defense_personnel,
                part.number_of_pass_rushers,
                part.n_offense,
                part.n_defense,
                part.offense_players,
                part.defense_players,
                part.ngs_air_yards,
                part.time_to_throw,
                part.was_pressure,
                part.route,
                part.defense_man_zone_type,
                part.defense_coverage_type
            """
            part_join = (
                f"LEFT JOIN read_parquet('{sql_path(part_path)}') part "
                "ON p.game_id = part.nflverse_game_id AND p.play_id = CAST(part.play_id AS DOUBLE)"
            )
        else:
            part_select = """
                CAST(NULL AS VARCHAR) AS offense_formation,
                CAST(NULL AS VARCHAR) AS offense_personnel,
                CAST(NULL AS INTEGER) AS defenders_in_box,
                CAST(NULL AS VARCHAR) AS defense_personnel,
                CAST(NULL AS INTEGER) AS number_of_pass_rushers,
                CAST(NULL AS INTEGER) AS n_offense,
                CAST(NULL AS INTEGER) AS n_defense,
                CAST(NULL AS VARCHAR) AS offense_players,
                CAST(NULL AS VARCHAR) AS defense_players,
                CAST(NULL AS DOUBLE) AS ngs_air_yards,
                CAST(NULL AS DOUBLE) AS time_to_throw,
                CAST(NULL AS BOOLEAN) AS was_pressure,
                CAST(NULL AS VARCHAR) AS route,
                CAST(NULL AS VARCHAR) AS defense_man_zone_type,
                CAST(NULL AS VARCHAR) AS defense_coverage_type
            """
            part_join = ""

        if ftn_path.exists():
            ftn_select = """
                ftn.starting_hash AS ftn_starting_hash,
                ftn.qb_location AS ftn_qb_location,
                ftn.n_offense_backfield AS ftn_n_offense_backfield,
                ftn.n_defense_box AS ftn_n_defense_box,
                ftn.is_no_huddle AS ftn_is_no_huddle,
                ftn.is_motion AS ftn_is_motion,
                ftn.is_play_action AS ftn_is_play_action,
                ftn.is_screen_pass AS ftn_is_screen_pass,
                ftn.is_rpo AS ftn_is_rpo,
                ftn.is_trick_play AS ftn_is_trick_play,
                ftn.is_qb_out_of_pocket AS ftn_is_qb_out_of_pocket,
                ftn.is_interception_worthy AS ftn_is_interception_worthy,
                ftn.is_throw_away AS ftn_is_throw_away,
                ftn.read_thrown AS ftn_read_thrown,
                ftn.is_catchable_ball AS ftn_is_catchable_ball,
                ftn.is_contested_ball AS ftn_is_contested_ball,
                ftn.is_created_reception AS ftn_is_created_reception,
                ftn.is_drop AS ftn_is_drop,
                ftn.is_qb_sneak AS ftn_is_qb_sneak,
                ftn.n_blitzers AS ftn_n_blitzers,
                ftn.n_pass_rushers AS ftn_n_pass_rushers,
                ftn.is_qb_fault_sack AS ftn_is_qb_fault_sack
            """
            ftn_join = (
                f"LEFT JOIN read_parquet('{sql_path(ftn_path)}') ftn "
                "ON p.game_id = ftn.nflverse_game_id AND p.play_id = CAST(ftn.nflverse_play_id AS DOUBLE)"
            )
        else:
            ftn_select = """
                CAST(NULL AS VARCHAR) AS ftn_starting_hash,
                CAST(NULL AS VARCHAR) AS ftn_qb_location,
                CAST(NULL AS INTEGER) AS ftn_n_offense_backfield,
                CAST(NULL AS INTEGER) AS ftn_n_defense_box,
                CAST(NULL AS BOOLEAN) AS ftn_is_no_huddle,
                CAST(NULL AS BOOLEAN) AS ftn_is_motion,
                CAST(NULL AS BOOLEAN) AS ftn_is_play_action,
                CAST(NULL AS BOOLEAN) AS ftn_is_screen_pass,
                CAST(NULL AS BOOLEAN) AS ftn_is_rpo,
                CAST(NULL AS BOOLEAN) AS ftn_is_trick_play,
                CAST(NULL AS BOOLEAN) AS ftn_is_qb_out_of_pocket,
                CAST(NULL AS BOOLEAN) AS ftn_is_interception_worthy,
                CAST(NULL AS BOOLEAN) AS ftn_is_throw_away,
                CAST(NULL AS VARCHAR) AS ftn_read_thrown,
                CAST(NULL AS BOOLEAN) AS ftn_is_catchable_ball,
                CAST(NULL AS BOOLEAN) AS ftn_is_contested_ball,
                CAST(NULL AS BOOLEAN) AS ftn_is_created_reception,
                CAST(NULL AS BOOLEAN) AS ftn_is_drop,
                CAST(NULL AS BOOLEAN) AS ftn_is_qb_sneak,
                CAST(NULL AS INTEGER) AS ftn_n_blitzers,
                CAST(NULL AS INTEGER) AS ftn_n_pass_rushers,
                CAST(NULL AS BOOLEAN) AS ftn_is_qb_fault_sack
            """
            ftn_join = ""

        query = f"""
            SELECT
                p.* EXCLUDE (home_team, away_team, posteam, defteam),
                {_norm_team("p.home_team")} AS home_team,
                {_norm_team("p.away_team")} AS away_team,
                {_norm_team("p.posteam")} AS posteam,
                {_norm_team("p.defteam")} AS defteam,
                {part_select},
                {ftn_select}
            FROM read_parquet('{sql_path(pbp_path)}') p
            {part_join}
            {ftn_join}
            ORDER BY p.game_id, p.play_id
        """
        total_rows += _copy_query_to_parquet(connection, query, staged_core_dir / "play" / f"play_{season}.parquet")
    return total_rows


def _build_player_week_status_table(
    connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path
) -> int:
    rosters = sql_path(clean_dir / "weekly_rosters" / "*.parquet")
    injuries = sql_path(clean_dir / "injuries" / "*.parquet")
    # The only record of when an injury report was last changed. Upstream stopped publishing it with the 2025
    # season, so a lake that holds no earlier season has no such column at all. Upstream marks every value as UTC,
    # but through the 2020 season the clock time it holds is US Pacific time, seven or eight hours behind.
    injury_columns = connection.sql(f"SELECT * FROM read_parquet('{injuries}', union_by_name=true) LIMIT 0").columns
    report_modified = (
        f"""CASE WHEN CAST(season AS INTEGER) <= {_LAST_SEASON_WITH_PACIFIC_REPORT_TIMES}
                 THEN timezone('America/Los_Angeles', date_modified AT TIME ZONE 'UTC')
                 ELSE date_modified END"""
        if "date_modified" in injury_columns
        else "CAST(NULL AS TIMESTAMPTZ)"
    )
    query = f"""
        WITH wr AS (
            SELECT
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                game_type,
                {_norm_team("team")} AS team,
                gsis_id,
                position,
                depth_chart_position,
                status AS roster_status,
                status_description_abbr,
                jersey_number
            FROM read_parquet('{rosters}', union_by_name=true)
            WHERE CAST(season AS INTEGER) >= 2016
        ),
        inj AS (
            SELECT
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER) AS week,
                game_type,
                {_norm_team("team")} AS team,
                gsis_id,
                position,
                report_primary_injury,
                report_secondary_injury,
                report_status,
                practice_primary_injury,
                practice_secondary_injury,
                practice_status,
                {report_modified} AS injury_report_modified_utc
            FROM read_parquet('{injuries}', union_by_name=true)
            WHERE CAST(season AS INTEGER) >= 2016
        )
        SELECT
            coalesce(wr.season, inj.season) AS season,
            coalesce(wr.week, inj.week) AS week,
            coalesce(wr.game_type, inj.game_type) AS game_type,
            coalesce(wr.team, inj.team) AS team,
            coalesce(wr.gsis_id, inj.gsis_id) AS gsis_id,
            coalesce(wr.position, inj.position) AS position,
            wr.depth_chart_position,
            wr.roster_status,
            wr.status_description_abbr,
            wr.jersey_number,
            inj.report_primary_injury,
            inj.report_secondary_injury,
            inj.report_status,
            inj.practice_primary_injury,
            inj.practice_secondary_injury,
            inj.practice_status,
            inj.injury_report_modified_utc
        FROM wr
        FULL OUTER JOIN inj USING (season, week, team, gsis_id)
        ORDER BY season, week, team, gsis_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "player_week_status.parquet")


def _build_depth_chart_snapshot_table(
    connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path
) -> int:
    depth_charts = sql_path(clean_dir / "depth_charts" / "*.parquet")
    player_path = sql_path(staged_core_dir / "player.parquet")
    query = f"""
        SELECT
            dc.dt,
            CAST(dc.dt AS TIMESTAMPTZ) AS snapshot_utc,
            {_norm_team("dc.team")} AS team,
            coalesce(NULLIF(dc.gsis_id, ''), p.gsis_id) AS gsis_id,
            NULLIF(dc.espn_id, '') AS espn_id,
            dc.player_name,
            dc.pos_grp_id,
            dc.pos_grp,
            dc.pos_id,
            dc.pos_name,
            dc.pos_abb,
            dc.pos_slot,
            dc.pos_rank
        FROM read_parquet('{depth_charts}') dc
        LEFT JOIN read_parquet('{player_path}') p
          ON NULLIF(dc.espn_id, '') = NULLIF(p.espn_id, '')
        ORDER BY dc.dt, team, dc.pos_id, dc.pos_rank
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "depth_chart_snapshot.parquet")


def _list_table_files(core_dir: Path, table_name: str) -> list[Path]:
    if table_name == "play":
        return sorted((core_dir / "play").glob("*.parquet"))
    single_file = core_dir / f"{table_name}.parquet"
    return [single_file] if single_file.exists() else []


# In build order: a later table reads the earlier ones from staging.
_TABLE_BUILDERS = {
    "game": _build_game_table,
    "team_game": _build_team_game_table,
    "player": _build_player_table,
    "player_game": _build_player_game_table,
    "play": _build_play_tables,
    "player_week_status": _build_player_week_status_table,
    "depth_chart_snapshot": _build_depth_chart_snapshot_table,
}
CORE_TABLE_NAMES = tuple(_TABLE_BUILDERS)


def _hash_clean_inputs(lake: LakeStore) -> dict[str, str]:
    """The checksum of every clean file a core build reads, by its path under the lake folder."""
    return {
        lake.relative_posix_path(clean_path): compute_file_sha256(clean_path)
        for dataset_name in REQUIRED_CLEAN_DATASETS + OPTIONAL_CLEAN_DATASETS
        for clean_path in sorted((lake.clean_dir / dataset_name).glob("*.parquet"))
    }


def list_changed_clean_inputs(lake: LakeStore, core_state: CoreState) -> list[str]:
    """Clean files that are not the ones the published core tables were built from. Empty when core is current."""
    current_inputs = _hash_clean_inputs(lake)
    return sorted(
        clean_path
        for clean_path in current_inputs.keys() | core_state.clean_inputs.keys()
        if current_inputs.get(clean_path) != core_state.clean_inputs.get(clean_path)
    )


def _publish_staged_core(lake: LakeStore, staged_core_dir: Path, core_state: CoreState) -> None:
    """Swap the staged folder in for core/ as a whole, so that core/ never holds tables from two builds.

    Windows refuses to move a folder while a file in it is open, so a core file that is open in another program
    fails the first move, before anything has changed.
    """
    retired_core_dir = staged_core_dir.with_name("retired_core")
    try:
        if lake.core_dir.exists():
            os.replace(lake.core_dir, retired_core_dir)
        try:
            os.replace(staged_core_dir, lake.core_dir)
        except OSError:
            if retired_core_dir.exists():
                os.replace(retired_core_dir, lake.core_dir)
            raise
    except OSError as error:
        reason = error.strerror or error
        raise CoreBuildFailed(
            [f"{lake.core_dir} could not be replaced ({reason}); close any program using a core file and try again"]
        ) from error
    lake.save_core_state(core_state)


def _compare_with_published(
    connection: duckdb.DuckDBPyConnection, lake: LakeStore, staged_core_dir: Path
) -> list[TableChange] | None:
    """What publishing the staged tables would change, or None when no core tables are published.

    A table that cannot be compared says so in its own entry and never stops a build: rebuilding is the remedy
    for a published file that cannot be read.
    """
    if not lake.core_dir.exists():
        return None
    table_changes: list[TableChange] = []
    for table_name in CORE_TABLE_NAMES:
        try:
            table_change = compare_table(
                connection,
                table_name,
                CORE_TABLE_KEYS[table_name],
                _list_table_files(lake.core_dir, table_name),
                _list_table_files(staged_core_dir, table_name),
            )
        except duckdb.Error as error:
            table_change = TableChange(table_name, not_compared=str(error).partition("\n")[0])
        table_changes.append(table_change)
    return table_changes


def build_core_tables(lake: LakeStore, *, dry_run: bool = False) -> CoreBuildSummary:
    """Build all 7 core tables into staging, run Strategy 1 checks, and publish only if all errors pass.

    A dry run builds and checks the same way and stops before publishing. It holds the lock while it runs,
    because it builds in the lake's staging folder.
    """
    started_at = utc_now()
    run_id = f"core_{started_at:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}"
    staged_core_dir = lake.staging_dir / run_id / "core"

    with lake.acquire_lock():
        _require_clean_inputs(lake.clean_dir)
        shutil.rmtree(lake.staging_dir, ignore_errors=True)
        try:
            clean_inputs = _hash_clean_inputs(lake)
            connection = _open_utc_duckdb()
            try:
                clean_report = run_checks(connection, build_clean_health_checks(lake.clean_dir))
                for warning in clean_report.warnings:
                    logger.warning("%s", format_check_outcome(warning))
                if not clean_report.passed:
                    raise CoreBuildFailed([format_check_outcome(err) for err in clean_report.errors], clean_report)

                row_counts: dict[str, int] = {}
                for table_name, build_table in _TABLE_BUILDERS.items():
                    try:
                        row_counts[table_name] = build_table(connection, lake.clean_dir, staged_core_dir)
                    except duckdb.Error as error:  # a clean file that cannot be read, or one that lost a column
                        reason = str(error).partition("\n")[0]
                        raise CoreBuildFailed([f"core/{table_name} could not be built: {reason}"]) from error
                    logger.info("staged core/%s (%d rows)", table_name, row_counts[table_name])

                core_report = run_checks(connection, build_core_checks(staged_core_dir, lake.clean_dir))
                table_changes = (
                    _compare_with_published(connection, lake, staged_core_dir) if core_report.passed else None
                )
            finally:
                connection.close()

            combined_report = CheckReport(outcomes=clean_report.outcomes + core_report.outcomes)
            for warning in core_report.warnings:
                logger.warning("%s", format_check_outcome(warning))
            if not core_report.passed:
                raise CoreBuildFailed([format_check_outcome(err) for err in core_report.errors], combined_report)
            if dry_run:
                return CoreBuildSummary(run_id, row_counts, combined_report, published=False, changes=table_changes)

            core_state = CoreState(
                run_id=run_id,
                built_at=format_utc_iso(utc_now()),
                clean_inputs=clean_inputs,
                tables={
                    table_name: CoreTableRecord(
                        rows=row_counts[table_name],
                        files={
                            lake.relative_posix_path(
                                lake.core_dir / staged_path.relative_to(staged_core_dir)
                            ): compute_file_sha256(staged_path)
                            for staged_path in _list_table_files(staged_core_dir, table_name)
                        },
                    )
                    for table_name in CORE_TABLE_NAMES
                },
            )
            _publish_staged_core(lake, staged_core_dir, core_state)
            return CoreBuildSummary(run_id, row_counts, combined_report, published=True, changes=table_changes)
        finally:
            shutil.rmtree(lake.staging_dir, ignore_errors=True)


def verify_core_tables(lake: LakeStore, *, deep: bool = False) -> tuple[int, list[str]]:
    """Verify published `data/core/` tables against `core_state.json`, the clean layer and Strategy 1 checks."""
    core_state = lake.load_core_state()
    if core_state is None:
        if any(lake.core_dir.rglob("*.parquet")):
            return 0, ["core/ holds files that manifest/core_state.json does not record; run build-core"]
        return 0, []
    errors: list[str] = []
    checked_files = 0
    for table_name in CORE_TABLE_NAMES:
        table_record = core_state.tables.get(table_name)
        if table_record is None or not table_record.files:
            errors.append(f"core/{table_name}: no files recorded in manifest/core_state.json; run build-core")
            continue
        if table_name == "play" and (lake.core_dir / "play").exists():
            actual_play_files = {
                lake.relative_posix_path(path) for path in sorted((lake.core_dir / "play").glob("*.parquet"))
            }
            for extra_path in sorted(actual_play_files - set(table_record.files)):
                errors.append(f"{extra_path}: untracked core file on disk; run build-core to clean up")
        for rel_path, expected_sha256 in sorted(table_record.files.items()):
            checked_files += 1
            file_path = lake.root_dir / rel_path
            if not file_path.exists():
                errors.append(f"{rel_path}: core file is missing; run build-core to rebuild it")
            elif deep:
                try:
                    actual_sha256 = compute_file_sha256(file_path)
                except OSError as error:
                    errors.append(f"{rel_path}: core file cannot be read ({type(error).__name__}); run build-core")
                else:
                    if actual_sha256 != expected_sha256:
                        errors.append(
                            f"{rel_path}: core file no longer matches its recorded checksum; run build-core to rebuild it"
                        )
    if errors:
        return checked_files, errors

    connection = _open_utc_duckdb()
    try:
        for table_name in CORE_TABLE_NAMES:
            recorded_rows = core_state.tables[table_name].rows
            table_glob = (
                lake.core_dir / "play" / "*.parquet"
                if table_name == "play"
                else lake.core_dir / f"{table_name}.parquet"
            )
            try:
                actual_rows = int(
                    connection.sql(f"SELECT count(*) FROM read_parquet('{sql_path(table_glob)}')").fetchall()[0][0]
                )
            except duckdb.Error as error:
                errors.append(f"core/{table_name}: core parquet cannot be read ({error}); run build-core")
                continue
            if actual_rows != recorded_rows:
                errors.append(f"core/{table_name}: has {actual_rows} rows, recorded {recorded_rows}; run build-core")
        if errors:
            return checked_files, errors
        # The checks compare core with clean, so they only mean something while core is built from this clean layer.
        changed_inputs = list_changed_clean_inputs(lake, core_state)
        if changed_inputs:
            stale_core = (
                f"core was built at {core_state.built_at} from an earlier clean layer: {len(changed_inputs)} clean "
                f"file(s) have changed since, such as {changed_inputs[0]}; run build-core"
            )
            return checked_files, [stale_core]
        report = run_checks(
            connection,
            build_clean_health_checks(lake.clean_dir) + build_core_checks(lake.core_dir, lake.clean_dir),
        )
    finally:
        connection.close()
    return checked_files, [format_check_outcome(outcome) for outcome in report.errors]
