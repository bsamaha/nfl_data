"""Stage 4: build the 7 normalized core tables into `data/core/` behind Strategy 1 publish-time checks.

Tables built from `data/clean/`:
- `game.parquet`                  key: game_id
- `team_game.parquet`             key: game_id, team
- `player.parquet`                key: gsis_id
- `player_game.parquet`           key: game_id, player_id
- `play/play_<season>.parquet`    key: game_id, play_id (partitioned by season for low RAM and fast reads)
- `player_week_status.parquet`    key: season, week, team, gsis_id (2016+)
- `depth_chart_snapshot.parquet`  key: dt, team, pos_id, pos_rank (2025+)

Every table is built into `_staging/<run_id>/core/` first. Only if every Strategy 1 `error` check passes
are the staged tables moved into `core/` and recorded in `manifest/core_state.json`.
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .checks import CheckReport, build_clean_health_checks, build_core_checks, format_check_outcome, run_checks
from .source import compute_file_sha256
from .store import LakeStore, format_utc_iso, utc_now

logger = logging.getLogger("nfl_lake")

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

CORE_TABLE_NAMES = (
    "game",
    "team_game",
    "player",
    "player_game",
    "play",
    "player_week_status",
    "depth_chart_snapshot",
)


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


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


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
    connection.execute(f"COPY ({query}) TO '{_sql_path(dest_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return int(connection.sql(f"SELECT count(*) FROM read_parquet('{_sql_path(dest_path)}')").fetchall()[0][0])


def _require_clean_inputs(clean_dir: Path) -> None:
    missing = [name for name in REQUIRED_CLEAN_DATASETS if not any((clean_dir / name).glob("*.parquet"))]
    if missing:
        raise CoreBuildFailed(
            [f"missing clean dataset(s) needed to build core tables: {', '.join(missing)}; run update first"]
        )


def _build_game_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    schedules = _sql_path(clean_dir / "schedules" / "games.parquet")
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
    team_stats = _sql_path(clean_dir / "stats_team_week" / "*.parquet")
    game_path = _sql_path(staged_core_dir / "game.parquet")
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
    players = _sql_path(clean_dir / "players" / "players.parquet")
    rosters = _sql_path(clean_dir / "weekly_rosters" / "*.parquet")
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


def _build_player_game_table(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    player_stats = _sql_path(clean_dir / "stats_player_week" / "*.parquet")
    game_path = _sql_path(staged_core_dir / "game.parquet")
    player_path = _sql_path(staged_core_dir / "player.parquet")
    snaps = _sql_path(clean_dir / "snap_counts" / "*.parquet")
    ngs_pass = _sql_path(clean_dir / "ngs_passing" / "ngs_passing.parquet")
    ngs_rush = _sql_path(clean_dir / "ngs_rushing" / "ngs_rushing.parquet")
    ngs_rec = _sql_path(clean_dir / "ngs_receiving" / "ngs_receiving.parquet")
    pfr_pass = _sql_path(clean_dir / "pfr_week_pass" / "*.parquet")
    pfr_rush = _sql_path(clean_dir / "pfr_week_rush" / "*.parquet")
    pfr_rec = _sql_path(clean_dir / "pfr_week_rec" / "*.parquet")
    pfr_def = _sql_path(clean_dir / "pfr_week_def" / "*.parquet")
    norm_team = _norm_team("s.team")
    norm_opp = _norm_team("s.opponent_team")
    sb_week_expr = (
        "CASE WHEN season_type = 'POST' AND ((season <= 2020 AND week = 22) OR (season >= 2021 AND week = 23)) "
        "THEN week - 1 ELSE week END"
    )
    query = f"""
        WITH ngs_p AS (
            SELECT season, {sb_week_expr} AS week, player_gsis_id AS player_id,
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
            SELECT season, {sb_week_expr} AS week, player_gsis_id AS player_id,
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
            SELECT season, {sb_week_expr} AS week, player_gsis_id AS player_id,
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
            s.game_id,
            s.player_id,
            s.season,
            s.week,
            g.game_type,
            s.season_type,
            g.kickoff_utc,
            {norm_team} AS team,
            {norm_opp} AS opponent,
            ({norm_team} = g.home_team) AS is_home,
            s.* EXCLUDE (game_id, player_id, season, week, season_type, team, opponent_team),
            sn.offense_snaps,
            sn.offense_pct,
            sn.defense_snaps,
            sn.defense_pct,
            sn.st_snaps,
            sn.st_pct,
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
        JOIN read_parquet('{game_path}') g USING (game_id)
        LEFT JOIN read_parquet('{player_path}') p ON s.player_id = p.gsis_id
        LEFT JOIN read_parquet('{snaps}') sn
            ON s.game_id = sn.game_id AND NULLIF(p.pfr_id, '') = NULLIF(sn.pfr_player_id, '')
        LEFT JOIN ngs_p ON s.season = ngs_p.season AND s.week = ngs_p.week AND s.player_id = ngs_p.player_id
        LEFT JOIN ngs_ru ON s.season = ngs_ru.season AND s.week = ngs_ru.week AND s.player_id = ngs_ru.player_id
        LEFT JOIN ngs_re ON s.season = ngs_re.season AND s.week = ngs_re.week AND s.player_id = ngs_re.player_id
        LEFT JOIN read_parquet('{pfr_pass}') pp
            ON s.game_id = pp.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pp.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_rush}') pru
            ON s.game_id = pru.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pru.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_rec}') pre
            ON s.game_id = pre.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pre.pfr_player_id, '')
        LEFT JOIN read_parquet('{pfr_def}') pd
            ON s.game_id = pd.game_id AND NULLIF(p.pfr_id, '') = NULLIF(pd.pfr_player_id, '')
        ORDER BY s.game_id, s.player_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "player_game.parquet")


def _build_play_tables(connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path) -> int:
    """Build per-season `core/play/play_<season>.parquet` so peak RAM stays low."""
    total_rows = 0
    for pbp_path in sorted((clean_dir / "pbp").glob("play_by_play_*.parquet")):
        season = int(pbp_path.stem.rsplit("_", 1)[1])
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
                f"LEFT JOIN read_parquet('{_sql_path(part_path)}') part "
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
                f"LEFT JOIN read_parquet('{_sql_path(ftn_path)}') ftn "
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
            FROM read_parquet('{_sql_path(pbp_path)}') p
            {part_join}
            {ftn_join}
            ORDER BY p.game_id, p.play_id
        """
        total_rows += _copy_query_to_parquet(connection, query, staged_core_dir / "play" / f"play_{season}.parquet")
    return total_rows


def _build_player_week_status_table(
    connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path
) -> int:
    rosters = _sql_path(clean_dir / "weekly_rosters" / "*.parquet")
    injuries = _sql_path(clean_dir / "injuries" / "*.parquet")
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
                practice_status
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
            inj.practice_status
        FROM wr
        FULL OUTER JOIN inj USING (season, week, team, gsis_id)
        ORDER BY season, week, team, gsis_id
    """
    return _copy_query_to_parquet(connection, query, staged_core_dir / "player_week_status.parquet")


def _build_depth_chart_snapshot_table(
    connection: duckdb.DuckDBPyConnection, clean_dir: Path, staged_core_dir: Path
) -> int:
    depth_charts = _sql_path(clean_dir / "depth_charts" / "*.parquet")
    player_path = _sql_path(staged_core_dir / "player.parquet")
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


def _publish_staged_core(
    lake: LakeStore,
    staged_core_dir: Path,
    row_counts: dict[str, int],
    run_id: str,
) -> None:
    published_at = format_utc_iso(utc_now())
    core_state: dict[str, dict] = {}
    lake.core_dir.mkdir(parents=True, exist_ok=True)
    for table_name in CORE_TABLE_NAMES:
        staged_files = _list_table_files(staged_core_dir, table_name)
        file_hashes: dict[str, str] = {}
        if table_name == "play":
            target_play_dir = lake.core_dir / "play"
            target_play_dir.mkdir(parents=True, exist_ok=True)
            new_names = {staged_file.name for staged_file in staged_files}
            for existing_file in target_play_dir.glob("*.parquet"):
                if existing_file.name not in new_names:
                    existing_file.unlink()
            for staged_file in staged_files:
                dest_path = target_play_dir / staged_file.name
                os.replace(staged_file, dest_path)
                file_hashes[lake.relative_posix_path(dest_path)] = compute_file_sha256(dest_path)
        else:
            (staged_file,) = staged_files
            dest_path = lake.core_dir / staged_file.name
            os.replace(staged_file, dest_path)
            file_hashes[lake.relative_posix_path(dest_path)] = compute_file_sha256(dest_path)
        core_state[table_name] = {
            "rows": row_counts[table_name],
            "files": file_hashes,
            "built_at": published_at,
            "run_id": run_id,
        }
    lake.save_core_state(core_state)


def build_core_tables(lake: LakeStore, *, strict_min_season: int = 2016) -> CoreBuildSummary:
    """Build all 7 core tables into staging, run Strategy 1 checks, and publish only if all errors pass."""
    started_at = utc_now()
    run_id = f"core_{started_at:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}"
    staged_core_dir = lake.staging_dir / run_id / "core"

    with lake.acquire_lock():
        _require_clean_inputs(lake.clean_dir)
        shutil.rmtree(lake.staging_dir, ignore_errors=True)
        try:
            connection = _open_utc_duckdb()
            try:
                clean_report = run_checks(connection, build_clean_health_checks(lake.clean_dir))
                for warning in clean_report.warnings:
                    logger.warning("%s", format_check_outcome(warning))
                if not clean_report.passed:
                    raise CoreBuildFailed([format_check_outcome(err) for err in clean_report.errors], clean_report)

                row_counts = {
                    "game": _build_game_table(connection, lake.clean_dir, staged_core_dir),
                    "team_game": _build_team_game_table(connection, lake.clean_dir, staged_core_dir),
                    "player": _build_player_table(connection, lake.clean_dir, staged_core_dir),
                    "player_game": _build_player_game_table(connection, lake.clean_dir, staged_core_dir),
                    "play": _build_play_tables(connection, lake.clean_dir, staged_core_dir),
                    "player_week_status": _build_player_week_status_table(connection, lake.clean_dir, staged_core_dir),
                    "depth_chart_snapshot": _build_depth_chart_snapshot_table(
                        connection, lake.clean_dir, staged_core_dir
                    ),
                }
                for table_name, row_count in row_counts.items():
                    logger.info("staged core/%s (%d rows)", table_name, row_count)

                core_report = run_checks(
                    connection,
                    build_core_checks(staged_core_dir, lake.clean_dir, strict_min_season=strict_min_season),
                )
            finally:
                connection.close()

            combined_report = CheckReport(outcomes=clean_report.outcomes + core_report.outcomes)
            for warning in core_report.warnings:
                logger.warning("%s", format_check_outcome(warning))
            if not core_report.passed:
                raise CoreBuildFailed([format_check_outcome(err) for err in core_report.errors], combined_report)

            _publish_staged_core(lake, staged_core_dir, row_counts, run_id)
            return CoreBuildSummary(run_id=run_id, row_counts_by_table=row_counts, report=combined_report)
        finally:
            shutil.rmtree(lake.staging_dir, ignore_errors=True)


def verify_core_tables(lake: LakeStore, *, deep: bool = False, strict_min_season: int = 2016) -> tuple[int, list[str]]:
    """Verify published `data/core/` tables against `core_state.json` and Strategy 1 checks."""
    core_state = lake.load_core_state()
    if not core_state:
        return 0, []
    errors: list[str] = []
    checked_files = 0
    for table_name in CORE_TABLE_NAMES:
        entry = core_state.get(table_name)
        if entry is None:
            errors.append(f"core/{table_name}: missing from manifest/core_state.json; run build-core")
            continue
        if not entry["files"]:
            errors.append(f"core/{table_name}: no files recorded in manifest/core_state.json; run build-core")
            continue
        for rel_path, expected_sha256 in sorted(entry["files"].items()):
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
            entry = core_state[table_name]
            table_glob = (
                lake.core_dir / "play" / "*.parquet"
                if table_name == "play"
                else lake.core_dir / f"{table_name}.parquet"
            )
            try:
                actual_rows = int(
                    connection.sql(f"SELECT count(*) FROM read_parquet('{_sql_path(table_glob)}')").fetchall()[0][0]
                )
            except duckdb.Error as error:
                errors.append(f"core/{table_name}: core parquet cannot be read ({error}); run build-core")
                continue
            if actual_rows != entry["rows"]:
                errors.append(f"core/{table_name}: has {actual_rows} rows, recorded {entry['rows']}; run build-core")
        if errors:
            return checked_files, errors
        try:
            clean_report = run_checks(connection, build_clean_health_checks(lake.clean_dir))
            core_report = run_checks(
                connection,
                build_core_checks(lake.core_dir, lake.clean_dir, strict_min_season=strict_min_season),
            )
        except duckdb.Error as error:
            return checked_files, [f"core verification query failed ({error}); run build-core"]
    finally:
        connection.close()
    for err in clean_report.errors + core_report.errors:
        errors.append(format_check_outcome(err))
    return checked_files, errors
