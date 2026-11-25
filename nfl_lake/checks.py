"""The DuckDB check runner, and the checks on the clean layer and the core tables that gate a publish.

Every check is a DuckDB SQL query that returns failing rows (0 rows = pass). Hard rules (`error`) block
publishing; soft rules (`warn`) report anomalies without blocking the build. Rules and thresholds come from
`docs/DATA_VERIFICATION.md`, measured on the full 1999-2026 nflverse lake.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb

CheckSeverity = Literal["error", "warn"]

CURRENT_TEAM_CODES = (
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GB",
    "HOU",
    "IND",
    "JAX",
    "KC",
    "LA",
    "LAC",
    "LV",
    "MIA",
    "MIN",
    "NE",
    "NO",
    "NYG",
    "NYJ",
    "PHI",
    "PIT",
    "SEA",
    "SF",
    "TB",
    "TEN",
    "WAS",
)
_TEAM_SQL_LIST = ", ".join(f"'{code}'" for code in CURRENT_TEAM_CODES)
# The columns that identify a row of each core table.
CORE_TABLE_KEYS = {
    "game": ("game_id",),
    "team_game": ("game_id", "team"),
    "player": ("gsis_id",),
    "player_game": ("game_id", "player_id"),
    "play": ("game_id", "play_id"),
    "player_week_status": ("season", "week", "team", "gsis_id"),
    "depth_chart_snapshot": ("dt", "team", "pos_id", "pos_rank"),
}
_HISTORICAL_PLAYLESS_GAMES = "'1999_01_BAL_STL', '2000_03_SD_KC', '2000_06_BUF_MIA'"
# The first season in which every cross-source identity holds with no exception; section 6 of
# docs/DATA_VERIFICATION.md gives the measurements.
STRICT_MIN_SEASON = 2016
# Next Gen Stats counts the Pro Bowl week, so it numbers the Super Bowl one week later than the schedule does.
NGS_WEEK_AS_SCHEDULE_WEEK = (
    "CASE WHEN season_type = 'POST' AND ((season <= 2020 AND week = 22) OR (season >= 2021 AND week = 23)) "
    "THEN week - 1 ELSE week END"
)
_SAMPLE_ROW_LIMIT = 5  # failing rows kept from each check to show what failed


@dataclass(frozen=True)
class DataCheck:
    name: str
    severity: CheckSeverity
    description: str
    query: str  # SQL query returning failing rows; 0 rows means the check passed.


@dataclass(frozen=True)
class CheckOutcome:
    name: str
    severity: CheckSeverity
    description: str
    failure_count: int
    columns: list[str]
    sample_rows: list[tuple]
    query_error: str | None = None  # why the check could not be run, e.g. a file it reads has lost a column


@dataclass(frozen=True)
class CheckReport:
    outcomes: list[CheckOutcome]

    @property
    def errors(self) -> list[CheckOutcome]:
        """Hard checks that found failing rows, and checks of either severity that could not be run."""
        return [
            outcome
            for outcome in self.outcomes
            if outcome.query_error or (outcome.severity == "error" and outcome.failure_count > 0)
        ]

    @property
    def warnings(self) -> list[CheckOutcome]:
        return [
            outcome
            for outcome in self.outcomes
            if not outcome.query_error and outcome.severity == "warn" and outcome.failure_count > 0
        ]

    @property
    def passed(self) -> bool:
        return not self.errors


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def run_checks(connection: duckdb.DuckDBPyConnection, checks: Sequence[DataCheck]) -> CheckReport:
    """Execute every check query and capture failing row counts plus a few sample rows.

    A query that cannot be run (a file it reads is unreadable or has lost a column) is recorded on its outcome and
    the remaining checks still run.
    """
    outcomes: list[CheckOutcome] = []
    for check in checks:
        try:
            relation = connection.sql(check.query)
            failing_rows = relation.fetchall()
            columns = list(relation.columns)
            query_error = None
        except duckdb.Error as error:
            failing_rows, columns, query_error = [], [], str(error).partition("\n")[0]
        outcomes.append(
            CheckOutcome(
                name=check.name,
                severity=check.severity,
                description=check.description,
                failure_count=len(failing_rows),
                columns=columns,
                sample_rows=failing_rows[:_SAMPLE_ROW_LIMIT],
                query_error=query_error,
            )
        )
    return CheckReport(outcomes=outcomes)


def format_check_outcome(outcome: CheckOutcome) -> str:
    """Format one failing or warning CheckOutcome with its sample rows."""
    if outcome.query_error:
        return f"{outcome.name} could not be run: {outcome.query_error}"
    samples = "; ".join(
        ", ".join(f"{col}={val!r}" for col, val in zip(outcome.columns, row, strict=True))
        for row in outcome.sample_rows
    )
    return f"{outcome.name} ({outcome.failure_count} failing): {outcome.description} [{samples}]"


def build_clean_health_checks(clean_dir: Path) -> list[DataCheck]:
    """Health checks across the datasets in `data/clean/`."""
    sched = sql_path(clean_dir / "schedules" / "games.parquet")
    pbp = sql_path(clean_dir / "pbp" / "*.parquet")
    player_stats = sql_path(clean_dir / "stats_player_week" / "*.parquet")
    team_stats = sql_path(clean_dir / "stats_team_week" / "*.parquet")
    snaps = sql_path(clean_dir / "snap_counts" / "*.parquet")
    participation = sql_path(clean_dir / "pbp_participation" / "*.parquet")
    ftn = sql_path(clean_dir / "ftn_charting" / "*.parquet")
    pfr_pass = sql_path(clean_dir / "pfr_week_pass" / "*.parquet")
    pfr_rush = sql_path(clean_dir / "pfr_week_rush" / "*.parquet")
    pfr_rec = sql_path(clean_dir / "pfr_week_rec" / "*.parquet")
    pfr_def = sql_path(clean_dir / "pfr_week_def" / "*.parquet")

    checks = [
        DataCheck(
            name="clean_reg_game_counts",
            severity="error",
            description="Completed seasons match expected NFL regular-season game counts (248 / 256 / 272; 271 in 2022)",
            query=f"""
                WITH season_counts AS (
                    SELECT season,
                           count(*) FILTER (WHERE game_type = 'REG') AS reg_total,
                           count(*) FILTER (WHERE game_type = 'REG' AND home_score IS NOT NULL) AS reg_played,
                           max(season) OVER () AS latest_season
                    FROM read_parquet('{sched}')
                    GROUP BY season
                )
                SELECT season, reg_played, reg_total
                FROM season_counts
                WHERE season < latest_season
                  AND (
                      reg_played != reg_total
                      OR reg_total != CASE
                          WHEN season <= 2001 THEN 248
                          WHEN season <= 2020 THEN 256
                          WHEN season = 2022 THEN 271
                          ELSE 272
                      END
                  )
            """,
        ),
        DataCheck(
            name="clean_no_orphan_game_ids",
            severity="error",
            description="Every game_id in pbp, stats_player_week, stats_team_week, snap_counts, and pfr_week_* exists in schedules",
            query=f"""
                WITH all_game_ids AS (
                    SELECT 'pbp' AS source, game_id FROM read_parquet('{pbp}')
                    UNION
                    SELECT 'stats_player_week' AS source, game_id FROM read_parquet('{player_stats}')
                    UNION
                    SELECT 'stats_team_week' AS source, game_id FROM read_parquet('{team_stats}')
                    UNION
                    SELECT 'snap_counts' AS source, game_id FROM read_parquet('{snaps}')
                    UNION
                    SELECT 'pfr_week_pass' AS source, game_id FROM read_parquet('{pfr_pass}')
                    UNION
                    SELECT 'pfr_week_rush' AS source, game_id FROM read_parquet('{pfr_rush}')
                    UNION
                    SELECT 'pfr_week_rec' AS source, game_id FROM read_parquet('{pfr_rec}')
                    UNION
                    SELECT 'pfr_week_def' AS source, game_id FROM read_parquet('{pfr_def}')
                )
                SELECT all_game_ids.source, all_game_ids.game_id
                FROM all_game_ids
                LEFT JOIN read_parquet('{sched}') g USING (game_id)
                WHERE g.game_id IS NULL
            """,
        ),
        DataCheck(
            name="clean_played_games_in_core_feeds",
            severity="error",
            description="Every played game in schedules appears in pbp, stats_player_week, stats_team_week, and snap_counts (2013+)",
            query=f"""
                WITH played AS (
                    SELECT game_id, season
                    FROM read_parquet('{sched}')
                    WHERE home_score IS NOT NULL
                ),
                pbp_games AS (SELECT DISTINCT game_id FROM read_parquet('{pbp}')),
                player_games AS (SELECT DISTINCT game_id FROM read_parquet('{player_stats}')),
                team_games AS (SELECT DISTINCT game_id FROM read_parquet('{team_stats}')),
                snap_games AS (SELECT DISTINCT game_id FROM read_parquet('{snaps}'))
                SELECT 'pbp' AS missing_from, p.game_id
                FROM played p LEFT JOIN pbp_games x USING (game_id)
                WHERE x.game_id IS NULL AND p.game_id NOT IN ({_HISTORICAL_PLAYLESS_GAMES})
                UNION ALL
                SELECT 'stats_player_week', p.game_id
                FROM played p LEFT JOIN player_games x USING (game_id)
                WHERE x.game_id IS NULL AND p.game_id NOT IN ({_HISTORICAL_PLAYLESS_GAMES})
                UNION ALL
                SELECT 'stats_team_week', p.game_id
                FROM played p LEFT JOIN team_games x USING (game_id)
                WHERE x.game_id IS NULL AND p.game_id NOT IN ({_HISTORICAL_PLAYLESS_GAMES})
                UNION ALL
                SELECT 'snap_counts', p.game_id
                FROM played p LEFT JOIN snap_games x USING (game_id)
                WHERE p.season >= 2013 AND x.game_id IS NULL
            """,
        ),
    ]

    if any((clean_dir / "pbp_participation").glob("*.parquet")):
        checks.append(
            DataCheck(
                name="clean_participation_no_orphan_game_ids",
                severity="error",
                description="Every nflverse_game_id in pbp_participation exists in schedules",
                query=f"""
                    SELECT DISTINCT src.nflverse_game_id AS game_id
                    FROM read_parquet('{participation}', union_by_name=true) src
                    LEFT JOIN read_parquet('{sched}') g ON src.nflverse_game_id = g.game_id
                    WHERE g.game_id IS NULL
                """,
            )
        )

    if any((clean_dir / "ftn_charting").glob("*.parquet")):
        checks.append(
            DataCheck(
                name="clean_ftn_no_orphan_game_ids",
                severity="error",
                description="Every nflverse_game_id in ftn_charting exists in schedules",
                query=f"""
                    SELECT DISTINCT src.nflverse_game_id AS game_id
                    FROM read_parquet('{ftn}') src
                    LEFT JOIN read_parquet('{sched}') g ON src.nflverse_game_id = g.game_id
                    WHERE g.game_id IS NULL
                """,
            )
        )
        checks.append(
            DataCheck(
                name="clean_ftn_in_season_lag",
                severity="warn",
                description="Played games since 2022 not yet charted in ftn_charting",
                query=f"""
                    SELECT g.game_id, g.season, g.week
                    FROM read_parquet('{sched}') g
                    LEFT JOIN (SELECT DISTINCT nflverse_game_id AS game_id FROM read_parquet('{ftn}')) f USING (game_id)
                    WHERE g.season >= 2022 AND g.home_score IS NOT NULL AND f.game_id IS NULL
                    ORDER BY g.season, g.week, g.game_id
                """,
            )
        )

    if any((clean_dir / "pfr_week_pass").glob("*.parquet")):
        checks.append(
            DataCheck(
                name="clean_pfr_in_season_lag",
                severity="warn",
                description="Played games since 2018 not yet published in pfr_week_pass",
                query=f"""
                    SELECT g.game_id, g.season, g.week
                    FROM read_parquet('{sched}') g
                    LEFT JOIN (SELECT DISTINCT game_id FROM read_parquet('{pfr_pass}')) p USING (game_id)
                    WHERE g.season >= 2018 AND g.home_score IS NOT NULL AND p.game_id IS NULL
                    ORDER BY g.season, g.week, g.game_id
                """,
            )
        )

    return checks


def build_core_checks(core_dir: Path, clean_dir: Path) -> list[DataCheck]:
    """The checks a core build must pass, run against staged or published `core/` tables."""
    game = sql_path(core_dir / "game.parquet")
    team_game = sql_path(core_dir / "team_game.parquet")
    player = sql_path(core_dir / "player.parquet")
    player_game = sql_path(core_dir / "player_game.parquet")
    play = sql_path(core_dir / "play" / "*.parquet")
    status = sql_path(core_dir / "player_week_status.parquet")
    depth = sql_path(core_dir / "depth_chart_snapshot.parquet")
    table_paths = {
        "game": game,
        "team_game": team_game,
        "player": player,
        "player_game": player_game,
        "play": play,
        "player_week_status": status,
        "depth_chart_snapshot": depth,
    }

    clean_schedules = sql_path(clean_dir / "schedules" / "games.parquet")
    clean_players = sql_path(clean_dir / "players" / "players.parquet")
    clean_depth = sql_path(clean_dir / "depth_charts" / "*.parquet")
    clean_pbp = sql_path(clean_dir / "pbp" / "*.parquet")
    clean_player_stats = sql_path(clean_dir / "stats_player_week" / "*.parquet")
    clean_snaps = sql_path(clean_dir / "snap_counts" / "*.parquet")
    clean_ngs_pass = sql_path(clean_dir / "ngs_passing" / "ngs_passing.parquet")
    clean_ngs_rush = sql_path(clean_dir / "ngs_rushing" / "ngs_rushing.parquet")
    clean_ngs_rec = sql_path(clean_dir / "ngs_receiving" / "ngs_receiving.parquet")

    checks = [
        # 1. Table shape & join safety ---------------------------------------------------------------
        DataCheck(
            name="core_keys_unique",
            severity="error",
            description="Primary keys are unique and non-null on all 7 core tables",
            query=" UNION ALL ".join(
                f"""
                SELECT '{table_name}' AS table_name,
                       count(*) - count(DISTINCT ({", ".join(key_columns)})) AS dups,
                       count(*) FILTER (WHERE {" OR ".join(f"{column} IS NULL" for column in key_columns)}) AS nulls
                FROM read_parquet('{table_paths[table_name]}')
                HAVING dups > 0 OR nulls > 0
                """
                for table_name, key_columns in CORE_TABLE_KEYS.items()
            ),
        ),
        DataCheck(
            name="core_zero_join_fanout",
            severity="error",
            description="Core tables hold exactly the rows of their sources (zero fan-out or accidental row drops)",
            query=f"""
                WITH row_counts AS (
                    SELECT 'game' AS table_name,
                           (SELECT count(*) FROM read_parquet('{game}')) AS core_rows,
                           (SELECT count(*) FROM read_parquet('{clean_schedules}')) AS source_rows
                    UNION ALL
                    SELECT 'player',
                           (SELECT count(*) FROM read_parquet('{player}')),
                           (SELECT count(*) FROM read_parquet('{clean_players}'))
                    UNION ALL
                    SELECT 'player_game_stat_lines',
                           (SELECT count(*) FROM read_parquet('{player_game}') WHERE has_stat_line),
                           (SELECT count(*) FROM read_parquet('{clean_player_stats}'))
                    UNION ALL
                    -- every stat line, plus every snap_counts row of a known player who has no stat line in that game
                    SELECT 'player_game',
                           (SELECT count(*) FROM read_parquet('{player_game}')),
                           (SELECT count(*) FROM (
                                SELECT game_id, player_id FROM read_parquet('{clean_player_stats}')
                                UNION
                                SELECT sn.game_id, p.gsis_id
                                FROM read_parquet('{clean_snaps}') sn
                                JOIN read_parquet('{player}') p ON NULLIF(sn.pfr_player_id, '') = NULLIF(p.pfr_id, '')
                           ))
                    UNION ALL
                    SELECT 'play',
                           (SELECT count(*) FROM read_parquet('{play}')),
                           (SELECT count(*) FROM read_parquet('{clean_pbp}'))
                    UNION ALL
                    SELECT 'depth_chart_snapshot',
                           (SELECT count(*) FROM read_parquet('{depth}')),
                           (SELECT count(*) FROM read_parquet('{clean_depth}'))
                    UNION ALL
                    SELECT 'team_game_strict_era',
                           (SELECT count(*) FROM read_parquet('{team_game}') WHERE season >= {STRICT_MIN_SEASON}),
                           2 * (SELECT count(*) FROM read_parquet('{game}') WHERE season >= {STRICT_MIN_SEASON} AND home_score IS NOT NULL)
                )
                SELECT table_name, core_rows, source_rows FROM row_counts WHERE core_rows != source_rows
            """,
        ),
        DataCheck(
            name="core_team_game_symmetry",
            severity="error",
            description=f"Every played game ({STRICT_MIN_SEASON}+) has 1 home and 1 away team_game row with matching reciprocal opponents",
            query=f"""
                WITH tg AS (SELECT game_id, team, opponent, is_home FROM read_parquet('{team_game}') WHERE season >= {STRICT_MIN_SEASON})
                SELECT game_id, count(*) AS rows_in_game,
                       count(*) FILTER (WHERE is_home) AS home_rows,
                       count(*) FILTER (WHERE NOT is_home) AS away_rows
                FROM tg
                GROUP BY game_id
                HAVING count(*) != 2 OR count(*) FILTER (WHERE is_home) != 1 OR count(*) FILTER (WHERE NOT is_home) != 1
                UNION ALL
                SELECT a.game_id, 2, 1, 1
                FROM tg a LEFT JOIN tg b ON a.game_id = b.game_id AND a.opponent = b.team AND b.opponent = a.team
                WHERE b.team IS NULL
            """,
        ),
        DataCheck(
            name="core_players_bridge_ids_unique",
            severity="error",
            description="player.pfr_id and player.espn_id are unique among non-null values (safe for 1:1 bridge joins)",
            query=f"""
                SELECT 'pfr_id' AS id_col, pfr_id AS id_val, count(*) AS cnt
                FROM read_parquet('{player}')
                WHERE pfr_id IS NOT NULL AND pfr_id != ''
                GROUP BY pfr_id HAVING count(*) > 1
                UNION ALL
                SELECT 'espn_id', espn_id, count(*)
                FROM read_parquet('{player}')
                WHERE espn_id IS NOT NULL AND espn_id != ''
                GROUP BY espn_id HAVING count(*) > 1
            """,
        ),
        DataCheck(
            name="core_foreign_keys_hold",
            severity="error",
            description=f"Every game_id in team_game/player_game/play exists in game, and every {STRICT_MIN_SEASON}+ player_game.player_id exists in player",
            query=f"""
                SELECT 'team_game.game_id' AS fk, tg.game_id AS bad_key
                FROM read_parquet('{team_game}') tg LEFT JOIN read_parquet('{game}') g USING (game_id)
                WHERE g.game_id IS NULL
                UNION ALL
                SELECT 'player_game.game_id', pg.game_id
                FROM read_parquet('{player_game}') pg LEFT JOIN read_parquet('{game}') g USING (game_id)
                WHERE g.game_id IS NULL
                UNION ALL
                SELECT 'play.game_id', pl.game_id
                FROM read_parquet('{play}') pl LEFT JOIN read_parquet('{game}') g USING (game_id)
                WHERE g.game_id IS NULL
                UNION ALL
                SELECT 'player_game.player_id', pg.player_id
                FROM read_parquet('{player_game}') pg LEFT JOIN read_parquet('{player}') p ON pg.player_id = p.gsis_id
                WHERE pg.season >= {STRICT_MIN_SEASON} AND p.gsis_id IS NULL
            """,
        ),
        DataCheck(
            name="core_snap_join_coverage",
            severity="error",
            description=f"At least 99.5% of {STRICT_MIN_SEASON}+ skill-position snap_counts rows resolve to player.gsis_id via pfr_id",
            query=f"""
                WITH stats AS (
                    SELECT count(*) AS total_rows,
                           count(*) FILTER (WHERE p.gsis_id IS NOT NULL) AS matched_rows
                    FROM read_parquet('{clean_snaps}') sn
                    LEFT JOIN read_parquet('{player}') p ON NULLIF(sn.pfr_player_id, '') = NULLIF(p.pfr_id, '')
                    WHERE sn.season >= {STRICT_MIN_SEASON} AND sn.position IN ('QB', 'RB', 'WR', 'TE')
                )
                SELECT total_rows, matched_rows, round(100.0 * matched_rows / NULLIF(total_rows, 0), 3) AS match_pct
                FROM stats
                WHERE total_rows > 0 AND 1.0 * matched_rows / total_rows < 0.995
            """,
        ),
        # 2. Standardization -------------------------------------------------------------------------
        DataCheck(
            name="core_only_32_current_teams",
            severity="error",
            description="Every non-null team column across all 7 core tables uses only the 32 current NFL team codes",
            query=f"""
                WITH team_vals AS (
                    SELECT 'game.home_team' AS col_name, home_team AS team_code FROM read_parquet('{game}')
                    UNION ALL SELECT 'game.away_team', away_team FROM read_parquet('{game}')
                    UNION ALL SELECT 'team_game.team', team FROM read_parquet('{team_game}')
                    UNION ALL SELECT 'team_game.opponent', opponent FROM read_parquet('{team_game}')
                    UNION ALL SELECT 'player.latest_team', latest_team FROM read_parquet('{player}')
                    UNION ALL SELECT 'player.draft_team', draft_team FROM read_parquet('{player}')
                    UNION ALL SELECT 'player_game.team', team FROM read_parquet('{player_game}')
                    UNION ALL SELECT 'player_game.opponent', opponent FROM read_parquet('{player_game}')
                    UNION ALL SELECT 'play.home_team', home_team FROM read_parquet('{play}')
                    UNION ALL SELECT 'play.away_team', away_team FROM read_parquet('{play}')
                    UNION ALL SELECT 'play.posteam', posteam FROM read_parquet('{play}')
                    UNION ALL SELECT 'play.defteam', defteam FROM read_parquet('{play}')
                    UNION ALL SELECT 'player_week_status.team', team FROM read_parquet('{status}')
                    UNION ALL SELECT 'depth_chart_snapshot.team', team FROM read_parquet('{depth}')
                )
                SELECT col_name, team_code, count(*) AS cnt
                FROM team_vals
                WHERE team_code IS NOT NULL AND team_code NOT IN ({_TEAM_SQL_LIST})
                GROUP BY col_name, team_code
            """,
        ),
        DataCheck(
            name="core_kickoff_utc_valid",
            severity="error",
            description="From 2000 onward, game.kickoff_utc is non-null UTC and never falls in 04:00-12:59 UTC",
            query=f"""
                SELECT game_id, season, gameday, gametime, kickoff_utc
                FROM read_parquet('{game}')
                WHERE season >= 2000
                  AND (kickoff_utc IS NULL OR extract('hour' FROM kickoff_utc AT TIME ZONE 'UTC') BETWEEN 4 AND 12)
            """,
        ),
        DataCheck(
            name="core_injury_report_times_in_the_afternoon",
            severity="error",
            description="In every season, the median injury_report_modified_utc falls between 13:00 and 18:00 US Eastern",
            query=f"""
                WITH report_time AS (
                    SELECT season, injury_report_modified_utc AT TIME ZONE 'America/New_York' AS eastern
                    FROM read_parquet('{status}')
                    WHERE injury_report_modified_utc IS NOT NULL
                )
                SELECT season, count(*) AS reports,
                       round(median(hour(eastern) + minute(eastern) / 60.0), 2) AS median_eastern_hour
                FROM report_time
                GROUP BY season
                HAVING median(hour(eastern) + minute(eastern) / 60.0) NOT BETWEEN 13 AND 18
            """,
        ),
        # 3. Totals that must agree across sources -------------------------------------------------------
        DataCheck(
            name="core_pass_td_equals_rec_td",
            severity="error",
            description="Within every (game_id, team), sum(player_game.passing_tds) equals sum(player_game.receiving_tds)",
            query=f"""
                SELECT game_id, team, sum(coalesce(passing_tds, 0)) AS pass_tds, sum(coalesce(receiving_tds, 0)) AS rec_tds
                FROM read_parquet('{player_game}')
                GROUP BY game_id, team
                HAVING sum(coalesce(passing_tds, 0)) != sum(coalesce(receiving_tds, 0))
            """,
        ),
        DataCheck(
            name="core_player_sums_equal_team",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, sum(player_game) matches team_game across all 12 core offensive stat columns",
            query=f"""
                WITH p AS (
                    SELECT game_id, team,
                           sum(coalesce(completions, 0)) AS completions,
                           sum(coalesce(attempts, 0)) AS attempts,
                           sum(coalesce(passing_yards, 0)) AS passing_yards,
                           sum(coalesce(passing_tds, 0)) AS passing_tds,
                           sum(coalesce(passing_interceptions, 0)) AS passing_interceptions,
                           sum(coalesce(carries, 0)) AS carries,
                           sum(coalesce(rushing_yards, 0)) AS rushing_yards,
                           sum(coalesce(rushing_tds, 0)) AS rushing_tds,
                           sum(coalesce(receptions, 0)) AS receptions,
                           sum(coalesce(targets, 0)) AS targets,
                           sum(coalesce(receiving_yards, 0)) AS receiving_yards,
                           sum(coalesce(receiving_tds, 0)) AS receiving_tds
                    FROM read_parquet('{player_game}')
                    WHERE season >= {STRICT_MIN_SEASON}
                    GROUP BY game_id, team
                )
                SELECT t.game_id, t.team,
                       p.passing_yards - t.passing_yards AS pass_yd_diff,
                       p.rushing_yards - t.rushing_yards AS rush_yd_diff,
                       p.receiving_yards - t.receiving_yards AS rec_yd_diff
                FROM read_parquet('{team_game}') t
                LEFT JOIN p USING (game_id, team)
                WHERE t.season >= {STRICT_MIN_SEASON}
                  AND (
                      coalesce(p.completions, 0) != coalesce(t.completions, 0)
                      OR coalesce(p.attempts, 0) != coalesce(t.attempts, 0)
                      OR coalesce(p.passing_yards, 0) != coalesce(t.passing_yards, 0)
                      OR coalesce(p.passing_tds, 0) != coalesce(t.passing_tds, 0)
                      OR coalesce(p.passing_interceptions, 0) != coalesce(t.passing_interceptions, 0)
                      OR coalesce(p.carries, 0) != coalesce(t.carries, 0)
                      OR coalesce(p.rushing_yards, 0) != coalesce(t.rushing_yards, 0)
                      OR coalesce(p.rushing_tds, 0) != coalesce(t.rushing_tds, 0)
                      OR coalesce(p.receptions, 0) != coalesce(t.receptions, 0)
                      OR coalesce(p.targets, 0) != coalesce(t.targets, 0)
                      OR coalesce(p.receiving_yards, 0) != coalesce(t.receiving_yards, 0)
                      OR coalesce(p.receiving_tds, 0) != coalesce(t.receiving_tds, 0)
                  )
            """,
        ),
        DataCheck(
            name="core_team_points_equal_schedule",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, team_game scoring plays add up to game.home_score / away_score",
            query=f"""
                WITH calc AS (
                    SELECT game_id, team, points_scored,
                           6 * (coalesce(passing_tds, 0) + coalesce(rushing_tds, 0) + coalesce(def_tds, 0)
                                + coalesce(special_teams_tds, 0) + coalesce(fumble_recovery_tds, 0))
                           + 3 * coalesce(fg_made, 0)
                           + 1 * coalesce(pat_made, 0)
                           + 2 * (coalesce(rushing_2pt_conversions, 0) + coalesce(receiving_2pt_conversions, 0)
                                  + coalesce(def_2pt_made, 0) + coalesce(def_safeties, 0)) AS derived_points
                    FROM read_parquet('{team_game}')
                    WHERE season >= {STRICT_MIN_SEASON}
                )
                SELECT game_id, team, points_scored, derived_points
                FROM calc
                WHERE points_scored IS NULL OR derived_points != points_scored
            """,
        ),
        DataCheck(
            name="core_pbp_equals_team_boxscore",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, play sums (including lateral_rushing_yards) equal team_game passing/rushing yards and TDs",
            query=f"""
                WITH p AS (
                    SELECT game_id, posteam AS team,
                           sum(coalesce(passing_yards, 0)) AS pass_yds,
                           sum(coalesce(rushing_yards, 0) + coalesce(lateral_rushing_yards, 0)) AS rush_yds,
                           sum(coalesce(pass_touchdown, 0)) AS pass_tds,
                           sum(coalesce(rush_touchdown, 0)) AS rush_tds
                    FROM read_parquet('{play}')
                    WHERE season >= {STRICT_MIN_SEASON} AND posteam IS NOT NULL
                    GROUP BY game_id, posteam
                )
                SELECT t.game_id, t.team,
                       t.passing_yards - coalesce(p.pass_yds, 0) AS pass_yd_diff,
                       t.rushing_yards - coalesce(p.rush_yds, 0) AS rush_yd_diff,
                       t.passing_tds - coalesce(p.pass_tds, 0) AS pass_td_diff,
                       t.rushing_tds - coalesce(p.rush_tds, 0) AS rush_td_diff
                FROM read_parquet('{team_game}') t
                LEFT JOIN p USING (game_id, team)
                WHERE t.season >= {STRICT_MIN_SEASON}
                  AND (
                      t.passing_yards IS DISTINCT FROM coalesce(p.pass_yds, 0)
                      OR t.rushing_yards IS DISTINCT FROM coalesce(p.rush_yds, 0)
                      OR t.passing_tds IS DISTINCT FROM coalesce(p.pass_tds, 0)
                      OR t.rushing_tds IS DISTINCT FROM coalesce(p.rush_tds, 0)
                  )
            """,
        ),
        DataCheck(
            name="core_targets_vs_attempts_gap",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, team_game.attempts - team_game.targets stays within [0, 15]",
            query=f"""
                SELECT game_id, team, attempts, targets, attempts - targets AS gap
                FROM read_parquet('{team_game}')
                WHERE season >= {STRICT_MIN_SEASON}
                  AND (attempts IS NULL OR targets IS NULL OR attempts - targets < 0 OR attempts - targets > 15)
            """,
        ),
        DataCheck(
            name="core_snap_teams_consistent_within_game",
            severity="error",
            description="Within a game, snap_counts lists every player who has a stat line for one team under one team",
            query=f"""
                SELECT s.game_id, s.team, count(DISTINCT sn.team) AS snap_count_teams
                FROM read_parquet('{clean_player_stats}') s
                JOIN read_parquet('{player}') p ON p.gsis_id = s.player_id
                JOIN read_parquet('{clean_snaps}') sn
                  ON sn.game_id = s.game_id AND NULLIF(sn.pfr_player_id, '') = NULLIF(p.pfr_id, '')
                GROUP BY s.game_id, s.team
                HAVING count(DISTINCT sn.team) > 1
            """,
        ),
        # A team's players who were on the field for every offensive snap all have that team's snap count. A player
        # put on the other team still has the other team's count.
        DataCheck(
            name="core_full_time_offense_one_snap_count",
            severity="error",
            description="Within a team-game, every player_game row with a 100% offense share has the same offense_snaps",
            query=f"""
                SELECT game_id, team, count(*) AS full_time_players,
                       min(offense_snaps) AS fewest_snaps, max(offense_snaps) AS most_snaps
                FROM read_parquet('{player_game}')
                WHERE offense_pct = 1
                GROUP BY game_id, team
                HAVING min(offense_snaps) != max(offense_snaps)
            """,
        ),
        DataCheck(
            name="core_snap_11_player_physics",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, sum(offense_snaps)/max(offense_snaps) per team-game stays within [10.0, 12.0]",
            query=f"""
                SELECT game_id, team,
                       round(sum(offense_snaps) / NULLIF(max(offense_snaps), 0), 3) AS snap_ratio
                FROM read_parquet('{clean_snaps}')
                WHERE season >= {STRICT_MIN_SEASON}
                GROUP BY game_id, team
                HAVING max(offense_snaps) > 0
                   AND (sum(offense_snaps) / max(offense_snaps) < 10.0 OR sum(offense_snaps) / max(offense_snaps) > 12.0)
            """,
        ),
        DataCheck(
            name="core_snap_pct_11_player_warn",
            severity="warn",
            description=f"For {STRICT_MIN_SEASON}+, sum(offense_pct) per team-game outside [10.5, 11.5] (e.g. PFR denominator quirks)",
            query=f"""
                SELECT game_id, team, round(sum(coalesce(offense_pct, 0)), 3) AS offense_pct_sum
                FROM read_parquet('{clean_snaps}')
                WHERE season >= {STRICT_MIN_SEASON}
                GROUP BY game_id, team
                HAVING sum(coalesce(offense_pct, 0)) < 10.5 OR sum(coalesce(offense_pct, 0)) > 11.5
            """,
        ),
        # 4. Domain sanity bounds --------------------------------------------------------------------
        DataCheck(
            name="core_stat_ranges_valid",
            severity="error",
            description=f"For {STRICT_MIN_SEASON}+, completions <= attempts, receptions <= targets, and key stats are non-null and within realistic NFL bounds",
            query=f"""
                SELECT game_id, player_id, completions, attempts, receptions, targets,
                       passing_yards, rushing_yards, receiving_yards, offense_pct
                FROM read_parquet('{player_game}')
                WHERE season >= {STRICT_MIN_SEASON}
                  AND (
                      completions IS NULL OR attempts IS NULL OR carries IS NULL OR receptions IS NULL OR targets IS NULL
                      OR passing_yards IS NULL OR rushing_yards IS NULL OR receiving_yards IS NULL
                      OR completions < 0 OR attempts < 0 OR carries < 0 OR receptions < 0 OR targets < 0
                      OR completions > attempts
                      OR receptions > targets
                      OR passing_yards < -50 OR passing_yards > 650
                      OR rushing_yards < -60 OR rushing_yards > 350
                      OR receiving_yards < -50 OR receiving_yards > 380
                      OR (offense_pct IS NOT NULL AND (offense_pct < 0 OR offense_pct > 1))
                  )
            """,
        ),
        # 5. Pinned real anchor games (enforced whenever those games exist in `core/game`) -----------
        # Every comparison uses `IS DISTINCT FROM` + `LEFT JOIN` from `game` so a dropped row or NULL join column
        # always fails instead of silently evaluating to SQL UNKNOWN.
        DataCheck(
            name="core_anchor_games_exact",
            severity="error",
            description="Pinned real NFL anchor games match measured ground truth (team codes, UTC kickoffs, trades, OT, snaps, NGS, depth charts)",
            query=f"""
                WITH g AS (SELECT * FROM read_parquet('{game}')),
                     tg AS (SELECT * FROM read_parquet('{team_game}')),
                     pg AS (SELECT * FROM read_parquet('{player_game}')),
                     dc AS (SELECT * FROM read_parquet('{depth}'))
                -- Anchor 1: 2015_01_CIN_OAK (OAK -> LV, EDT 16:25 -> 20:25Z, Derek Carr snaps)
                SELECT 'anchor_1_2015_oak_game' AS anchor, g.game_id AS key_id
                FROM g
                WHERE g.game_id = '2015_01_CIN_OAK'
                  AND (
                      g.home_team IS DISTINCT FROM 'LV'
                      OR g.away_team IS DISTINCT FROM 'CIN'
                      OR g.home_score IS DISTINCT FROM 13
                      OR g.away_score IS DISTINCT FROM 33
                      OR strftime(g.kickoff_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') IS DISTINCT FROM '2015-09-13T20:25:00Z'
                  )
                UNION ALL
                SELECT 'anchor_1_2015_oak_carr', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0031280'
                WHERE g.game_id = '2015_01_CIN_OAK'
                  AND (
                      pg.player_id IS NULL
                      OR pg.team IS DISTINCT FROM 'LV'
                      OR pg.passing_yards IS DISTINCT FROM 61
                      OR pg.offense_snaps IS DISTINCT FROM 23
                  )
                UNION ALL
                -- Anchor 2a & 2b: EDT (17:00Z) vs EST (18:00Z) for 13:00 ET kickoffs
                SELECT 'anchor_2a_edt_kickoff', g.game_id
                FROM g
                WHERE g.game_id = '2025_01_ARI_NO'
                  AND strftime(g.kickoff_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') IS DISTINCT FROM '2025-09-07T17:00:00Z'
                UNION ALL
                SELECT 'anchor_2b_est_kickoff', g.game_id
                FROM g
                WHERE g.game_id = '2025_10_BAL_MIN'
                  AND strftime(g.kickoff_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') IS DISTINCT FROM '2025-11-09T18:00:00Z'
                UNION ALL
                -- Anchor 3: Davante Adams 2024 trade (LV -> NYJ, snaps & stats exact)
                SELECT 'anchor_3_adams_trade', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0031381'
                WHERE g.game_id = '2024_03_CAR_LV'
                  AND (
                      pg.player_id IS NULL
                      OR pg.team IS DISTINCT FROM 'LV'
                      OR pg.receiving_yards IS DISTINCT FROM 40
                      OR pg.offense_snaps IS DISTINCT FROM 61
                  )
                UNION ALL
                SELECT 'anchor_3_adams_trade', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0031381'
                WHERE g.game_id = '2024_15_NYJ_JAX'
                  AND (
                      pg.player_id IS NULL
                      OR pg.team IS DISTINCT FROM 'NYJ'
                      OR pg.receiving_yards IS DISTINCT FROM 198
                      OR pg.receiving_tds IS DISTINCT FROM 2
                      OR pg.offense_snaps IS DISTINCT FROM 54
                  )
                UNION ALL
                -- Anchor 4: Super Bowl LVIII OT (2023_22_SF_KC) + NGS SB week 23->22 join
                SELECT 'anchor_4_sb58_mahomes', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0033873'
                WHERE g.game_id = '2023_22_SF_KC'
                  AND (
                      pg.player_id IS NULL
                      OR pg.passing_yards IS DISTINCT FROM 333
                      OR pg.rushing_yards IS DISTINCT FROM 66
                      OR pg.offense_snaps IS DISTINCT FROM 79
                      OR pg.ngs_pass_attempts IS DISTINCT FROM 46
                  )
                UNION ALL
                -- Anchor 5: 2025 snapshot depth chart strictly before kickoff
                SELECT 'anchor_5_depth_chart_asof', g.game_id
                FROM g
                WHERE g.game_id = '2025_10_ARI_SEA'
                  AND NOT EXISTS (
                      SELECT 1 FROM dc
                      WHERE dc.team = 'SEA' AND dc.pos_abb = 'QB' AND dc.pos_rank = 1
                        AND dc.gsis_id = '00-0034869'
                        AND dc.snapshot_utc = (
                            SELECT max(dc2.snapshot_utc) FROM dc dc2
                            WHERE dc2.team = 'SEA' AND dc2.snapshot_utc < g.kickoff_utc
                        )
                  )
                UNION ALL
                -- Anchor 6 & 8: 2024_13_CLE_DEN (Jameis Winston 497 yds + DEN DST 2 pick-sixes)
                SELECT 'anchor_6_winston_497', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0031503'
                WHERE g.game_id = '2024_13_CLE_DEN'
                  AND (
                      pg.player_id IS NULL
                      OR pg.passing_yards IS DISTINCT FROM 497
                      OR pg.passing_tds IS DISTINCT FROM 4
                      OR pg.passing_interceptions IS DISTINCT FROM 3
                      OR pg.passing_2pt_conversions IS DISTINCT FROM 1
                  )
                UNION ALL
                SELECT 'anchor_8_den_dst', g.game_id
                FROM g
                LEFT JOIN tg ON g.game_id = tg.game_id AND tg.team = 'DEN'
                WHERE g.game_id = '2024_13_CLE_DEN'
                  AND (
                      tg.team IS NULL
                      OR tg.points_scored IS DISTINCT FROM 41
                      OR tg.points_allowed IS DISTINCT FROM 32
                      OR tg.def_sacks IS DISTINCT FROM 3
                      OR tg.def_interceptions IS DISTINCT FROM 3
                      OR tg.def_tds IS DISTINCT FROM 2
                  )
                UNION ALL
                -- Anchor 7: Saquon Barkley 2024_09_JAX_PHI
                SELECT 'anchor_7_barkley_159', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0034844'
                WHERE g.game_id = '2024_09_JAX_PHI'
                  AND (
                      pg.player_id IS NULL
                      OR pg.rushing_yards IS DISTINCT FROM 159
                      OR pg.receiving_yards IS DISTINCT FROM 40
                      OR pg.rushing_fumbles_lost IS DISTINCT FROM 1
                  )
                UNION ALL
                -- Anchor 9: Jake Moody 2024_01_NYJ_SF (2 FG in every DK distance bracket + 2 PAT)
                SELECT 'anchor_9_moody_kicker', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0038562'
                WHERE g.game_id = '2024_01_NYJ_SF'
                  AND (
                      pg.player_id IS NULL
                      OR pg.fg_made IS DISTINCT FROM 6
                      OR pg.pat_made IS DISTINCT FROM 2
                      OR (coalesce(pg.fg_made_0_19, 0) + coalesce(pg.fg_made_20_29, 0) + coalesce(pg.fg_made_30_39, 0)) IS DISTINCT FROM 2
                      OR pg.fg_made_40_49 IS DISTINCT FROM 2
                      OR (coalesce(pg.fg_made_50_59, 0) + coalesce(pg.fg_made_60_, 0)) IS DISTINCT FROM 2
                  )
                UNION ALL
                -- Anchor 10: Cade Otton 2025_03_NYJ_TB (66 snaps, no target: a row that only snap_counts supplies)
                SELECT 'anchor_10_otton_snaps_without_a_stat', g.game_id
                FROM g
                LEFT JOIN pg ON g.game_id = pg.game_id AND pg.player_id = '00-0038129'
                WHERE g.game_id = '2025_03_NYJ_TB'
                  AND (
                      pg.player_id IS NULL
                      OR pg.team IS DISTINCT FROM 'TB'
                      OR pg.offense_snaps IS DISTINCT FROM 66
                      OR pg.targets IS DISTINCT FROM 0
                  )
            """,
        ),
    ]

    # NGS weekly join completeness (only when NGS files exist)
    if (
        (clean_dir / "ngs_passing" / "ngs_passing.parquet").exists()
        and (clean_dir / "ngs_rushing" / "ngs_rushing.parquet").exists()
        and (clean_dir / "ngs_receiving" / "ngs_receiving.parquet").exists()
    ):
        checks.append(
            DataCheck(
                name="core_ngs_weekly_join_complete",
                severity="error",
                description="All weekly NGS rows (after SB week 22->21 / 23->22 mapping) join into player_game with 0 misses",
                query=f"""
                    WITH ngs_all AS (
                        SELECT 'passing' AS kind, season, {NGS_WEEK_AS_SCHEDULE_WEEK} AS week,
                               player_gsis_id AS player_id
                        FROM read_parquet('{clean_ngs_pass}') WHERE week > 0
                        UNION ALL
                        SELECT 'rushing', season, {NGS_WEEK_AS_SCHEDULE_WEEK}, player_gsis_id
                        FROM read_parquet('{clean_ngs_rush}') WHERE week > 0
                        UNION ALL
                        SELECT 'receiving', season, {NGS_WEEK_AS_SCHEDULE_WEEK}, player_gsis_id
                        FROM read_parquet('{clean_ngs_rec}') WHERE week > 0
                    )
                    SELECT ngs_all.kind, ngs_all.season, ngs_all.week, ngs_all.player_id
                    FROM ngs_all
                    LEFT JOIN read_parquet('{player_game}') pg USING (season, week, player_id)
                    WHERE pg.player_id IS NULL
                """,
            )
        )

    return checks
