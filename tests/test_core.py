"""Stage 4 core tables and Strategy 1 publish-time checkbook tests (no network)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from nfl_lake.checks import build_core_checks, run_checks
from nfl_lake.cli import main
from nfl_lake.core import CoreBuildFailed, build_core_tables, verify_core_tables
from nfl_lake.source import compute_file_sha256
from nfl_lake.store import LakeLocked, LakeStore


def _write_clean(lake: LakeStore, dataset_name: str, filename: str, frame: pl.DataFrame) -> Path:
    dest_path = lake.clean_file_path(dataset_name, filename)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(dest_path)
    return dest_path


def _populate_valid_clean_lake(lake: LakeStore) -> None:
    """Create a minimal, self-consistent 2025 clean lake across all 18 datasets.

    Includes:
    - Game 1 (`2025_01_KC_OAK`): EDT kickoff (`13:00` -> `17:00Z`), `OAK -> LV` normalization, lateral rushing play,
      blank `posteam` play.
    - Game 2 (`2025_22_LAR_BUF`): Super Bowl EST kickoff (`18:30` -> `23:30Z`), `LAR -> LA` normalization, NGS
      Super Bowl week `23 -> 22` mapping, and `week = 0` season-summary NGS rows that must not fan out.
    """
    _write_clean(
        lake,
        "schedules",
        "games.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK", "2025_22_LAR_BUF"],
                "season": [2025, 2025],
                "game_type": ["REG", "SB"],
                "week": [1, 22],
                "gameday": ["2025-09-07", "2026-02-08"],
                "weekday": ["Sunday", "Sunday"],
                "gametime": ["13:00", "18:30"],
                "home_team": ["OAK", "BUF"],
                "away_team": ["KC", "LAR"],
                "home_score": [14, 17],
                "away_score": [21, 10],
                "result": [-7, 7],
                "total": [35, 27],
                "overtime": [0, 0],
                "location": ["Home", "Neutral"],
                "div_game": [1, 0],
                "spread_line": [-3.5, 2.5],
                "total_line": [47.5, 49.5],
                "home_moneyline": [150, -135],
                "away_moneyline": [-180, 115],
                "home_spread_odds": [-110, -110],
                "away_spread_odds": [-110, -110],
                "over_odds": [-110, -110],
                "under_odds": [-110, -110],
                "home_rest": [7, 14],
                "away_rest": [7, 14],
                "roof": ["dome", "outdoors"],
                "surface": ["", "grass"],
                "temp": [72, 65],
                "wind": [0, 6],
                "home_qb_id": ["00-LVQB", "00-BUFQB"],
                "away_qb_id": ["00-KCQB", "00-LAQB"],
                "home_qb_name": ["LV QB", "BUF QB"],
                "away_qb_name": ["KC QB", "LA QB"],
                "home_coach": ["Coach LV", "Coach BUF"],
                "away_coach": ["Coach KC", "Coach LA"],
                "referee": ["Ref One", "Ref Two"],
                "stadium_id": ["VEG00", "SFO01"],
                "stadium": ["Allegiant Stadium", "Levi's Stadium"],
                "old_game_id": ["2025090700", "2026020800"],
                "pfr": ["202509070rai", "202602080buf"],
                "espn": ["401000001", "401000002"],
            }
        ),
    )

    # Team stats: 4 team-games, scoring formulas match home_score / away_score exactly.
    _write_clean(
        lake,
        "stats_team_week",
        "stats_team_week_2025.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK", "2025_01_KC_OAK", "2025_22_LAR_BUF", "2025_22_LAR_BUF"],
                "team": ["OAK", "KC", "BUF", "LAR"],
                "opponent_team": ["KC", "OAK", "LAR", "BUF"],
                "season": [2025, 2025, 2025, 2025],
                "week": [1, 1, 22, 22],
                "season_type": ["REG", "REG", "POST", "POST"],
                "completions": [20, 24, 22, 18],
                "attempts": [30, 32, 30, 28],
                "passing_yards": [210, 280, 240, 195],
                "passing_tds": [1, 2, 1, 1],
                "passing_interceptions": [1, 0, 0, 1],
                "carries": [22, 25, 26, 20],
                "rushing_yards": [105, 120, 115, 88],
                "rushing_tds": [1, 1, 1, 0],
                "receptions": [20, 24, 22, 18],
                "targets": [28, 30, 29, 26],
                "receiving_yards": [210, 280, 240, 195],
                "receiving_tds": [1, 2, 1, 1],
                "def_tds": [0, 0, 0, 0],
                "special_teams_tds": [0, 0, 0, 0],
                "fumble_recovery_tds": [0, 0, 0, 0],
                "fg_made": [0, 0, 1, 1],
                "pat_made": [2, 3, 2, 1],
                "rushing_2pt_conversions": [0, 0, 0, 0],
                "receiving_2pt_conversions": [0, 0, 0, 0],
                "def_2pt_made": [0, 0, 0, 0],
                "def_safeties": [0, 0, 0, 0],
                "def_sacks": [2, 3, 2, 1],
                "def_interceptions": [0, 1, 1, 0],
            }
        ),
    )

    # Player stats: 3 skill players (QB, RB, WR) per team-game = 12 rows; sums equal stats_team_week on all 12 columns.
    player_rows = [
        # Game 1: OAK (14 pts)
        ("2025_01_KC_OAK", "00-LVQB", 2025, 1, "REG", "OAK", "KC", 20, 30, 210, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ("2025_01_KC_OAK", "00-LVRB", 2025, 1, "REG", "OAK", "KC", 0, 0, 0, 0, 0, 22, 105, 1, 0, 0, 0, 0, 0, 0),
        ("2025_01_KC_OAK", "00-LVWR", 2025, 1, "REG", "OAK", "KC", 0, 0, 0, 0, 0, 0, 0, 0, 20, 28, 210, 1, 0, 0),
        # Game 1: KC (21 pts)
        ("2025_01_KC_OAK", "00-KCQB", 2025, 1, "REG", "KC", "OAK", 24, 32, 280, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ("2025_01_KC_OAK", "00-KCRB", 2025, 1, "REG", "KC", "OAK", 0, 0, 0, 0, 0, 25, 120, 1, 0, 0, 0, 0, 0, 0),
        ("2025_01_KC_OAK", "00-KCWR", 2025, 1, "REG", "KC", "OAK", 0, 0, 0, 0, 0, 0, 0, 0, 24, 30, 280, 2, 0, 0),
        # Game 2: BUF (17 pts)
        ("2025_22_LAR_BUF", "00-BUFQB", 2025, 22, "POST", "BUF", "LAR", 22, 30, 240, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ("2025_22_LAR_BUF", "00-BUFRB", 2025, 22, "POST", "BUF", "LAR", 0, 0, 0, 0, 0, 26, 115, 1, 0, 0, 0, 0, 0, 0),
        ("2025_22_LAR_BUF", "00-BUFWR", 2025, 22, "POST", "BUF", "LAR", 0, 0, 0, 0, 0, 0, 0, 0, 22, 29, 240, 1, 0, 0),
        # Game 2: LAR (10 pts)
        ("2025_22_LAR_BUF", "00-LAQB", 2025, 22, "POST", "LAR", "BUF", 18, 28, 195, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ("2025_22_LAR_BUF", "00-LARB", 2025, 22, "POST", "LAR", "BUF", 0, 0, 0, 0, 0, 20, 88, 0, 0, 0, 0, 0, 0, 0),
        ("2025_22_LAR_BUF", "00-LAWR", 2025, 22, "POST", "LAR", "BUF", 0, 0, 0, 0, 0, 0, 0, 0, 18, 26, 195, 1, 0, 0),
    ]
    _write_clean(
        lake,
        "stats_player_week",
        "stats_player_week_2025.parquet",
        pl.DataFrame(
            player_rows,
            schema=[
                "game_id",
                "player_id",
                "season",
                "week",
                "season_type",
                "team",
                "opponent_team",
                "completions",
                "attempts",
                "passing_yards",
                "passing_tds",
                "passing_interceptions",
                "carries",
                "rushing_yards",
                "rushing_tds",
                "receptions",
                "targets",
                "receiving_yards",
                "receiving_tds",
                "fg_made",
                "pat_made",
            ],
            orient="row",
        ).with_columns(
            pl.lit(0).alias("passing_2pt_conversions"),
            pl.lit(0).alias("rushing_fumbles_lost"),
            pl.lit(0).alias("fg_made_0_19"),
            pl.lit(0).alias("fg_made_20_29"),
            pl.lit(0).alias("fg_made_30_39"),
            pl.lit(0).alias("fg_made_40_49"),
            pl.lit(0).alias("fg_made_50_59"),
            pl.lit(0).alias("fg_made_60_"),
        ),
    )

    # Players master table: 12 skill players + 8 linemen per team + 1 injury-only player + 1 ESPN-rescue player.
    skill_specs = [
        ("00-LVQB", "LV QB", "QB", "OAK", "OAK", "LvQb00", "1001"),
        ("00-LVRB", "LV RB", "RB", "OAK", "OAK", "LvRb00", "1002"),
        ("00-LVWR", "LV WR", "WR", "OAK", "OAK", "LvWr00", "1003"),
        ("00-KCQB", "KC QB", "QB", "KC", "KC", "KcQb00", "2001"),
        ("00-KCRB", "KC RB", "RB", "KC", "KC", "KcRb00", "2002"),
        ("00-KCWR", "KC WR", "WR", "KC", "KC", "KcWr00", "2003"),
        ("00-BUFQB", "BUF QB", "QB", "BUF", "BUF", "BufQb00", "3001"),
        ("00-BUFRB", "BUF RB", "RB", "BUF", "BUF", "BufRb00", "3002"),
        ("00-BUFWR", "BUF WR", "WR", "BUF", "BUF", "BufWr00", "3003"),
        ("00-LAQB", "LA QB", "QB", "LAR", "STL", "LaQb00", "4001"),
        ("00-LARB", "LA RB", "RB", "LAR", "LAR", "LaRb00", "4002"),
        ("00-LAWR", "LA WR", "WR", "LAR", "LAR", "LaWr00", "4003"),
        ("00-INJONLY", "Injury Only", "TE", "KC", "KC", "InjOn00", "5001"),
        ("00-RESCUE", "Rescued Player", "WR", "OAK", "OAK", "Rescu00", "6001"),
    ]
    linemen_specs = []
    for team_code in ("OAK", "KC", "BUF", "LAR"):
        for slot in range(1, 9):
            linemen_specs.append(
                (
                    f"00-{team_code}OL{slot}",
                    f"{team_code} OL {slot}",
                    "OL",
                    team_code,
                    team_code,
                    f"{team_code}Ol0{slot}",
                    f"9{team_code[:1]}{slot}",
                )
            )
    all_players = skill_specs + linemen_specs
    _write_clean(
        lake,
        "players",
        "players.parquet",
        pl.DataFrame(
            all_players,
            schema=["gsis_id", "display_name", "position", "latest_team", "draft_team", "pfr_id", "espn_id"],
            orient="row",
        ),
    )

    # Weekly rosters (2025): include weeks 1 and 2 for 00-LVQB to verify latest sleeper_id / yahoo_id wins.
    _write_clean(
        lake,
        "weekly_rosters",
        "roster_weekly_2025.parquet",
        pl.DataFrame(
            {
                "season": [2025, 2025, 2025],
                "week": [1, 2, 1],
                "game_type": ["REG", "REG", "REG"],
                "team": ["OAK", "OAK", "KC"],
                "gsis_id": ["00-LVQB", "00-LVQB", "00-KCQB"],
                "position": ["QB", "QB", "QB"],
                "depth_chart_position": ["QB", "QB", "QB"],
                "status": ["ACT", "ACT", "ACT"],
                "status_description_abbr": ["A01", "A01", "A01"],
                "jersey_number": [4, 4, 15],
                "sleeper_id": ["OLD_SLP", "NEW_SLP", "KC_SLP"],
                "yahoo_id": ["OLD_YAH", "NEW_YAH", "KC_YAH"],
            }
        ),
    )

    # Injuries (2025): 00-LVQB (present in weekly_rosters) + 00-INJONLY (missing from weekly_rosters).
    _write_clean(
        lake,
        "injuries",
        "injuries_2025.parquet",
        pl.DataFrame(
            {
                "season": [2025, 2025],
                "week": [1, 1],
                "game_type": ["REG", "REG"],
                "team": ["OAK", "KC"],
                "gsis_id": ["00-LVQB", "00-INJONLY"],
                "position": ["QB", "TE"],
                "report_primary_injury": ["Ankle", "Knee"],
                "report_secondary_injury": [None, None],
                "report_status": ["Questionable", "Out"],
                "practice_primary_injury": ["Ankle", "Knee"],
                "practice_secondary_injury": [None, None],
                "practice_status": ["Limited Participation in Practice", "Did Not Participate In Practice"],
            }
        ),
    )

    # Snap counts: exactly 11 offensive players per team-game (60 snaps, 1.0 pct each => sum/max = 11.0).
    snap_rows = []
    team_game_specs = [
        ("2025_01_KC_OAK", "OAK", [("LvQb00", "QB"), ("LvRb00", "RB"), ("LvWr00", "WR")]),
        ("2025_01_KC_OAK", "KC", [("KcQb00", "QB"), ("KcRb00", "RB"), ("KcWr00", "WR")]),
        ("2025_22_LAR_BUF", "BUF", [("BufQb00", "QB"), ("BufRb00", "RB"), ("BufWr00", "WR")]),
        ("2025_22_LAR_BUF", "LAR", [("LaQb00", "QB"), ("LaRb00", "RB"), ("LaWr00", "WR")]),
    ]
    for game_id, team_code, skill_pfr_list in team_game_specs:
        for pfr_player_id, position in skill_pfr_list:
            snap_rows.append((game_id, 2025, team_code, pfr_player_id, position, 60, 1.0, 0, 0.0, 0, 0.0))
        for slot in range(1, 9):
            snap_rows.append((game_id, 2025, team_code, f"{team_code}Ol0{slot}", "OL", 60, 1.0, 0, 0.0, 0, 0.0))
    _write_clean(
        lake,
        "snap_counts",
        "snap_counts_2025.parquet",
        pl.DataFrame(
            snap_rows,
            schema=[
                "game_id",
                "season",
                "team",
                "pfr_player_id",
                "position",
                "offense_snaps",
                "offense_pct",
                "defense_snaps",
                "defense_pct",
                "st_snaps",
                "st_pct",
            ],
            orient="row",
        ),
    )

    # Play-by-play: sums match stats_team_week exactly (including a lateral rush on OAK's play 2 and a blank posteam row).
    pbp_rows = [
        ("2025_01_KC_OAK", 1.0, 2025, 1, "OAK", "KC", "OAK", "KC", 210.0, 0.0, 0.0, 1, 0),
        ("2025_01_KC_OAK", 2.0, 2025, 1, "OAK", "KC", "OAK", "KC", 0.0, 95.0, 10.0, 0, 1),
        ("2025_01_KC_OAK", 3.0, 2025, 1, "OAK", "KC", "", "", 0.0, 0.0, 0.0, 0, 0),
        ("2025_01_KC_OAK", 4.0, 2025, 1, "OAK", "KC", "KC", "OAK", 280.0, 120.0, 0.0, 2, 1),
        ("2025_22_LAR_BUF", 1.0, 2025, 22, "BUF", "LAR", "BUF", "LAR", 240.0, 115.0, 0.0, 1, 1),
        ("2025_22_LAR_BUF", 2.0, 2025, 22, "BUF", "LAR", "LAR", "BUF", 195.0, 88.0, 0.0, 1, 0),
    ]
    _write_clean(
        lake,
        "pbp",
        "play_by_play_2025.parquet",
        pl.DataFrame(
            pbp_rows,
            schema=[
                "game_id",
                "play_id",
                "season",
                "week",
                "home_team",
                "away_team",
                "posteam",
                "defteam",
                "passing_yards",
                "rushing_yards",
                "lateral_rushing_yards",
                "pass_touchdown",
                "rush_touchdown",
            ],
            orient="row",
        ),
    )

    _write_clean(
        lake,
        "pbp_participation",
        "pbp_participation_2025.parquet",
        pl.DataFrame(
            {
                "nflverse_game_id": ["2025_01_KC_OAK"],
                "play_id": [1.0],
                "offense_formation": ["SHOTGUN"],
                "offense_personnel": ["1 RB, 1 TE, 3 WR"],
                "defenders_in_box": [6],
                "defense_personnel": ["4 DL, 2 LB, 5 DB"],
                "number_of_pass_rushers": [4],
                "n_offense": [11],
                "n_defense": [11],
                "offense_players": ["00-LVQB;00-LVWR"],
                "defense_players": ["00-KCOL1"],
                "ngs_air_yards": [12.5],
                "time_to_throw": [2.4],
                "was_pressure": [False],
                "route": ["SLANT"],
                "defense_man_zone_type": ["ZONE_COVERAGE"],
                "defense_coverage_type": ["COVER_3"],
            }
        ),
    )

    _write_clean(
        lake,
        "ftn_charting",
        "ftn_charting_2025.parquet",
        pl.DataFrame(
            {
                "nflverse_game_id": ["2025_01_KC_OAK", "2025_22_LAR_BUF"],
                "nflverse_play_id": [1, 1],
                "starting_hash": ["L", "R"],
                "qb_location": ["S", "U"],
                "n_offense_backfield": [1, 1],
                "n_defense_box": [6, 7],
                "is_no_huddle": [False, True],
                "is_motion": [True, False],
                "is_play_action": [True, False],
                "is_screen_pass": [False, False],
                "is_rpo": [False, False],
                "is_trick_play": [False, False],
                "is_qb_out_of_pocket": [False, True],
                "is_interception_worthy": [False, False],
                "is_throw_away": [False, False],
                "read_thrown": ["1", "2"],
                "is_catchable_ball": [True, True],
                "is_contested_ball": [False, False],
                "is_created_reception": [False, False],
                "is_drop": [False, False],
                "is_qb_sneak": [False, False],
                "n_blitzers": [0, 1],
                "n_pass_rushers": [4, 5],
                "is_qb_fault_sack": [False, False],
            }
        ),
    )

    # Depth charts (2025): include one row with empty gsis_id that must be rescued via players.espn_id = "6001".
    _write_clean(
        lake,
        "depth_charts",
        "depth_charts_2025.parquet",
        pl.DataFrame(
            {
                "dt": ["2025-09-07T15:00:00Z", "2025-09-07T15:00:00Z"],
                "team": ["OAK", "OAK"],
                "gsis_id": ["00-LVQB", ""],
                "espn_id": ["1001", "6001"],
                "player_name": ["LV QB", "Rescued Player"],
                "pos_grp_id": ["1", "2"],
                "pos_grp": ["Offense", "Offense"],
                "pos_id": ["10", "20"],
                "pos_name": ["Quarterback", "Wide Receiver"],
                "pos_abb": ["QB", "WR"],
                "pos_slot": [1, 2],
                "pos_rank": [1, 1],
            }
        ),
    )

    # Next Gen Stats: include REG week 1, POST Super Bowl week 23 (-> 22), and season-summary week 0 (ignored).
    _write_clean(
        lake,
        "ngs_passing",
        "ngs_passing.parquet",
        pl.DataFrame(
            {
                "season": [2025, 2025, 2025],
                "season_type": ["REG", "POST", "REG"],
                "week": [1, 23, 0],
                "player_gsis_id": ["00-LVQB", "00-BUFQB", "00-LVQB"],
                "attempts": [30, 30, 500],
                "avg_time_to_throw": [2.6, 2.8, 2.7],
                "avg_completed_air_yards": [6.1, 7.2, 6.5],
                "avg_intended_air_yards": [8.2, 9.1, 8.4],
                "avg_air_yards_differential": [-2.1, -1.9, -1.9],
                "aggressiveness": [14.0, 16.5, 15.0],
                "max_completed_air_distance": [41.2, 48.0, 52.0],
                "avg_air_yards_to_sticks": [-0.5, 0.3, -0.2],
                "expected_completion_percentage": [64.5, 68.0, 65.0],
                "completion_percentage_above_expectation": [2.2, 5.3, 1.5],
                "avg_air_distance": [20.1, 22.3, 21.0],
                "max_air_distance": [45.0, 51.0, 54.0],
            }
        ),
    )
    _write_clean(
        lake,
        "ngs_rushing",
        "ngs_rushing.parquet",
        pl.DataFrame(
            {
                "season": [2025, 2025],
                "season_type": ["REG", "POST"],
                "week": [1, 23],
                "player_gsis_id": ["00-LVRB", "00-BUFRB"],
                "efficiency": [3.8, 3.5],
                "percent_attempts_gte_eight_defenders": [22.0, 18.0],
                "avg_time_to_los": [2.7, 2.5],
                "expected_rush_yards": [92.0, 100.0],
                "rush_yards_over_expected": [13.0, 15.0],
                "rush_yards_over_expected_per_att": [0.59, 0.58],
                "rush_pct_over_expected": [45.0, 50.0],
            }
        ),
    )
    _write_clean(
        lake,
        "ngs_receiving",
        "ngs_receiving.parquet",
        pl.DataFrame(
            {
                "season": [2025, 2025],
                "season_type": ["REG", "POST"],
                "week": [1, 23],
                "player_gsis_id": ["00-LVWR", "00-BUFWR"],
                "avg_cushion": [5.5, 6.2],
                "avg_separation": [3.1, 3.4],
                "avg_intended_air_yards": [9.4, 11.2],
                "percent_share_of_intended_air_yards": [38.0, 42.0],
                "avg_yac": [4.8, 5.1],
                "avg_expected_yac": [4.1, 4.4],
                "avg_yac_above_expectation": [0.7, 0.7],
            }
        ),
    )

    # PFR weekly advanced stats (bridged via pfr_player_id -> player.pfr_id).
    _write_clean(
        lake,
        "pfr_week_pass",
        "advstats_week_pass_2025.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK", "2025_22_LAR_BUF"],
                "pfr_player_id": ["LvQb00", "BufQb00"],
                "passing_drops": [1, 2],
                "passing_drop_pct": [0.033, 0.067],
                "passing_bad_throws": [4, 3],
                "passing_bad_throw_pct": [0.133, 0.100],
                "times_sacked": [2, 1],
                "times_blitzed": [8, 6],
                "times_hurried": [3, 2],
                "times_hit": [2, 1],
                "times_pressured": [7, 4],
                "times_pressured_pct": [0.219, 0.129],
            }
        ),
    )
    _write_clean(
        lake,
        "pfr_week_rush",
        "advstats_week_rush_2025.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK"],
                "pfr_player_id": ["LvRb00"],
                "rushing_yards_before_contact": [60.0],
                "rushing_yards_before_contact_avg": [2.7],
                "rushing_yards_after_contact": [45.0],
                "rushing_yards_after_contact_avg": [2.0],
                "rushing_broken_tackles": [3],
            }
        ),
    )
    _write_clean(
        lake,
        "pfr_week_rec",
        "advstats_week_rec_2025.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK"],
                "pfr_player_id": ["LvWr00"],
                "receiving_broken_tackles": [2],
                "receiving_drop": [1],
                "receiving_drop_pct": [0.036],
                "receiving_int": [0],
                "receiving_rat": [108.5],
            }
        ),
    )
    _write_clean(
        lake,
        "pfr_week_def",
        "advstats_week_def_2025.parquet",
        pl.DataFrame(
            {
                "game_id": ["2025_01_KC_OAK"],
                "pfr_player_id": ["KcQb00"],
                "def_ints": [0],
                "def_targets": [0],
                "def_completions_allowed": [0],
                "def_completion_pct": [0.0],
                "def_yards_allowed": [0.0],
                "def_receiving_td_allowed": [0],
                "def_passer_rating_allowed": [0.0],
                "def_adot": [0.0],
                "def_times_blitzed": [0],
                "def_times_hurried": [0],
                "def_times_hitqb": [0],
                "def_sacks": [0.0],
                "def_pressures": [0],
                "def_tackles_combined": [1],
                "def_missed_tackles": [0],
                "def_missed_tackle_pct": [0.0],
            }
        ),
    )


def test_build_core_publishes_all_seven_tables_and_normalizes_sources(lake: LakeStore):
    _populate_valid_clean_lake(lake)

    summary = build_core_tables(lake)

    assert summary.report.passed
    assert not summary.report.warnings
    assert not lake.staging_dir.exists()
    assert summary.row_counts_by_table == {
        "game": 2,
        "team_game": 4,
        "player": 46,
        "player_game": 12,
        "play": 6,
        "player_week_status": 4,
        "depth_chart_snapshot": 2,
    }

    # game: normalized teams + EDT/EST -> UTC kickoffs
    games = pl.read_parquet(lake.core_dir / "game.parquet").sort("game_id")
    assert games["home_team"].to_list() == ["LV", "BUF"]
    assert games["away_team"].to_list() == ["KC", "LA"]
    assert games["kickoff_utc"].to_list() == [
        datetime(2025, 9, 7, 17, 0, tzinfo=UTC),
        datetime(2026, 2, 8, 23, 30, tzinfo=UTC),
    ]

    # team_game: favored_by is positive when that team is favored
    team_game = pl.read_parquet(lake.core_dir / "team_game.parquet")
    oak_game = team_game.filter((pl.col("game_id") == "2025_01_KC_OAK") & (pl.col("team") == "LV"))
    kc_game = team_game.filter((pl.col("game_id") == "2025_01_KC_OAK") & (pl.col("team") == "KC"))
    assert oak_game["is_home"].item() is True
    assert oak_game["favored_by"].item() == -3.5
    assert kc_game["is_home"].item() is False
    assert kc_game["favored_by"].item() == 3.5

    # player: latest sleeper_id & yahoo_id from weekly_rosters + normalized draft_team (STL -> LA)
    players = pl.read_parquet(lake.core_dir / "player.parquet")
    lv_qb = players.filter(pl.col("gsis_id") == "00-LVQB")
    la_qb = players.filter(pl.col("gsis_id") == "00-LAQB")
    assert lv_qb["sleeper_id"].item() == "NEW_SLP"
    assert lv_qb["yahoo_id"].item() == "NEW_YAH"
    assert la_qb["latest_team"].item() == "LA"
    assert la_qb["draft_team"].item() == "LA"

    # player_game: snaps, NGS (including SB week 23 -> 22), and PFR advanced stats joined cleanly
    player_game = pl.read_parquet(lake.core_dir / "player_game.parquet")
    buf_qb_sb = player_game.filter((pl.col("game_id") == "2025_22_LAR_BUF") & (pl.col("player_id") == "00-BUFQB"))
    lv_qb_w1 = player_game.filter((pl.col("game_id") == "2025_01_KC_OAK") & (pl.col("player_id") == "00-LVQB"))
    assert buf_qb_sb["offense_snaps"].item() == 60
    assert buf_qb_sb["ngs_pass_attempts"].item() == 30
    assert buf_qb_sb["pfr_pass_drops"].item() == 2
    assert lv_qb_w1["team"].item() == "LV"
    assert lv_qb_w1["ngs_pass_attempts"].item() == 30

    # play: blank posteam -> NULL, teams normalized, participation & FTN attached
    plays = pl.read_parquet(lake.core_dir / "play" / "play_2025.parquet").sort("game_id", "play_id")
    play_one = plays.filter((pl.col("game_id") == "2025_01_KC_OAK") & (pl.col("play_id") == 1.0))
    play_three = plays.filter((pl.col("game_id") == "2025_01_KC_OAK") & (pl.col("play_id") == 3.0))
    assert play_one["posteam"].item() == "LV"
    assert play_one["offense_formation"].item() == "SHOTGUN"
    assert play_one["ftn_is_play_action"].item() is True
    assert play_three["posteam"].item() is None

    # player_week_status: FULL OUTER JOIN preserves injury-only row
    status_frame = pl.read_parquet(lake.core_dir / "player_week_status.parquet")
    injury_only = status_frame.filter(pl.col("gsis_id") == "00-INJONLY")
    assert injury_only.height == 1
    assert injury_only["report_status"].item() == "Out"
    assert injury_only["roster_status"].item() is None

    # depth_chart_snapshot: null/empty gsis_id rescued via players.espn_id
    depth_frame = pl.read_parquet(lake.core_dir / "depth_chart_snapshot.parquet")
    rescued = depth_frame.filter(pl.col("espn_id") == "6001")
    assert rescued["gsis_id"].item() == "00-RESCUE"
    assert rescued["team"].item() == "LV"

    # verify_core_tables --deep succeeds across all 7 published files
    checked_count, errors = verify_core_tables(lake, deep=True)
    assert checked_count == 7
    assert errors == []


def test_build_core_blocks_publish_when_a_strategy_1_check_fails(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)
    published_path = lake.core_dir / "player_game.parquet"
    original_sha256 = compute_file_sha256(published_path)

    # Break player vs team checkbook identity: add 50 passing yards to one player without updating team stats.
    clean_player_path = lake.clean_file_path("stats_player_week", "stats_player_week_2025.parquet")
    broken_stats = pl.read_parquet(clean_player_path).with_columns(
        pl.when(pl.col("player_id") == "00-LVQB")
        .then(pl.col("passing_yards") + 50)
        .otherwise(pl.col("passing_yards"))
        .alias("passing_yards")
    )
    broken_stats.write_parquet(clean_player_path)

    with pytest.raises(CoreBuildFailed) as exc_info:
        build_core_tables(lake)

    assert exc_info.value.report is not None
    failed_names = {outcome.name for outcome in exc_info.value.report.errors}
    assert "core_player_sums_equal_team" in failed_names
    assert compute_file_sha256(published_path) == original_sha256  # previous published core is untouched
    assert not lake.staging_dir.exists()


def test_build_core_blocks_duplicate_pfr_bridge_id(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    players_path = lake.clean_file_path("players", "players.parquet")
    duplicate_bridge = pl.read_parquet(players_path).with_columns(
        pl.when(pl.col("gsis_id") == "00-LVRB").then(pl.lit("LvQb00")).otherwise(pl.col("pfr_id")).alias("pfr_id")
    )
    duplicate_bridge.write_parquet(players_path)

    with pytest.raises(CoreBuildFailed) as exc_info:
        build_core_tables(lake)

    assert exc_info.value.report is not None
    failed_names = {outcome.name for outcome in exc_info.value.report.errors}
    assert "core_players_bridge_ids_unique" in failed_names
    assert not lake.core_dir.exists()


def test_verify_core_detects_checksum_tampering_row_loss_and_missing_files(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)

    # Tamper a non-key non-checked column (stadium name) while keeping row count valid -> --deep catches checksum mismatch.
    game_path = lake.core_dir / "game.parquet"
    tampered_game = pl.read_parquet(game_path).with_columns(pl.lit("Tampered Stadium").alias("stadium"))
    tampered_game.write_parquet(game_path)

    _, fast_errors = verify_core_tables(lake, deep=False)
    _, deep_errors = verify_core_tables(lake, deep=True)
    assert fast_errors == []
    assert len(deep_errors) == 1

    # Drop a row from core/player.parquet -> even fast verify catches row-count mismatch.
    player_path = lake.core_dir / "player.parquet"
    pl.read_parquet(player_path).head(10).write_parquet(player_path)
    _, row_errors = verify_core_tables(lake, deep=False)
    assert row_errors

    # Delete core/team_game.parquet -> fast verify catches missing file.
    (lake.core_dir / "team_game.parquet").unlink()
    _, missing_errors = verify_core_tables(lake, deep=False)
    assert missing_errors


def test_verify_core_detects_stray_play_parquet(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)

    # An empty stray file does not change row count, so verify must still flag it as untracked.
    stray_path = lake.core_dir / "play" / "play_1998.parquet"
    pl.read_parquet(lake.core_dir / "play" / "play_2025.parquet").clear().write_parquet(stray_path)

    _, errors = verify_core_tables(lake, deep=False)
    assert errors


def test_anchor_check_fails_when_anchor_value_is_null_or_player_row_missing(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)

    # Put a real anchor game_id (`2023_22_SF_KC`) into `core/game.parquet` with no matching Mahomes row in player_game:
    # SQL `LEFT JOIN` + `IS DISTINCT FROM` must fail instead of silently passing on NULL / missing row.
    game_path = lake.core_dir / "game.parquet"
    games = pl.read_parquet(game_path)
    fake_sb58 = games.head(1).with_columns(pl.lit("2023_22_SF_KC").alias("game_id"))
    pl.concat([games, fake_sb58]).write_parquet(game_path)

    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone = 'UTC'")
        anchor_check = next(
            check
            for check in build_core_checks(lake.core_dir, lake.clean_dir)
            if check.name == "core_anchor_games_exact"
        )
        report = run_checks(connection, [anchor_check])
    finally:
        connection.close()
    assert not report.passed
    assert report.errors[0].failure_count == 1

    # Now add Mahomes's row with NULL offense_snaps (simulating a broken snap join): must STILL fail!
    player_game_path = lake.core_dir / "player_game.parquet"
    player_game = pl.read_parquet(player_game_path)
    mahomes_null_snaps = player_game.head(1).with_columns(
        pl.lit("2023_22_SF_KC").alias("game_id"),
        pl.lit("00-0033873").alias("player_id"),
        pl.lit(333, dtype=player_game.schema["passing_yards"]).alias("passing_yards"),
        pl.lit(66, dtype=player_game.schema["rushing_yards"]).alias("rushing_yards"),
        pl.lit(None, dtype=player_game.schema["offense_snaps"]).alias("offense_snaps"),
        pl.lit(46, dtype=player_game.schema["ngs_pass_attempts"]).alias("ngs_pass_attempts"),
    )
    pl.concat([player_game, mahomes_null_snaps]).write_parquet(player_game_path)

    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone = 'UTC'")
        report = run_checks(connection, [anchor_check])
    finally:
        connection.close()
    assert not report.passed
    assert report.errors[0].failure_count == 1


def test_build_core_requires_clean_inputs_and_respects_lock(lake: LakeStore):
    with pytest.raises(CoreBuildFailed):
        build_core_tables(lake)

    lake.lock_path.parent.mkdir(parents=True, exist_ok=True)
    lake.lock_path.write_text("{}")
    with pytest.raises(LakeLocked):
        build_core_tables(lake)


def test_cli_build_core_exits_0_when_healthy_and_1_when_blocked(lake: LakeStore, monkeypatch, capsys):
    _populate_valid_clean_lake(lake)
    monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root_dir))
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    main(["build-core"])
    assert (lake.core_dir / "game.parquet").exists()
    assert lake.load_core_state()

    # Corrupt schedules score so core_team_points_equal_schedule blocks build-core via CLI exit code 1.
    sched_path = lake.clean_file_path("schedules", "games.parquet")
    broken_sched = pl.read_parquet(sched_path).with_columns(pl.lit(99).alias("home_score"))
    broken_sched.write_parquet(sched_path)

    capsys.readouterr()
    with pytest.raises(SystemExit) as exit_info:
        main(["build-core"])
    assert exit_info.value.code == 1


@pytest.mark.parametrize(
    "bad_json",
    [
        "[]",
        '{"game": {}}',
        '{"game": {"rows": 1, "files": [], "built_at": "x"}}',
        '{"game": {"rows": "1", "files": {"core/game.parquet": "abc"}, "built_at": "x"}}',
        '{"game": {"rows": true, "files": {"core/game.parquet": "abc"}, "built_at": "x"}}',
        '{"game": {"rows": 1, "files": {"../outside.parquet": "abc"}, "built_at": "x"}}',
    ],
)
def test_damaged_core_state_is_refused(lake: LakeStore, bad_json: str):
    lake.manifest_dir.mkdir(parents=True, exist_ok=True)
    (lake.manifest_dir / "core_state.json").write_text(bad_json, encoding="utf-8")
    with pytest.raises(ValueError):
        lake.load_core_state()
