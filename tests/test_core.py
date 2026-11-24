"""Stage 4 core tables: what build-core publishes, what blocks it, and what verify reports. No network."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from nfl_lake.cli import main
from nfl_lake.core import CoreBuildFailed, build_core_tables, verify_core_tables
from nfl_lake.source import compute_file_sha256
from nfl_lake.store import LakeLocked, LakeStore

GAME_1 = "2025_01_KC_OAK"
GAME_2 = "2025_22_LAR_BUF"


def _write_clean(lake: LakeStore, dataset_name: str, filename: str, frame: pl.DataFrame) -> Path:
    dest_path = lake.clean_file_path(dataset_name, filename)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(dest_path)
    return dest_path


def _names(column_names: str) -> list[str]:
    return column_names.split()


def _filler(column_names: str, value, row_count: int) -> dict[str, list]:
    """Columns that core selects and no test looks at: the same value in every row."""
    return {column_name: [value] * row_count for column_name in _names(column_names)}


def _populate_valid_clean_lake(lake: LakeStore) -> None:
    """Create a small 2025 clean lake that passes every check.

    - Game 1 (`2025_01_KC_OAK`): EDT kickoff (`13:00` -> `17:00Z`), `OAK -> LV`, a lateral rushing play, and a play
      with a blank `posteam`.
    - Game 2 (`2025_22_LAR_BUF`): Super Bowl EST kickoff (`18:30` -> `23:30Z`), `LAR -> LA`, Next Gen Stats rows in
      Super Bowl week 23 (schedule week 22), and a `week = 0` season-total row that must not join.
    - Each team has a QB, RB and WR with stat lines, and 8 linemen who appear only in snap_counts.
    """
    _write_clean(
        lake,
        "schedules",
        "games.parquet",
        pl.DataFrame(
            {
                "game_id": [GAME_1, GAME_2],
                "season": [2025, 2025],
                "game_type": ["REG", "SB"],
                "week": [1, 22],
                "gameday": ["2025-09-07", "2026-02-08"],
                "gametime": ["13:00", "18:30"],
                "home_team": ["OAK", "BUF"],
                "away_team": ["KC", "LAR"],
                "home_score": [14, 17],
                "away_score": [21, 10],
                "spread_line": [-3.5, 2.5],
                **_filler(
                    """weekday location roof surface home_qb_id away_qb_id home_qb_name away_qb_name home_coach
                    away_coach referee stadium_id stadium old_game_id pfr espn""",
                    "x",
                    2,
                ),
                **_filler(
                    """result total overtime div_game total_line home_moneyline away_moneyline home_spread_odds
                    away_spread_odds over_odds under_odds home_rest away_rest temp wind""",
                    0,
                    2,
                ),
            }
        ),
    )

    # Team stats: the scoring columns add up to home_score and away_score.
    _write_clean(
        lake,
        "stats_team_week",
        "stats_team_week_2025.parquet",
        pl.DataFrame(
            {
                "game_id": [GAME_1, GAME_1, GAME_2, GAME_2],
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
                "fg_made": [0, 0, 1, 1],
                "pat_made": [2, 3, 2, 1],
                **_filler(
                    """def_tds special_teams_tds fumble_recovery_tds rushing_2pt_conversions
                    receiving_2pt_conversions def_2pt_made def_safeties def_sacks def_interceptions""",
                    0,
                    4,
                ),
            }
        ),
    )

    # Player stats: a QB, RB and WR per team-game, whose sums equal stats_team_week on all 12 columns.
    player_rows = [
        (GAME_1, "00-LVQB", 2025, 1, "REG", "OAK", "KC", 20, 30, 210, 1, 1, 0, 0, 0, 0, 0, 0, 0),
        (GAME_1, "00-LVRB", 2025, 1, "REG", "OAK", "KC", 0, 0, 0, 0, 0, 22, 105, 1, 0, 0, 0, 0),
        (GAME_1, "00-LVWR", 2025, 1, "REG", "OAK", "KC", 0, 0, 0, 0, 0, 0, 0, 0, 20, 28, 210, 1),
        (GAME_1, "00-KCQB", 2025, 1, "REG", "KC", "OAK", 24, 32, 280, 2, 0, 0, 0, 0, 0, 0, 0, 0),
        (GAME_1, "00-KCRB", 2025, 1, "REG", "KC", "OAK", 0, 0, 0, 0, 0, 25, 120, 1, 0, 0, 0, 0),
        (GAME_1, "00-KCWR", 2025, 1, "REG", "KC", "OAK", 0, 0, 0, 0, 0, 0, 0, 0, 24, 30, 280, 2),
        (GAME_2, "00-BUFQB", 2025, 22, "POST", "BUF", "LAR", 22, 30, 240, 1, 0, 0, 0, 0, 0, 0, 0, 0),
        (GAME_2, "00-BUFRB", 2025, 22, "POST", "BUF", "LAR", 0, 0, 0, 0, 0, 26, 115, 1, 0, 0, 0, 0),
        (GAME_2, "00-BUFWR", 2025, 22, "POST", "BUF", "LAR", 0, 0, 0, 0, 0, 0, 0, 0, 22, 29, 240, 1),
        (GAME_2, "00-LAQB", 2025, 22, "POST", "LAR", "BUF", 18, 28, 195, 1, 1, 0, 0, 0, 0, 0, 0, 0),
        (GAME_2, "00-LARB", 2025, 22, "POST", "LAR", "BUF", 0, 0, 0, 0, 0, 20, 88, 0, 0, 0, 0, 0),
        (GAME_2, "00-LAWR", 2025, 22, "POST", "LAR", "BUF", 0, 0, 0, 0, 0, 0, 0, 0, 18, 26, 195, 1),
    ]
    _write_clean(
        lake,
        "stats_player_week",
        "stats_player_week_2025.parquet",
        pl.DataFrame(
            player_rows,
            schema=_names(
                """game_id player_id season week season_type team opponent_team completions attempts passing_yards
                passing_tds passing_interceptions carries rushing_yards rushing_tds receptions targets receiving_yards
                receiving_tds"""
            ),
            orient="row",
        ).with_columns(
            pl.col("player_id").str.slice(-2).alias("position"),  # QB, RB or WR
            *[
                pl.lit(0).alias(column_name)
                for column_name in _names(
                    """fg_made pat_made passing_2pt_conversions rushing_fumbles_lost fg_made_0_19 fg_made_20_29
                    fg_made_30_39 fg_made_40_49 fg_made_50_59 fg_made_60_"""
                )
            ],
        ),
    )

    # Players: the 12 with stat lines, 8 linemen per team, one who is only on an injury report, and one whom a
    # depth chart names by ESPN id alone.
    player_specs = [
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
    for team_code in ("OAK", "KC", "BUF", "LAR"):
        for slot in range(1, 9):
            player_specs.append(
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
    _write_clean(
        lake,
        "players",
        "players.parquet",
        pl.DataFrame(
            player_specs,
            schema=["gsis_id", "display_name", "position", "latest_team", "draft_team", "pfr_id", "espn_id"],
            orient="row",
        ),
    )

    # Weekly rosters: 00-LVQB has weeks 1 and 2, so the newer sleeper_id and yahoo_id must win.
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

    # Injuries: 00-LVQB is also on a roster, 00-INJONLY is not.
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
                "date_modified": [datetime(2025, 9, 5, 20, 0, tzinfo=UTC), datetime(2025, 9, 5, 21, 30, tzinfo=UTC)],
            }
        ),
    )

    # Snap counts: 11 offensive players per team-game with 60 snaps each, so snaps summed over their maximum is 11.
    snap_rows = []
    for game_id, team_code, opponent_code, skill_pfr_ids in [
        (GAME_1, "OAK", "KC", ("LvQb00", "LvRb00", "LvWr00")),
        (GAME_1, "KC", "OAK", ("KcQb00", "KcRb00", "KcWr00")),
        (GAME_2, "BUF", "LAR", ("BufQb00", "BufRb00", "BufWr00")),
        (GAME_2, "LAR", "BUF", ("LaQb00", "LaRb00", "LaWr00")),
    ]:
        for pfr_player_id, position in zip(skill_pfr_ids, ("QB", "RB", "WR"), strict=True):
            snap_rows.append((game_id, 2025, team_code, opponent_code, pfr_player_id, position, 60, 1.0))
        for slot in range(1, 9):
            snap_rows.append((game_id, 2025, team_code, opponent_code, f"{team_code}Ol0{slot}", "OL", 60, 1.0))
    _write_clean(
        lake,
        "snap_counts",
        "snap_counts_2025.parquet",
        pl.DataFrame(
            snap_rows,
            schema=_names("game_id season team opponent pfr_player_id position offense_snaps offense_pct"),
            orient="row",
        ).with_columns(
            pl.lit(0).alias("defense_snaps"),
            pl.lit(0.0).alias("defense_pct"),
            pl.lit(0).alias("st_snaps"),
            pl.lit(0.0).alias("st_pct"),
        ),
    )

    # Play-by-play: sums equal stats_team_week, counting the lateral on OAK's second play.
    pbp_rows = [
        (GAME_1, 1.0, 2025, 1, "OAK", "KC", "OAK", "KC", 210.0, 0.0, 0.0, 1, 0),
        (GAME_1, 2.0, 2025, 1, "OAK", "KC", "OAK", "KC", 0.0, 95.0, 10.0, 0, 1),
        (GAME_1, 3.0, 2025, 1, "OAK", "KC", "", "", 0.0, 0.0, 0.0, 0, 0),
        (GAME_1, 4.0, 2025, 1, "OAK", "KC", "KC", "OAK", 280.0, 120.0, 0.0, 2, 1),
        (GAME_2, 1.0, 2025, 22, "BUF", "LAR", "BUF", "LAR", 240.0, 115.0, 0.0, 1, 1),
        (GAME_2, 2.0, 2025, 22, "BUF", "LAR", "LAR", "BUF", 195.0, 88.0, 0.0, 1, 0),
    ]
    _write_clean(
        lake,
        "pbp",
        "play_by_play_2025.parquet",
        pl.DataFrame(
            pbp_rows,
            schema=_names(
                """game_id play_id season week home_team away_team posteam defteam passing_yards rushing_yards
                lateral_rushing_yards pass_touchdown rush_touchdown"""
            ),
            orient="row",
        ),
    )

    _write_clean(
        lake,
        "pbp_participation",
        "pbp_participation_2025.parquet",
        pl.DataFrame(
            {
                "nflverse_game_id": [GAME_1],
                "play_id": [1.0],
                "offense_formation": ["SHOTGUN"],
                "was_pressure": [False],
                **_filler(
                    """offense_personnel defense_personnel offense_players defense_players route
                    defense_man_zone_type defense_coverage_type""",
                    "x",
                    1,
                ),
                **_filler("defenders_in_box number_of_pass_rushers n_offense n_defense", 11, 1),
                **_filler("ngs_air_yards time_to_throw", 2.5, 1),
            }
        ),
    )

    _write_clean(
        lake,
        "ftn_charting",
        "ftn_charting_2025.parquet",
        pl.DataFrame(
            {
                "nflverse_game_id": [GAME_1, GAME_2],
                "nflverse_play_id": [1, 1],
                "is_play_action": [True, False],
                **_filler("starting_hash qb_location read_thrown", "x", 2),
                **_filler("n_offense_backfield n_defense_box n_blitzers n_pass_rushers", 1, 2),
                **_filler(
                    """is_no_huddle is_motion is_screen_pass is_rpo is_trick_play is_qb_out_of_pocket
                    is_interception_worthy is_throw_away is_catchable_ball is_contested_ball is_created_reception
                    is_drop is_qb_sneak is_qb_fault_sack""",
                    False,
                    2,
                ),
            }
        ),
    )

    # Depth charts: the second row has no gsis_id and must be found through players.espn_id.
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

    # Next Gen Stats: a regular-season week, the Super Bowl as week 23, and a week 0 season total.
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
                **_filler(
                    """avg_time_to_throw avg_completed_air_yards avg_intended_air_yards avg_air_yards_differential
                    aggressiveness max_completed_air_distance avg_air_yards_to_sticks expected_completion_percentage
                    completion_percentage_above_expectation avg_air_distance max_air_distance""",
                    1.5,
                    3,
                ),
            }
        ),
    )
    for dataset_name, player_ids, stat_columns in [
        (
            "ngs_rushing",
            ["00-LVRB", "00-BUFRB"],
            """efficiency percent_attempts_gte_eight_defenders avg_time_to_los expected_rush_yards
            rush_yards_over_expected rush_yards_over_expected_per_att rush_pct_over_expected""",
        ),
        (
            "ngs_receiving",
            ["00-LVWR", "00-BUFWR"],
            """avg_cushion avg_separation avg_intended_air_yards percent_share_of_intended_air_yards avg_yac
            avg_expected_yac avg_yac_above_expectation""",
        ),
    ]:
        _write_clean(
            lake,
            dataset_name,
            f"{dataset_name}.parquet",
            pl.DataFrame(
                {
                    "season": [2025, 2025],
                    "season_type": ["REG", "POST"],
                    "week": [1, 23],
                    "player_gsis_id": player_ids,
                    **_filler(stat_columns, 1.5, 2),
                }
            ),
        )

    # PFR weekly advanced stats, which reach a player through pfr_player_id.
    _write_clean(
        lake,
        "pfr_week_pass",
        "advstats_week_pass_2025.parquet",
        pl.DataFrame(
            {
                "game_id": [GAME_1, GAME_2],
                "pfr_player_id": ["LvQb00", "BufQb00"],
                "passing_drops": [1, 2],
                **_filler(
                    """passing_drop_pct passing_bad_throws passing_bad_throw_pct times_sacked times_blitzed
                    times_hurried times_hit times_pressured times_pressured_pct""",
                    1.0,
                    2,
                ),
            }
        ),
    )
    for stat_kind, pfr_player_id, stat_columns in [
        (
            "rush",
            "LvRb00",
            """rushing_yards_before_contact rushing_yards_before_contact_avg rushing_yards_after_contact
            rushing_yards_after_contact_avg rushing_broken_tackles""",
        ),
        ("rec", "LvWr00", "receiving_broken_tackles receiving_drop receiving_drop_pct receiving_int receiving_rat"),
        (
            "def",
            "KcQb00",
            """def_ints def_targets def_completions_allowed def_completion_pct def_yards_allowed
            def_receiving_td_allowed def_passer_rating_allowed def_adot def_times_blitzed def_times_hurried
            def_times_hitqb def_sacks def_pressures def_tackles_combined def_missed_tackles def_missed_tackle_pct""",
        ),
    ]:
        _write_clean(
            lake,
            f"pfr_week_{stat_kind}",
            f"advstats_week_{stat_kind}_2025.parquet",
            pl.DataFrame({"game_id": [GAME_1], "pfr_player_id": [pfr_player_id], **_filler(stat_columns, 1.0, 1)}),
        )


def _rewrite_clean(lake: LakeStore, dataset_name: str, filename: str, *changes: pl.Expr) -> None:
    """Change a clean file in place, as a later update would."""
    clean_path = lake.clean_file_path(dataset_name, filename)
    pl.read_parquet(clean_path).with_columns(*changes).write_parquet(clean_path)


def _add_unplayed_game(lake: LakeStore, game_id: str) -> None:
    """Put one more game on the schedule. A game not yet played needs no stats or plays, so no check minds it."""
    sched_path = lake.clean_file_path("schedules", "games.parquet")
    games = pl.read_parquet(sched_path)
    unplayed_game = games.tail(1).with_columns(
        pl.lit(game_id).alias("game_id"),
        pl.lit(None, dtype=games.schema["home_score"]).alias("home_score"),
        pl.lit(None, dtype=games.schema["away_score"]).alias("away_score"),
    )
    pl.concat([games, unplayed_game]).write_parquet(sched_path)


def _hash_core_files(lake: LakeStore) -> dict[Path, str]:
    return {path: compute_file_sha256(path) for path in sorted(lake.core_dir.rglob("*.parquet"))}


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
        "player_game": 44,  # 12 stat lines and 32 linemen who only have snaps
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
    oak_game = team_game.filter((pl.col("game_id") == GAME_1) & (pl.col("team") == "LV"))
    kc_game = team_game.filter((pl.col("game_id") == GAME_1) & (pl.col("team") == "KC"))
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
    buf_qb_sb = player_game.filter((pl.col("game_id") == GAME_2) & (pl.col("player_id") == "00-BUFQB"))
    lv_qb_w1 = player_game.filter((pl.col("game_id") == GAME_1) & (pl.col("player_id") == "00-LVQB"))
    assert buf_qb_sb["has_stat_line"].item() is True
    assert buf_qb_sb["offense_snaps"].item() == 60
    assert buf_qb_sb["ngs_pass_attempts"].item() == 30
    assert buf_qb_sb["pfr_pass_drops"].item() == 2
    assert lv_qb_w1["team"].item() == "LV"
    assert lv_qb_w1["ngs_pass_attempts"].item() == 30

    # player_game: a player with snaps and no stat line still has a row, with zeros where a stat line has them
    snaps_only = player_game.filter((pl.col("game_id") == GAME_1) & (pl.col("player_id") == "00-OAKOL1"))
    assert snaps_only["has_stat_line"].item() is False
    assert snaps_only["offense_snaps"].item() == 60
    assert snaps_only["targets"].item() == 0
    assert snaps_only["season_type"].item() == "REG"
    assert snaps_only["team"].item() == "LV"
    assert snaps_only["is_home"].item() is True
    assert snaps_only["position"].item() == "OL"

    # play: blank posteam -> NULL, teams normalized, participation & FTN attached
    plays = pl.read_parquet(lake.core_dir / "play" / "play_2025.parquet").sort("game_id", "play_id")
    play_one = plays.filter((pl.col("game_id") == GAME_1) & (pl.col("play_id") == 1.0))
    play_three = plays.filter((pl.col("game_id") == GAME_1) & (pl.col("play_id") == 3.0))
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
    assert injury_only["injury_report_modified_utc"].item() == datetime(2025, 9, 5, 21, 30, tzinfo=UTC)

    # depth_chart_snapshot: null/empty gsis_id rescued via players.espn_id
    depth_frame = pl.read_parquet(lake.core_dir / "depth_chart_snapshot.parquet")
    rescued = depth_frame.filter(pl.col("espn_id") == "6001")
    assert rescued["gsis_id"].item() == "00-RESCUE"
    assert rescued["team"].item() == "LV"

    assert verify_core_tables(lake, deep=True) == (7, [])


def test_dry_run_reports_what_a_build_would_change_and_publishes_nothing(lake: LakeStore, monkeypatch):
    _populate_valid_clean_lake(lake)
    monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root_dir))
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    main(["build-core", "--dry-run"])
    assert not lake.core_dir.exists() and lake.load_core_state() is None and not lake.staging_dir.exists()

    build_core_tables(lake)
    published = _hash_core_files(lake)
    # Upstream corrects both stadium names and adds a game to the schedule.
    _rewrite_clean(lake, "schedules", "games.parquet", pl.lit("Renamed Stadium").alias("stadium"))
    _add_unplayed_game(lake, "2025_23_LAR_BUF")

    summary = build_core_tables(lake, dry_run=True)

    assert summary.changes is not None
    changes = {change.table_name: change for change in summary.changes}
    game_change = changes.pop("game")
    assert (game_change.added_rows, game_change.removed_rows, game_change.changed_rows) == (1, 0, 2)
    assert game_change.changed_columns == {"stadium": 2}
    assert all(change.unchanged for change in changes.values())
    assert not summary.published and _hash_core_files(lake) == published


def test_build_core_blocks_publish_when_a_strategy_1_check_fails(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)
    published = _hash_core_files(lake)

    # Break the player vs team identity: 50 more passing yards for one player, none for the team.
    _rewrite_clean(
        lake,
        "stats_player_week",
        "stats_player_week_2025.parquet",
        pl.when(pl.col("player_id") == "00-LVQB")
        .then(pl.col("passing_yards") + 50)
        .otherwise(pl.col("passing_yards"))
        .alias("passing_yards"),
    )

    with pytest.raises(CoreBuildFailed) as exc_info:
        build_core_tables(lake)

    assert exc_info.value.report is not None
    failed_names = {outcome.name for outcome in exc_info.value.report.errors}
    assert "core_player_sums_equal_team" in failed_names
    assert _hash_core_files(lake) == published  # the previous core layer is untouched
    assert not lake.staging_dir.exists()


def test_build_core_blocks_duplicate_pfr_bridge_id(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    _rewrite_clean(
        lake,
        "players",
        "players.parquet",
        pl.when(pl.col("gsis_id") == "00-LVRB").then(pl.lit("LvQb00")).otherwise(pl.col("pfr_id")).alias("pfr_id"),
    )

    with pytest.raises(CoreBuildFailed) as exc_info:
        build_core_tables(lake)

    assert exc_info.value.report is not None
    failed_names = {outcome.name for outcome in exc_info.value.report.errors}
    assert "core_players_bridge_ids_unique" in failed_names
    assert not lake.core_dir.exists()


def test_pinned_anchor_game_without_its_pinned_player_row_blocks_the_build(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    # Super Bowl LVIII is pinned to Patrick Mahomes's stat line. Here it is on the schedule as a game not yet
    # played, so the missing pinned row is the only fault.
    _add_unplayed_game(lake, "2023_22_SF_KC")

    with pytest.raises(CoreBuildFailed) as exc_info:
        build_core_tables(lake)

    assert exc_info.value.report is not None
    assert {outcome.name for outcome in exc_info.value.report.errors} == {"core_anchor_games_exact"}


def test_clean_file_that_core_cannot_read_fails_the_build_without_publishing(lake: LakeStore, monkeypatch):
    _populate_valid_clean_lake(lake)
    monkeypatch.setenv("NFL_LAKE_ROOT", str(lake.root_dir))
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    # As after `update --accept-schema-change` lets through a file that lost a column core reads.
    ngs_path = lake.clean_file_path("ngs_passing", "ngs_passing.parquet")
    pl.read_parquet(ngs_path).drop("avg_air_distance").write_parquet(ngs_path)

    with pytest.raises(SystemExit) as exit_info:
        main(["build-core"])

    assert exit_info.value.code == 1
    assert not lake.core_dir.exists()


@pytest.mark.skipif(os.name != "nt", reason="only Windows refuses to move a folder that holds an open file")
def test_core_file_open_in_another_program_fails_the_build_and_leaves_core_as_it_was(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)
    published = _hash_core_files(lake)
    _rewrite_clean(lake, "schedules", "games.parquet", pl.lit("Renamed Stadium").alias("stadium"))  # changes game

    with open(lake.core_dir / "player.parquet", "rb"), pytest.raises(CoreBuildFailed):
        build_core_tables(lake)

    assert _hash_core_files(lake) == published  # not even the tables published before player were replaced


def _change_a_value_no_check_reads(lake: LakeStore) -> None:
    game_path = lake.core_dir / "game.parquet"
    pl.read_parquet(game_path).with_columns(pl.lit("Tampered Stadium").alias("stadium")).write_parquet(game_path)


def _drop_rows(lake: LakeStore) -> None:
    player_path = lake.core_dir / "player.parquet"
    pl.read_parquet(player_path).head(10).write_parquet(player_path)


def _delete_a_table(lake: LakeStore) -> None:
    (lake.core_dir / "team_game.parquet").unlink()


@pytest.mark.parametrize(
    "damage, quick_verify_notices",
    [(_change_a_value_no_check_reads, False), (_drop_rows, True), (_delete_a_table, True)],
    ids=["changed value: only --deep", "lost rows", "missing file"],
)
def test_verify_core_reports_a_damaged_core_file(lake: LakeStore, damage, quick_verify_notices):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)

    damage(lake)

    _, quick_errors = verify_core_tables(lake)
    _, deep_errors = verify_core_tables(lake, deep=True)
    assert bool(quick_errors) is quick_verify_notices
    assert deep_errors


def test_verify_core_detects_stray_play_parquet(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)

    # An empty stray file does not change row count, so verify must still flag it as untracked.
    stray_path = lake.core_dir / "play" / "play_1998.parquet"
    pl.read_parquet(lake.core_dir / "play" / "play_2025.parquet").clear().write_parquet(stray_path)

    _, errors = verify_core_tables(lake, deep=False)
    assert errors


def test_verify_core_reports_core_built_from_an_earlier_clean_layer(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)
    # An upstream correction to a value no check compares: core is intact and every check would pass, but
    # core/game no longer says what clean/schedules says.
    _rewrite_clean(lake, "schedules", "games.parquet", pl.lit("Renamed Stadium").alias("stadium"))

    _, errors = verify_core_tables(lake)
    assert errors

    build_core_tables(lake)
    assert verify_core_tables(lake) == (7, [])


def test_verify_core_reports_core_files_that_have_no_record(lake: LakeStore):
    _populate_valid_clean_lake(lake)
    build_core_tables(lake)
    (lake.manifest_dir / "core_state.json").unlink()

    _, errors = verify_core_tables(lake)
    assert errors


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
    assert lake.load_core_state() is not None

    # A schedule score the team stats do not add up to blocks the build, and the command exits 1.
    _rewrite_clean(lake, "schedules", "games.parquet", pl.lit(99).alias("home_score"))

    capsys.readouterr()
    with pytest.raises(SystemExit) as exit_info:
        main(["build-core"])
    assert exit_info.value.code == 1


def test_core_state_in_the_form_an_earlier_build_core_wrote_is_refused(lake: LakeStore):
    lake.manifest_dir.mkdir(parents=True, exist_ok=True)
    (lake.manifest_dir / "core_state.json").write_text(
        '{"game": {"rows": 2, "files": {"core/game.parquet": "abc"}, "built_at": "2026-11-24T18:17:54Z"}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        lake.load_core_state()
