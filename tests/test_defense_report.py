from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.lib import data as report_data  # noqa: E402

REPORTS_BASE_ATTR = getattr(report_data, "REPORTS_BASE", "data/gold/reports")


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def _run_defense_materialization(sql_path: Path, weekly_dir: Path, schedules_dir: Path, output_dir: Path) -> None:
    query = sql_path.read_text()
    query = query.replace("data/silver/weekly", weekly_dir.parent.parent.as_posix())
    query = query.replace("data/silver/schedules", schedules_dir.parent.as_posix())
    query = query.replace("data/gold/reports/defense_position_points_allowed", output_dir.as_posix())

    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(query)
    finally:
        con.close()


def test_materialize_defense_position_points_allowed_aggregates_expected_metrics(tmp_path: Path):
    silver_root = tmp_path / "data" / "silver"
    weekly_dir = silver_root / "weekly" / "season=2025" / "week=1"
    schedules_dir = silver_root / "schedules" / "season=2025"

    weekly_df = pd.DataFrame(
        {
            "season": [2025, 2025, 2025, 2025],
            "week": [1, 1, 1, 1],
            "season_type": ["REG", "REG", "REG", "REG"],
            "opponent_team": ["NYJ", "NYJ", "NYJ", "DAL"],
            "position": ["WR", "WR", "RB", "WR"],
            "fantasy_points_ppr": [15.0, 10.0, 12.0, 5.0],
        }
    )
    _write_parquet(weekly_df, weekly_dir / "weekly.parquet")

    schedules_df = pd.DataFrame(
        {
            "season": [2025, 2025],
            "week": [1, 1],
            "game_type": ["REG", "REG"],
            "home_team": ["NYJ", "DAL"],
            "away_team": ["BUF", "PHI"],
        }
    )
    _write_parquet(schedules_df, schedules_dir / "schedules.parquet")

    sql_path = Path("queries/reports/materialize_defense_position_points_allowed.sql")
    _run_defense_materialization(sql_path, weekly_dir, schedules_dir, tmp_path / "data" / "gold" / "reports" / "defense_position_points_allowed")

    result_path = tmp_path / "data" / "gold" / "reports" / "defense_position_points_allowed"
    result = pl.read_parquet(result_path.as_posix() + "/**/*.parquet")

    nyj_wr = result.filter((pl.col("defense_team") == "NYJ") & (pl.col("position") == "WR"))
    assert nyj_wr["points_allowed_ppr"].item() == pytest.approx(25.0)
    assert nyj_wr["games_played"].item() == 1
    assert nyj_wr["avg_points_allowed"].item() == pytest.approx(25.0)

    wr_rows = result.filter(pl.col("position") == "WR")
    league_avg_wr = wr_rows["avg_points_allowed"].mean()
    assert nyj_wr["league_avg_points_allowed"].item() == pytest.approx(league_avg_wr)

    dal_rb = result.filter((pl.col("defense_team") == "DAL") & (pl.col("position") == "RB"))
    assert dal_rb["points_allowed_ppr"].item() == pytest.approx(0.0)
    rb_rows = result.filter(pl.col("position") == "RB")
    league_avg_rb = rb_rows["avg_points_allowed"].mean()
    assert dal_rb["league_avg_points_allowed"].item() == pytest.approx(league_avg_rb)


def test_load_defense_position_points_respects_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    gold_root = tmp_path / "data" / "gold" / "reports"
    target_dir = gold_root / "defense_position_points_allowed" / "season=2025" / "week=1"

    df = pd.DataFrame(
        {
            "season": [2025, 2025],
            "week": [1, 1],
            "season_type": ["REG", "REG"],
            "defense_team": ["NYJ", "DAL"],
            "position": ["WR", "WR"],
            "points_allowed_ppr": [25.0, 5.0],
            "games_played": [1, 1],
            "total_points_allowed": [25.0, 5.0],
            "avg_points_allowed": [25.0, 5.0],
            "league_avg_points_allowed": [15.0, 15.0],
            "avg_vs_league": [10.0, -10.0],
            "league_std_points_allowed": [10.0, 10.0],
        }
    )
    _write_parquet(df, target_dir / "part.parquet")

    monkeypatch.setattr(report_data, "REPORTS_BASE", gold_root.as_posix(), raising=False)

    loader = getattr(report_data, "load_defense_position_points", None)
    if loader is None:
        pytest.skip("load_defense_position_points not available in app.lib.data")

    report_data.list_defenses.cache_clear()
    report_data.list_positions.cache_clear()
    loader.cache_clear()

    result = loader(seasons=[2025], weeks=[1], positions=["WR"], defenses=["NYJ"])

    assert result.shape[0] == 1
    row = result.iloc[0]
    assert row["defense_team"] == "NYJ"
    assert row["position"] == "WR"
    assert row["avg_points_allowed"] == pytest.approx(25.0)


