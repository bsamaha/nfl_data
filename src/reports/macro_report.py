from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd


RESEARCH_DIR = Path(__file__).resolve().parents[2] / "research"
QUERIES_DIR = Path(__file__).resolve().parents[2] / "queries"


def ensure_research_dir() -> None:
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)


def run_sql(file_rel: str, duckdb_flags: str | None = None) -> pd.DataFrame:
    sql_path = QUERIES_DIR / file_rel
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    # DuckDB executes SQL directly; we rely on default params embedded in the SQL files
    con = duckdb.connect()
    try:
        df = con.execute(sql).df()
        return df
    finally:
        con.close()


def save_df(df: pd.DataFrame, name: str) -> None:
    # Save both Parquet and CSV for convenience
    pq_path = RESEARCH_DIR / f"{name}.parquet"
    csv_path = RESEARCH_DIR / f"{name}.csv"
    df.to_parquet(pq_path, index=False)
    df.to_csv(csv_path, index=False)


def main() -> None:
    ensure_research_dir()

    # 1) League weekly metrics
    league_week = run_sql("league/league_aggregates_by_week.sql")
    save_df(league_week, "league_week_metrics")

    # 2) League YoY season aggregates
    league_yoy = run_sql("league/league_aggregates_yoy.sql")
    save_df(league_yoy, "league_yoy_metrics")

    # 3) Efficiency trends (weekly epa splits)
    eff_trends = run_sql("league/league_efficiency_trends.sql")
    save_df(eff_trends, "league_efficiency_trends")

    # 3b) Roof splits (indoor/outdoor/retractable)
    roof_splits = run_sql("league/league_aggregates_by_roof.sql")
    save_df(roof_splits, "league_by_roof")

    # 3c) Team × roof game counts
    team_roof_counts = run_sql("league/team_roof_game_counts.sql")
    save_df(team_roof_counts, "team_roof_counts")

    # 3d) Team × week × roof game counts (for playoff-week filtering)
    try:
        team_roof_by_week = run_sql("league/team_roof_game_counts_by_week.sql")
        save_df(team_roof_by_week, "team_roof_counts_by_week")
    except Exception as e:
        print("Skipping team_roof_counts_by_week:", e)

    # 4) Position tier shares
    pos_tiers = run_sql("weekly/pos_tier_shares.sql")
    save_df(pos_tiers, "pos_tier_shares")

    # 5) FLEX tier shares
    flex_tiers = run_sql("weekly/flex_tier_shares.sql")
    save_df(flex_tiers, "flex_tier_shares")

    # 6) Calendar effects
    try:
      cal = run_sql("calendar/calendar_effects.sql")
      save_df(cal, "calendar_effects")
    except Exception as e:
      # Calendar analysis requires date fields across seasons; skip if schema variance
      print("Skipping calendar_effects:", e)

    print("Report tables written to:", RESEARCH_DIR)


if __name__ == "__main__":
    main()


