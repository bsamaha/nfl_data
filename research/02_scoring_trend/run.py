from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 2 — Are team/game scores trending up over time?

Approach:
- Use `pbp` Silver to derive final game scores per `game_id` (REG only).
- Aggregate to season-level averages: average home, away, and total points per game.
- Output: CSV and HTML line chart of average total points by season.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH games AS (
        SELECT
            game_id,
            season,
            MAX(home_score) AS home_score,
            MAX(away_score) AS away_score
        FROM read_parquet('{pbp_path}')
        WHERE season_type = 'REG'
        GROUP BY 1,2
    )
    SELECT
        season,
        COUNT(*) AS games,
        AVG(home_score) AS avg_home_pts,
        AVG(away_score) AS avg_away_pts,
        AVG(home_score + away_score) AS avg_total_pts
    FROM games
    GROUP BY 1
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average total points per game by season (REG)",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "avg_total_pts", "type": "quantitative", "title": "Avg total points"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "games", "type": "quantitative"},
                {"field": "avg_total_pts", "type": "quantitative", "format": ".2f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


