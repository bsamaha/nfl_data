from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 12 — How have DK points per game by position trended?

Approach:
- Use `weekly` Silver to compute average DK points per player-week for QB, RB, WR, TE by season.
- Output: CSV and HTML line chart.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe, dk_weekly_points_sql_expr


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    weekly_path = parquet_glob("weekly", "season=*")
    dk_expr = dk_weekly_points_sql_expr("dk_points")

    sql = f"""
    WITH w AS (
        SELECT season, position, {dk_expr}
        FROM read_parquet('{weekly_path}')
        WHERE position IN ('QB','RB','WR','TE')
    )
    SELECT season, position, AVG(dk_points) AS avg_dk_per_week
    FROM w
    GROUP BY 1,2
    ORDER BY season, position;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average DK points per player-week by position and season",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "avg_dk_per_week", "type": "quantitative", "title": "Avg DK / week"},
            "color": {"field": "position", "type": "nominal"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "position", "type": "nominal"},
                {"field": "avg_dk_per_week", "type": "quantitative", "format": ".2f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


