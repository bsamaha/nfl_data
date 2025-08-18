from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 8 — How have receptions per player-week changed by position?

Approach:
- Use `weekly` Silver (REG) with positions WR, RB, TE.
- Compute average receptions per player-week per season and position.
- Output: CSV and HTML line chart (faceted by position).
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    weekly_path = parquet_glob("weekly", "season=*")

    sql = f"""
    WITH base AS (
        SELECT season, position, COALESCE(receptions, rec, 0) AS rec
        FROM read_parquet('{weekly_path}')
        WHERE position IN ('WR','RB','TE')
    )
    SELECT season, position, AVG(rec) AS avg_rec_per_week
    FROM base
    GROUP BY 1,2
    ORDER BY season, position;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average receptions per player-week by position and season",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "avg_rec_per_week", "type": "quantitative", "title": "Avg receptions/week"},
            "color": {"field": "position", "type": "nominal"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "position", "type": "nominal"},
                {"field": "avg_rec_per_week", "type": "quantitative", "format": ".2f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


