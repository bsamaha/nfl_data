from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 11 — Does wind speed correlate with lower totals?

Approach:
- Use `pbp` Silver (REG) to extract per-game max wind speed and final totals.
- Group by wind bins to compute average total points.
- Output: CSV and HTML bar chart of avg totals by wind bin.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH games AS (
        SELECT game_id, season,
               MAX(home_score) AS home_score,
               MAX(away_score) AS away_score,
               MAX(TRY_CAST(wind AS DOUBLE)) AS wind
        FROM read_parquet('{pbp_path}')
        WHERE season_type='REG'
        GROUP BY 1,2
    ), bins AS (
        SELECT *,
               CASE
                 WHEN wind IS NULL THEN 'unknown'
                 WHEN wind < 5 THEN '0-4'
                 WHEN wind < 10 THEN '5-9'
                 WHEN wind < 15 THEN '10-14'
                 WHEN wind < 20 THEN '15-19'
                 ELSE '20+'
               END AS wind_bin
        FROM games
    )
    SELECT wind_bin, COUNT(*) AS games, AVG(home_score + away_score) AS avg_total
    FROM bins
    GROUP BY 1
    ORDER BY CASE wind_bin WHEN '0-4' THEN 1 WHEN '5-9' THEN 2 WHEN '10-14' THEN 3 WHEN '15-19' THEN 4 WHEN '20+' THEN 5 ELSE 6 END;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average totals by wind speed bin (REG)",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "bar",
        "encoding": {
            "x": {"field": "wind_bin", "type": "nominal", "title": "Wind (mph)"},
            "y": {"field": "avg_total", "type": "quantitative", "title": "Avg total points"},
            "tooltip": [
                {"field": "wind_bin"},
                {"field": "games", "type": "quantitative"},
                {"field": "avg_total", "type": "quantitative", "format": ".2f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


