from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 1 — Do games in indoor stadiums score more points?

Approach:
- Use `pbp` (play-by-play) Silver to derive final game scores per `game_id`.
- Classify games by roof type (indoor/retractable/outdoor/unknown) from `pbp.roof`.
- Compute average total points per game by roof group, REG season only.
- Output: CSV and an HTML bar chart.
"""

from pathlib import Path

from research._common import (
    parquet_glob,
    run_sql,
    save_chart_html,
    save_dataframe,
    simplify_roof_sql,
)


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")
    roof_case = simplify_roof_sql("roof", "roof_group")

    sql = f"""
    WITH games AS (
        SELECT
            game_id,
            season,
            {roof_case},
            MAX(home_score) AS home_score,
            MAX(away_score) AS away_score
        FROM read_parquet('{pbp_path}')
        WHERE season_type = 'REG'
        GROUP BY 1,2,3
    )
    SELECT
        roof_group,
        COUNT(*) AS games,
        AVG(home_score + away_score) AS avg_total_points
    FROM games
    GROUP BY 1
    ORDER BY avg_total_points DESC;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    # Build a Vega-Lite bar chart spec
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average total points by roof type (REG only)",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "bar",
        "encoding": {
            "x": {"field": "roof_group", "type": "nominal", "sort": "-y", "title": "Roof type"},
            "y": {"field": "avg_total_points", "type": "quantitative", "title": "Avg total points"},
            "tooltip": [
                {"field": "roof_group", "type": "nominal"},
                {"field": "games", "type": "quantitative"},
                {"field": "avg_total_points", "type": "quantitative", "format": ".2f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


