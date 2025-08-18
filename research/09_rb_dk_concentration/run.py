from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 9 — Is RB fantasy scoring more concentrated (top-heavy) today?

Approach:
- Use `weekly` Silver to compute season DK totals for RBs.
- For each season, compute share of DK points by top 12 RBs vs all RBs.
- Output: CSV and HTML line chart of top-12 share over time.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe, dk_weekly_points_sql_expr


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    weekly_path = parquet_glob("weekly", "season=*")
    dk_expr = dk_weekly_points_sql_expr("dk_points")

    sql = f"""
    WITH w AS (
        SELECT season, player_id, position, {dk_expr}
        FROM read_parquet('{weekly_path}')
        WHERE position='RB'
    ), totals AS (
        SELECT season, player_id, SUM(dk_points) AS season_dk
        FROM w GROUP BY 1,2
    ), ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY season ORDER BY season_dk DESC) AS rnk
        FROM totals
    ), agg AS (
        SELECT season,
               SUM(season_dk) AS dk_all,
               SUM(CASE WHEN rnk<=12 THEN season_dk ELSE 0 END) AS dk_top12
        FROM ranked
        GROUP BY 1
    )
    SELECT season, dk_top12::DOUBLE / NULLIF(dk_all,0) AS top12_share
    FROM agg
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "RB top-12 share of DK points by season",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "top12_share", "type": "quantitative", "title": "Top-12 share"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "top12_share", "type": "quantitative", "format": ".1%"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


