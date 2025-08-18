from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 10 — How has QB rushing contribution changed?

Approach:
- Use `weekly` Silver to compute per-season QB rushing share of team rushing yards and TDs.
- Aggregate by season: sum QB rushing yards / total rushing yards; sum QB rush TD / total rush TD.
- Output: CSV and HTML line chart with two series (yards share, TD share).
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    weekly_path = parquet_glob("weekly", "season=*")

    sql = f"""
    WITH base AS (
        SELECT season,
               position,
               COALESCE(rushing_yards, rush_yds, 0) AS rush_yds,
               COALESCE(rushing_tds, rush_tds, 0) AS rush_tds
        FROM read_parquet('{weekly_path}')
        WHERE season IS NOT NULL
    ), agg AS (
        SELECT season,
               SUM(CASE WHEN position='QB' THEN rush_yds ELSE 0 END) AS qb_yds,
               SUM(CASE WHEN position='QB' THEN rush_tds ELSE 0 END) AS qb_tds,
               SUM(rush_yds) AS all_yds,
               SUM(rush_tds) AS all_tds
        FROM base
        GROUP BY season
    )
    SELECT season,
           qb_yds::DOUBLE / NULLIF(all_yds,0) AS qb_rush_yards_share,
           qb_tds::DOUBLE / NULLIF(all_tds,0) AS qb_rush_td_share
    FROM agg
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    values = []
    for _, r in df.iterrows():
        values.append({"season": int(r["season"]), "metric": "Yards share", "share": float(r["qb_rush_yards_share"]) if r["qb_rush_yards_share"] is not None else None})
        values.append({"season": int(r["season"]), "metric": "TD share", "share": float(r["qb_rush_td_share"]) if r["qb_rush_td_share"] is not None else None})

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "QB rushing contribution over time",
        "data": {"values": values},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "share", "type": "quantitative", "title": "Share"},
            "color": {"field": "metric", "type": "nominal"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "metric", "type": "nominal"},
                {"field": "share", "type": "quantitative", "format": ".1%"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


