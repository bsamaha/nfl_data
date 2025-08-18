from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 7 — How has YAC share of passing yardage trended?

Approach:
- Use `pbp` Silver (REG) passing plays with positive yards.
- Sum air_yards and yards_after_catch where available; compute YAC share = sum_yac / (sum_air + sum_yac).
- Aggregate by season.
- Output: CSV and HTML line chart.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH pass_plays AS (
        SELECT season,
               NULLIF(air_yards, 0) AS air_yards,
               NULLIF(yards_after_catch, 0) AS yac
        FROM read_parquet('{pbp_path}')
        WHERE season_type='REG' AND COALESCE(pass_attempt,0)=1 AND yards_gained IS NOT NULL AND yards_gained > 0
    ), agg AS (
        SELECT season,
               SUM(COALESCE(air_yards,0)) AS air_sum,
               SUM(COALESCE(yac,0)) AS yac_sum
        FROM pass_plays
        GROUP BY 1
    )
    SELECT season,
           yac_sum::DOUBLE / NULLIF(air_sum + yac_sum, 0) AS yac_share
    FROM agg
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "YAC share of passing yardage by season (REG)",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "yac_share", "type": "quantitative", "title": "YAC share"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "yac_share", "type": "quantitative", "format": ".1%"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


