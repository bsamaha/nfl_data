from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 5 — How have pass rates changed over time? (overall and neutral situations)

Approach:
- Use `pbp` Silver (REG) to compute per-season pass rate:
  pass_attempt=1 over offensive plays (pass_attempt OR rush_attempt OR sack).
- Neutral situation: score differential between -7 and +7, and between 2:00 and 12:00 of quarters 1-3.
- Output: CSV and HTML line chart for overall vs neutral pass rate by season.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH base AS (
        SELECT season,
               COALESCE(pass_attempt,0)=1 AS is_pass,
               (COALESCE(rush_attempt,0)=1 OR COALESCE(pass_attempt,0)=1 OR COALESCE(sack,0)=1) AS is_play,
               score_differential AS sd,
               qtr,
               quarter_seconds_remaining AS qsr
        FROM read_parquet('{pbp_path}')
        WHERE season_type='REG'
    ), overall AS (
        SELECT season,
               SUM(CASE WHEN is_pass AND is_play THEN 1 ELSE 0 END)::DOUBLE / NULLIF(SUM(CASE WHEN is_play THEN 1 ELSE 0 END),0) AS pass_rate
        FROM base
        GROUP BY 1
    ), neutral AS (
        SELECT season,
               SUM(CASE WHEN is_pass AND is_play AND sd BETWEEN -7 AND 7 AND qtr IN (1,2,3) AND qsr BETWEEN 120 AND 720 THEN 1 ELSE 0 END)::DOUBLE
               / NULLIF(SUM(CASE WHEN is_play AND sd BETWEEN -7 AND 7 AND qtr IN (1,2,3) AND qsr BETWEEN 120 AND 720 THEN 1 ELSE 0 END),0) AS pass_rate_neutral
        FROM base
        GROUP BY 1
    )
    SELECT o.season, o.pass_rate, n.pass_rate_neutral
    FROM overall o
    LEFT JOIN neutral n USING (season)
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    # Two series line chart
    values = []
    for _, r in df.iterrows():
        values.append({"season": int(r["season"]), "metric": "overall", "rate": float(r["pass_rate"]) if r["pass_rate"] is not None else None})
        values.append({"season": int(r["season"]), "metric": "neutral", "rate": float(r["pass_rate_neutral"]) if r["pass_rate_neutral"] is not None else None})
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Pass rate trends (overall vs neutral)",
        "data": {"values": values},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "rate", "type": "quantitative", "title": "Pass rate"},
            "color": {"field": "metric", "type": "nominal"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "metric", "type": "nominal"},
                {"field": "rate", "type": "quantitative", "format": ".2%"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


