from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 6 — How has red-zone play mix and TD distribution changed?

Approach:
- Use `pbp` Silver (REG) and filter to red zone (yardline_100 <= 20).
- Compute per-season share of pass vs rush plays in red zone and share of pass TD vs rush TD.
- Output: CSV and HTML line chart (two panels) of red-zone pass share and TD pass share.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH rz AS (
        SELECT season,
               COALESCE(pass_attempt,0)=1 AS is_pass,
               (COALESCE(rush_attempt,0)=1 OR COALESCE(pass_attempt,0)=1 OR COALESCE(sack,0)=1) AS is_play,
               COALESCE(pass_touchdown,0)=1 AS pass_td,
               COALESCE(rush_touchdown,0)=1 AS rush_td
        FROM read_parquet('{pbp_path}')
        WHERE season_type='REG' AND yardline_100 IS NOT NULL AND yardline_100 <= 20
    ), play_mix AS (
        SELECT season,
               SUM(CASE WHEN is_pass AND is_play THEN 1 ELSE 0 END)::DOUBLE / NULLIF(SUM(CASE WHEN is_play THEN 1 ELSE 0 END),0) AS rz_pass_share
        FROM rz GROUP BY 1
    ), td_mix AS (
        SELECT season,
               SUM(CASE WHEN pass_td THEN 1 ELSE 0 END)::DOUBLE / NULLIF(SUM(CASE WHEN pass_td OR rush_td THEN 1 ELSE 0 END),0) AS rz_td_pass_share
        FROM rz GROUP BY 1
    )
    SELECT p.season, p.rz_pass_share, t.rz_td_pass_share
    FROM play_mix p
    LEFT JOIN td_mix t USING (season)
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    values = []
    for _, r in df.iterrows():
        values.append({"season": int(r["season"]), "metric": "RZ pass share (plays)", "val": float(r["rz_pass_share"]) if r["rz_pass_share"] is not None else None})
        values.append({"season": int(r["season"]), "metric": "RZ pass share (TDs)", "val": float(r["rz_td_pass_share"]) if r["rz_td_pass_share"] is not None else None})

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Red-zone pass share (plays and TDs)",
        "data": {"values": values},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "val", "type": "quantitative", "title": "Share"},
            "color": {"field": "metric", "type": "nominal"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "metric", "type": "nominal"},
                {"field": "val", "type": "quantitative", "format": ".1%"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


