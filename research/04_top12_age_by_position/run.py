from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 4 — What are the age ranges for the top-12 fantasy scorers at each position by season?

Approach:
- Use `weekly` Silver to compute season-long DK points per player and position.
- Rank by DK points within each season and position, take top 12.
- Join to `rosters_seasonal` (seasonal roster) to get age if available; otherwise estimate from `birth_date` in players if present.
- Output: CSV and HTML boxplot of age distribution by position.

Notes:
- Age calculation: prefer `rosters_seasonal.age` if present; else players birth_date converted to years at mid-season (Nov 1).
"""

from datetime import date
from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe, dk_weekly_points_sql_expr


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    weekly_path = parquet_glob("weekly", "season=*")
    rost_seasonal_path = parquet_glob("rosters_seasonal", "season=*")
    players_path = parquet_glob("players")
    dk_expr = dk_weekly_points_sql_expr("dk_points")

    # Compute season DK totals
    sql = f"""
    WITH w AS (
        SELECT season, player_id, position, team, {dk_expr}
        FROM read_parquet('{weekly_path}')
        WHERE season IS NOT NULL AND position IN ('QB','RB','WR','TE')
    ), totals AS (
        SELECT season, player_id, position, SUM(dk_points) AS season_dk
        FROM w GROUP BY 1,2,3
    ), ranks AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY season, position ORDER BY season_dk DESC) AS rnk
        FROM totals
    ), top12 AS (
        SELECT season, position, player_id, season_dk
        FROM ranks WHERE rnk <= 12
    ), roster AS (
        SELECT season, player_id, CAST(age AS DOUBLE) AS age
        FROM read_parquet('{rost_seasonal_path}')
    ), players AS (
        SELECT gsis_id AS player_id,
               TRY_CAST(birth_date AS DATE) AS birth_date
        FROM read_parquet('{players_path}')
    ), ages AS (
        SELECT t.season, t.position, t.player_id, t.season_dk,
               COALESCE(r.age,
                        DATE_DIFF('year', p.birth_date, MAKE_DATE(t.season, 11, 1))) AS age
        FROM top12 t
        LEFT JOIN roster r USING (season, player_id)
        LEFT JOIN players p USING (player_id)
    )
    SELECT season, position, player_id, season_dk, age
    FROM ages
    WHERE age IS NOT NULL
    ORDER BY season, position, season_dk DESC;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    # Vega-Lite boxplot by position (all seasons pooled) and small multiples over seasons
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Age distribution of top-12 DK scorers by position",
        "data": {"values": df.to_dict(orient="records")},
        "mark": {"type": "boxplot"},
        "encoding": {
            "x": {"field": "position", "type": "nominal"},
            "y": {"field": "age", "type": "quantitative", "title": "Age"},
            "color": {"field": "position", "type": "nominal"},
            "tooltip": [
                {"field": "position"},
                {"field": "age", "type": "quantitative"},
                {"field": "season", "type": "quantitative"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


