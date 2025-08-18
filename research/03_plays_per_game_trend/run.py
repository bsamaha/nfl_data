from __future__ import annotations
# Ensure the repository root is on sys.path when running as a script
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

"""
Fact 3 — Are teams running more plays per game today than they used to?

Approach:
- Use `pbp` Silver (REG) to count offensive plays per game.
- Define offensive plays as rush_attempt=1 OR pass_attempt=1 OR sack=1.
- Aggregate to per-season average plays per game and per-team plays per game.
- Output: CSV and HTML line chart of avg plays per game by season.
"""

from pathlib import Path

from research._common import parquet_glob, run_sql, save_chart_html, save_dataframe


def main() -> None:
    out_dir = Path(__file__).parent / "out"
    pbp_path = parquet_glob("pbp", "year=*")

    sql = f"""
    WITH plays AS (
        SELECT game_id, season,
               (COALESCE(rush_attempt,0)=1 OR COALESCE(pass_attempt,0)=1 OR COALESCE(sack,0)=1) AS is_play
        FROM read_parquet('{pbp_path}')
        WHERE season_type='REG'
    ), per_game AS (
        SELECT season, game_id, SUM(CASE WHEN is_play THEN 1 ELSE 0 END) AS plays
        FROM plays
        GROUP BY season, game_id
    )
    SELECT season, COUNT(*) AS games, AVG(plays) AS avg_plays_per_game
    FROM per_game
    GROUP BY 1
    ORDER BY season;
    """

    df = run_sql(sql)
    save_dataframe(df, out_dir)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Average offensive plays per game by season (REG)",
        "data": {"values": df.to_dict(orient="records")},
        "mark": "line",
        "encoding": {
            "x": {"field": "season", "type": "quantitative"},
            "y": {"field": "avg_plays_per_game", "type": "quantitative", "title": "Avg plays/game"},
            "tooltip": [
                {"field": "season", "type": "quantitative"},
                {"field": "avg_plays_per_game", "type": "quantitative", "format": ".1f"}
            ]
        }
    }
    save_chart_html(spec, out_dir)


if __name__ == "__main__":
    main()


