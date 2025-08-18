from __future__ import annotations
# pyright: reportMissingImports=false, reportMissingModuleSource=false

import json
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import yaml


def repo_root() -> Path:
    """Return repository root assuming this file lives under <root>/research/._common.py."""
    return Path(__file__).resolve().parents[1]


def load_catalog_root() -> Path:
    """Read catalog/datasets.yml to discover data root (default to 'data')."""
    cat_path = repo_root() / "catalog" / "datasets.yml"
    if not cat_path.exists():
        return repo_root() / "data"
    cfg = yaml.safe_load(cat_path.read_text()) or {}
    root_str = cfg.get("root", "data")
    return (repo_root() / root_str).resolve()


def parquet_glob(dataset: str, partition_glob: Optional[str] = None) -> str:
    """Build a DuckDB-compatible glob to the Silver parquet files for a dataset.

    - For partitioned datasets, pass partition_glob like 'season=*' or 'year=*'.
    - For non-partitioned datasets, leave partition_glob None.
    """
    root = load_catalog_root()
    if partition_glob:
        return str((root / "silver" / dataset / partition_glob / "**" / "*.parquet").as_posix())
    return str((root / "silver" / dataset / "*.parquet").as_posix())


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    """Create a short-lived DuckDB connection (no persistent db on disk)."""
    return duckdb.connect()


def run_sql(sql: str) -> pd.DataFrame:
    """Run SQL against DuckDB and return a pandas DataFrame."""
    con = connect_duckdb()
    try:
        return con.sql(sql).df()
    finally:
        con.close()


def save_dataframe(df: pd.DataFrame, out_dir: Path, name: str = "data.csv") -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    df.to_csv(path, index=False)
    return path


def save_chart_html(chart_spec: dict, out_dir: Path, name: str = "chart.html") -> Path:
    """Save a Vega-Lite spec as a standalone HTML file using CDN scripts.

    This avoids extra dependencies like altair_saver; we embed the spec and
    load vega/vega-lite/vega-embed from jsdelivr.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    spec_json = json.dumps(chart_spec, separators=(",", ":"))
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Chart</title>
  <script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>
  <script src=\"https://cdn.jsdelivr.net/npm/vega-lite@5\"></script>
  <script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>
  <style>body{{margin:0;padding:0;font-family:sans-serif}}#vis{{width:100%;height:100vh}}</style>
  </head>
<body>
  <div id=\"vis\"></div>
  <script>
    const spec = {spec_json};
    vegaEmbed('#vis', spec, {{renderer: 'canvas', actions: false}});
  </script>
</body>
</html>
"""
    path.write_text(html)
    return path


def dk_weekly_points_sql_expr(alias: str = "dk_points") -> str:
    """Return a SQL expression that computes DraftKings points from a weekly row.

    Uses COALESCE across common nflverse weekly column variants.
    Scoring source: catalog/draftkings/bestball.yml (PPR, std DK bonuses).
    """
    # Column candidates
    pass_yds = "COALESCE(passing_yards, pass_yds, 0)"
    pass_tds = "COALESCE(passing_tds, pass_tds, 0)"
    pass_int = "COALESCE(interceptions, pass_int, 0)"
    rush_yds = "COALESCE(rushing_yards, rush_yds, 0)"
    rush_tds = "COALESCE(rushing_tds, rush_tds, 0)"
    rec_yds = "COALESCE(receiving_yards, rec_yds, 0)"
    rec_tds = "COALESCE(receiving_tds, rec_tds, 0)"
    recs = "COALESCE(receptions, rec, 0)"
    fum_lost = "COALESCE(fumbles_lost, 0)"
    two_pt = "COALESCE(two_point_conversions, two_pt_conversions, two_pt, 0)"
    # Bonuses per week
    pass_bonus = f"(CASE WHEN {pass_yds} >= 300 THEN 3 ELSE 0 END)"
    rush_bonus = f"(CASE WHEN {rush_yds} >= 100 THEN 3 ELSE 0 END)"
    rec_bonus = f"(CASE WHEN {rec_yds} >= 100 THEN 3 ELSE 0 END)"
    expr = (
        f"(0.04*{pass_yds} + 4*{pass_tds} - 1*{pass_int} + "
        f"0.1*{rush_yds} + 6*{rush_tds} + "
        f"0.1*{rec_yds} + 6*{rec_tds} + 1*{recs} + "
        f"2*{two_pt} - 1*{fum_lost} + "
        f"{pass_bonus} + {rush_bonus} + {rec_bonus}) AS {alias}"
    )
    return expr


def simplify_roof_sql(col: str = "roof", alias: str = "roof_group") -> str:
    """Simplify schedules roof values into groups."""
    return (
        "CASE LOWER("
        + col
        + ") "
        + "WHEN 'dome' THEN 'indoor' "
        + "WHEN 'closed' THEN 'indoor' "
        + "WHEN 'retractable' THEN 'retractable' "
        + "WHEN 'outdoors' THEN 'outdoor' "
        + "WHEN 'outdoor' THEN 'outdoor' "
        + "WHEN 'open' THEN 'outdoor' "
        + "ELSE 'unknown' END AS "
        + alias
    )


