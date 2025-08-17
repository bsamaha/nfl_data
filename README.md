# Local NFL Data Lake

Parquet-based lake for NFL analytics with Bronze/Silver layers, a Typer CLI, partition-aware incremental promotes, and DuckDB for querying.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1) Bootstrap history (idempotent)
python -m src.cli bootstrap --years 1999-2024

# 2) Promote any existing bronze to silver (partition-scoped)
python -m src.cli promote --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts

# 3) Profile silver quality by partition
python -m src.cli profile --layer silver --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts

# 4) In-season update (when upstream publishes 2025 data)
python -m src.cli update --season 2025

# Query examples
duckdb -c "SELECT season, COUNT(*) FROM read_parquet('data/silver/weekly/season=*/**/*.parquet') GROUP BY season ORDER BY season"
```

## What’s inside

- Architecture and plan: `plan.md`
- Technical overview: `docs/TECHNICAL_OVERVIEW.md`
- Dataset catalog/config: `catalog/datasets.yml`
- Lineage and quality outputs: `catalog/lineage.json`, `catalog/quality/`

## CLI commands

```bash
# See available commands
python -m src.cli --help

# Historical backfill (select datasets)
python -m src.cli bootstrap --years 2012-2024 --datasets injuries,depth_charts,snap_counts

# Promote existing bronze → silver (no fetch)
python -m src.cli promote --datasets weekly,schedules

# Profile partition metrics
python -m src.cli profile --layer silver --datasets weekly
```

## Scheduling (cron examples)

```bash
# 03:30 in-season nightly
30 3 * 9-2 * cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli update --season 2025 | ts | tee -a logs/cron_update.log
# Thu corrections re-pull
0 6 * 9-2 4 cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli recache-pbp --season 2025 | ts | tee -a logs/cron_recache.log
# Schedules daily
5 4 * * * cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli update --season 2025 --datasets schedules | ts | tee -a logs/cron_schedules.log
```

## Notes

- Weekly 2025 may 404 until upstream publishes; re-run `update` later or use `promote` on existing Bronze.
- Optional datasets are scaffolded (`officials`, `win_totals`, `scoring_lines`, `draft_picks`, `combine`); enable in `catalog/datasets.yml` when needed.

