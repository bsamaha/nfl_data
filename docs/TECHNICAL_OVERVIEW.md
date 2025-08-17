# Local NFL Data Lake — Technical Overview

## Purpose
Local, reproducible Parquet-based lake for NFL analytics with clear contracts, partition-aware incremental promotes, and first-class CLI ergonomics.

## Storage, Layout, and Formats
- Files: Parquet with ZSTD compression
- Partitions (Hive-style directories):
  - `pbp`: `year`
  - `weekly`, `schedules`, `rosters`, `injuries`, `depth_charts`, `snap_counts`: `season`
  - Optional (disabled by default): `officials`, `win_totals`, `scoring_lines`, `draft_picks`, `combine` (all `season`)
- Root directories (relative to `catalog/datasets.yml: root`, default `data`):
  - `data/bronze/<dataset>/<partition>=<value>/part-*.parquet`
  - `data/silver/<dataset>/<partition>=<value>/part-*.parquet`
  - Staging for atomic writes: `data/silver/_staging/<dataset>/...`

## Layers
- Bronze: as-ingested (typed minimally), append-only, partitioned, metadata stamped
- Silver: deduped, normalized schemas/dtypes, stable keys, optional sort within partition
- Gold (future): denormalized marts fed from Silver

## Dataset Catalog (keys, partitions)
Configured in `catalog/datasets.yml`.

| Dataset | Partitions | Primary keys (Silver) | Enabled |
|---|---|---|---|
| pbp | `year` | `(game_id, play_id)` | yes |
| schedules | `season` | `(game_id)` | yes |
| weekly | `season` | `(season, week, player_id, team)` | yes |
| rosters | `season` | `(season, week, player_id, team)` | yes |
| injuries | `season` | `(season, week, team, player_id)` | yes |
| depth_charts | `season` | `(season, week, team, position, player_id)` | yes |
| snap_counts | `season` | `(season, week, team, player_id)` | yes |
| officials | `season` | `(game_id, official_id)` | no |
| win_totals | `season` | `(season, team)` | no |
| scoring_lines | `season` | `(season, game_id)` | no |
| draft_picks | `season` | `(season, overall, team)` | no |
| combine | `season` | `(season, player_id)` | no |

## CLI Commands
Entry: `python -m src.cli`

- `bootstrap` — historical backfill
  - Args: `--years 1999-2024`, `--datasets pbp,weekly,...`, `--max-workers`, `--no-validate`
- `update` — in-season for a single season
  - Args: `--season 2025`, `--datasets ...`, `--since YYYY-MM-DD`, `--no-validate`
- `recache-pbp` — re-pull current PBP season
- `promote` — promote existing Bronze to Silver (no fetch)
  - Args: `--datasets ...`, `--values 1999,2000` to scope partitions
- `profile` — emit partition metrics to `catalog/quality/<dataset>/`

## Ingestion, Promotion, and Atomicity
Code: `src/importers/`, `src/promote.py`, `src/io.py`.

Flow per dataset:
1) Fetch (per year/season) via `nfl_data_py`, normalize some dtypes (e.g., `season`, `week`, IDs)
2) Bronze write: `pyarrow.dataset.write_dataset(..., partitioning=hive)`
3) Discover changed partitions from the ingested frame
4) Silver promote per changed partition only:
   - Read Bronze partition; read existing Silver partition (if any)
   - Align/union schemas and harmonize dtypes (int/float/string) before concat
   - Apply `to_silver(dataset, df)` transform (dedupe by keys; keep newest `ingested_at`)
   - Write to `_staging`, then atomically move partition dir into `data/silver/<dataset>/<part>`

## Transforms and Schemas
Code: `src/transforms/__init__.py`, `src/schemas/__init__.py`.

Common rules:
- IDs to string, nullable integers via `Int64`, timestamps UTC, upcast all-null columns to Utf8
- Dedupe by keys (prefer latest by `ingested_at` when present)

Dataset specifics:
- weekly: rename `recent_team → team` if needed
- injuries/depth_charts/snap_counts: ensure `player_id` from `gsis_id` if missing; cast `week`
- rosters: normalize mixed numeric columns (e.g., `jersey_number`, `draft_*`) on ingest

Validation:
- Bronze (Pandera minimal checks where applicable)
- Silver:
  - pbp: non-null keys and year
  - schedules: presence of `game_id`
  - weekly/rosters/injuries/depth_charts/snap_counts: required key columns exist

## Lineage and Quality
Code: `src/lineage.py`, outputs in `catalog/lineage.json` and `catalog/quality/`.

- Dataset-level: `last_ingest_utc`, `rows_last_batch`, `changed_partitions`
- Partition-level: `row_count`, key `sha256_fingerprint`, `min/max ingested_at`
- Quality profiles per partition: row counts, dtypes, key nulls/duplicates

## Performance and Reliability
- Writer knobs: `compression=zstd`, `max_rows_per_file`, `row_group_mb` (constrained for Arrow), dictionary encoding (future)
- Parallelism: CLI `--max-workers` (thread pool)
- File lock to prevent overlaps (`.lake.lock`)
- Retries/backoff (tenacity) used in orchestration (can be extended to importers as needed)

## Scheduling
Cron examples (local time): see `plan.md` section 10.
- Nightly 03:30: `update --season 2025`
- Thu 06:00: `recache-pbp`
- Daily schedules: `update --datasets schedules`

## Querying (DuckDB Examples)
Ad-hoc examples:
```sql
-- Weekly counts by season
SELECT season, COUNT(*) AS rows
FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
GROUP BY season ORDER BY season;

-- Schedules per season
SELECT season, COUNT(*) AS games
FROM read_parquet('data/silver/schedules/season=*/**/*.parquet')
GROUP BY season ORDER BY season;
```

## Backfill and Promote Playbook
```bash
# All-history bootstrap (idempotent)
python -m src.cli bootstrap --years 1999-2024

# Selective backfills
python -m src.cli bootstrap --years 1999-2024 --datasets weekly,schedules
python -m src.cli bootstrap --years 2012-2024 --datasets injuries,depth_charts,snap_counts
python -m src.cli bootstrap --years 2002-2024 --datasets rosters

# Promote and profile
python -m src.cli promote --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts
python -m src.cli profile --layer silver --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts
```

## Optional Datasets
Disabled by default; enable in `catalog/datasets.yml`:
- `officials`, `win_totals`, `scoring_lines`, `draft_picks`, `combine`

## Extending the Lake (Add a Dataset)
1) Add importer in `src/importers/nflverse.py` (or custom)
2) Register in `src/importers/__init__.py`
3) Add entry to `catalog/datasets.yml` (importer, partitions, keys, sort_by)
4) Add transform rule in `src/transforms/__init__.py` and schema checks in `src/schemas/__init__.py`
5) Bootstrap, promote, and profile

## Known Upstream Constraints
- Weekly/in-season endpoints may 404 before publications (e.g., early 2025); re-run later or use `promote` on existing Bronze
- Historical availability varies by dataset (e.g., injuries ~2009+, snap_counts ~2013+, rosters coverage differs by function)

## Licensing
- nflverse datasets: CC-BY-4.0; attribute when publishing downstream assets


