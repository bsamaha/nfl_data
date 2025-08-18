# Local NFL Data Lake — Technical Overview

## Purpose
Local, reproducible Parquet-based lake for NFL analytics with clear contracts, partition-aware incremental promotes, and first-class CLI ergonomics.

## Storage, Layout, and Formats
- Files: Parquet with ZSTD compression
- Partitions (Hive-style directories):
  - `pbp`: `year`
  - `weekly`, `schedules`, `rosters`, `rosters_seasonal`, `injuries`, `depth_charts`, `snap_counts`: `season`
  - `ngs_weekly`, `pfr_weekly`: `season`, `stat_type`
  - `pfr_seasonal`: `season`, `stat_type`
  - `dk_bestball`: `section`
  - `players`, `ids`: none (single non-partitioned table)
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
| rosters_seasonal | `season` | `(season, player_id)` | yes |
| injuries | `season` | `(season, week, team, player_id)` | yes |
| depth_charts | `season` | `(season, week, team, position, player_id)` | yes |
| snap_counts | `season` | `(season, week, team, player_id)` | yes |
| players | none | `(gsis_id)` | yes |
| ids | none | `(gsis_id, pfr_id)` | yes |
| dk_bestball | `section` | `(section, id)` | yes |
| ngs_weekly | `season, stat_type` | `(season, week, player_id, stat_type)` | no |
| pfr_weekly | `season, stat_type` | `(season, week, player_id, stat_type)` | no |
| pfr_seasonal | `season, stat_type` | `(season, player_id, stat_type)` | no |
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
- weekly:
  - rename `recent_team → team` if needed
  - enrich `player_name` via coalesce of (`player_name`, `player_display_name`) and left join to `rosters_seasonal` for a roster-derived name, with a final fallback to a season-level PBP name mode; older seasons may have null `team`, which is treated as optional for deduplication
- rosters: normalize mixed numeric columns (e.g., `jersey_number`, `draft_*`) on ingest
- rosters_seasonal: stable per-season roster keyed by `(season, player_id)` with name fields as strings
- ids: deduplicate by `(gsis_id, pfr_id)` keeping the most complete row
- dk_bestball: YAML-normalized with string `value` field and partition `section`
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

## Performance and Reliability
- Writer knobs: `compression=zstd`, `max_rows_per_file`, `row_group_mb` (constrained for Arrow), dictionary encoding (future)
- Parallelism: CLI `--max-workers` (thread pool)
- File lock to prevent overlaps (`.lake.lock`)
- Retries/backoff (tenacity) used in orchestration (can be extended to importers as needed)
 - Importers avoid pandas fragmentation when adding constant columns (e.g., `season`, `year`, `stat_type`) via a concat helper for stability and speed

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

## Parameterized Saved Queries
Saved SQL under `queries/` include a `WITH params AS (...)` block with default values like `season`, `thru_week`, and `season_type`. Use the runner script to override them without editing files.

- Runner:
  - `scripts/run_query.sh -f queries/<file.sql> [flags] [--] [duckdb_args...]`
  - Flags:
    - `-s, --season N` — set `SELECT N AS season`
    - `-w, --thru-week N` — set `SELECT N AS thru_week`
    - `-t, --season-type STR` — set `'REG'|'POST'|'PRE' AS season_type`
    - `-S, --season-start N` — set `SELECT N AS season_start` (YoY range)
    - `-E, --season-end N` — set `SELECT N AS season_end` (YoY range)

- Examples:
  - Top passing leaders through week 10 of 2024:
    ```bash
    scripts/run_query.sh -f queries/passing_leaders_through_week.sql -s 2024 -w 10 -t REG
    ```
  - Team offense EPA by week for 2023 postseason, piping to CSV:
    ```bash
    scripts/run_query.sh -f queries/team_offense_epa_by_week.sql -s 2023 -t POST -- -csv
    ```
  - League efficiency trends for 2005–2024:
    ```bash
    scripts/run_query.sh -f queries/league_efficiency_trends.sql -S 2005 -E 2024 -t REG
    ```
  - Weekly league aggregates for 2024 (default REG):
    ```bash
    scripts/run_query.sh -f queries/league_aggregates_by_week.sql -s 2024
    ```

Notes:
- The runner does an in-place substitution only for queries that declare those params. If a query lacks a given param (e.g., no `thru_week`), the flag is ignored.
- Additional DuckDB CLI flags can follow `--` (e.g., `-csv`, `-json`, or `-unsigned`).

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
- Historical availability varies by dataset (e.g., injuries ~2009+, snap_counts ~2013+, roster coverage differs by function)
- Early seasons (1999–2005) from the weekly endpoint lack `player_name` in upstream; the Silver promotion now backfills names from `rosters_seasonal` and PBP-derived modes, leaving only a tiny set of fringe records unnamed

## Licensing
- nflverse datasets: CC-BY-4.0; attribute when publishing downstream assets


