 ### Local NFL Data Lake — Architecture & Implementation Plan
 
 ## Goals
 - **Durable + queryable** local lake with minimal infra
 - **Fast interactive analytics** on Parquet with partition pruning
 - **Idempotent, auditable pipelines** with validation and lineage
 - **Easy to extend and operate** with simple CLI and Make targets
 
 ## 1) Data Layout and Formats
 - **Storage**: Parquet with ZSTD compression
 - **Partitioning**:
   - **pbp**: `year`
   - **weekly / schedules / rosters / injuries / depth_charts / snap_counts**: `season` (optionally `week` for high-churn datasets)
   - **ngs / pfr**: `season`, `stat_type`
 - **Avoid** high-cardinality partitions (e.g., `team`, `player_id`)
 - **Row groups**: target ~96–128 MB for large tables
 - **Dtypes**:
   - IDs as strings; timestamps UTC; booleans nullable; numeric via pandas nullable dtypes (`Int64`, `Float64`); categoricals for enums
 - **Metadata columns** (bronze and silver): `source`, `ingested_at` (UTC), `pipeline_version`, `upstream_version`, `run_id`
 - **Timezone**: all timestamps in UTC
 
 Note: Keep using `pyarrow.parquet.write_to_dataset` for simplicity now; switch to `pyarrow.dataset.write_dataset` later if you need finer control over file sizes and sorting.
 
 ## 2) Lake Layers
 - **Bronze**: raw-as-downloaded (typed, minimal reshaping), append-only, partitioned, stamped with metadata
 - **Silver**: deduped and normalized; stable column names & dtypes; merge by business keys; prefer newest by `ingested_at`
 - **Gold**: denormalized marts (e.g., player-week, team-week, game)
 
 Bronze retention: keep all bronze by default; optionally compact/GC old bronze for very high-churn datasets while retaining lineage.
 
 ## 3) Catalog and Configuration
 - `catalog/datasets.yml` top-level: `root`, `compression`, `row_group_mb`
 - Per-dataset fields: `importer`, `years` or `ALL`, `partitions`, `key`, `args`, `options`, `enabled`, `sort_by`, `max_rows_per_file`
 - Use `.env` (loaded via `python-dotenv`) to override `root` and scheduling knobs locally
 
 Example:
 ```yaml
 root: "data"
 compression: "zstd"
 row_group_mb: 96
 
 datasets:
   pbp:
     importer: "import_pbp_data"
     years: "ALL"
     partitions: ["year"]
     key: ["game_id","play_id"]
     options: { downcast: true, cache: true }
     enabled: true
     sort_by: ["year","game_id","play_id"]
     max_rows_per_file: 5000000
 ```
 
 ## 4) Ingestion and Orchestration
 - **CLI (Typer)** remains the entrypoint; add:
   - `--datasets` filter (comma-separated) to run a subset
   - `--max-workers` to control parallelism across independent datasets
   - Retries with exponential backoff per dataset
   - A simple file lock to prevent overlapping runs
   - `promote` command to promote existing bronze partitions to silver without re-fetching
 - **Flows**:
   - `bootstrap`: historical backfill (1999 → last season)
   - `update`: in-season (current season), idempotent
   - `recache-pbp`: re-pull current season on Thu for stat corrections
   - `promote`: read existing bronze by partition, merge with existing silver, dedupe on keys, and atomically replace only that partition
 - **Parallelism**: parallelize independent datasets; cap workers to avoid upstream hammering
 
 Incremental silver promotion (partition-aware):
 - Track which partitions changed during bronze ingest
 - Merge/dedupe only those partitions into silver to avoid full scans
 
 Pseudo-logic:
 ```python
 # During bronze write:
 changed_partitions = discover_changed_partitions(df, partitions)
 update_lineage(..., info={"changed_partitions": list(changed_partitions)})
 
 # During promote_to_silver:
 for partition in changed_partitions:
     bronze_part = load_bronze_partition(dataset, partition)
     silver_existing = try_load_silver_partition(dataset, partition)
     merged = merge_dedupe(silver_existing, bronze_part, key_cols)
     write_silver_partition(dataset, merged, partition)
 ```
 
 ## 5) Data Quality and Schemas
 - Use **Pandera** for per-dataset schemas in `catalog/schema/*.py` (or YAML + codegen)
 - Enforce:
   - Column presence and dtypes
   - Unique keys in silver
   - Nullability and domain checks
   - Rowcount deltas and freshness thresholds
 - Persist quality reports to `catalog/quality/<dataset>/<run_id>.json`
 
 Example:
 ```python
 from pandera import DataFrameSchema, Column, Check
 PBPSchema = DataFrameSchema({
   "game_id": Column(str),
   "play_id": Column("Int64", Check.ge(1)),
 }, coerce=True)
 ```
 
 ## 6) Lineage, Observability, and Audit
 - Expand `catalog/lineage.json` to include:
   - **Run-level**: `run_id`, `started_at`, `ended_at`, `pipeline_version`, `upstream_version`, `status`
   - **Dataset-level**: `last_ingest_utc`, `rows_last_batch`, `changed_partitions`
   - **Partition-level**: `row_count`, `sha256_fingerprint`, `min_ingested_at`, `max_ingested_at`
 - Partition fingerprint on keys (or key + stable columns) to detect changes efficiently
 - Structured logs to `logs/<run_id>.jsonl`; emit per-dataset timings and row counts
 - Implemented: per-partition lineage stats computed during promote (row_count, key fingerprint, min/max ingested_at)
 
 Example lineage entry:
 ```json
 {
   "pbp": {
     "last_ingest_utc": "2025-08-15T03:30:00Z",
     "rows_last_batch": 94321,
     "changed_partitions": ["year=2025"],
     "partitions": {
       "year=2025": {
         "row_count": 1345678,
         "sha256_fingerprint": "9f3e...a1b",
         "max_ingested_at": "2025-08-15T03:30:00Z"
       }
     }
   }
 }
 ```
 
 ## 7) Silver Normalization Rules
 - Enforce snake_case columns and stable order
 - Cast IDs to string; timestamps to UTC; booleans to nullable
 - Drop volatile, low-value columns in silver unless needed downstream
 - Optional sort within partition by `sort_by` before write (improves stats/pruning) — Implemented: silver writer now pre-sorts by configured `sort_by` if present
 - Add per-dataset transform hook: `src/lake/transforms/<dataset>.py` with `def to_silver(df) -> df`
 - Add per-dataset transform hook: `src/lake/transforms/<dataset>.py` with `def to_silver(df) -> df`
 - Weekly specifics: if `team` is missing but `recent_team` exists, rename `recent_team → team`
 
 ## 8) Querying and Gold
 - Use **DuckDB** for ad-hoc SQL (no server) and direct Parquet reads
 - Optionally keep a `duckdb.db` for views; still query Parquet directly
 - Provide `queries/` with ready-to-run SQL
 - Gold marts:
   - `gold/player_week`: player-week wide (usage, fantasy, NGS, PFR)
   - `gold/team_week`: team-week aggregates
   - `gold/game`: game-level aggregates (officials, lines)
 
 Example DuckDB query:
 ```sql
 -- 2025 receiving yards leaders through week 3
 SELECT player_id, player_name, SUM(receiving_yards) AS yds
 FROM read_parquet('data/silver/weekly/season=2025/*.parquet')
 WHERE week <= 3
 GROUP BY 1,2
 ORDER BY yds DESC
 LIMIT 50;
 ```
 
 ## 9) Testing
 - `pytest`:
   - Unit tests for `fetch`, `merge_dedupe`, schema validation, lineage updates
   - Fixtures to monkeypatch `nfl_data_py` with small deterministic frames
   - Property tests (Hypothesis) on dedupe stability
 - Contract tests:
   - Verify dataset key uniqueness in silver
   - Verify dtype and column set invariants
 
 ## 10) Scheduling
 - Start with **cron**; use a file lock to avoid overlaps
 - Suggested cadence (in-season, local time):
   - Nightly 03:30: pbp, weekly, rosters, injuries, depth_charts, ngs, snap_counts
   - Thu 06:00: recache PBP
   - Schedules daily or more often if building live features
 
 Crontab examples:
 ```bash
 # 03:30 in-season nightly
 30 3 * 9-2 * cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli update --season 2025 | ts | tee -a logs/cron_update.log
 # Thu corrections re-pull
 0 6 * 9-2 4 cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli recache-pbp --season 2025 | ts | tee -a logs/cron_recache.log
 # Schedules once daily
 5 4 * * * cd /home/r16/workspace/nfl_data && flock -n .lake.lock -- python -m src.cli update --season 2025 --datasets schedules | ts | tee -a logs/cron_schedules.log
 ```
 
 Backfill scripts:
 ```bash
 # All-history bootstrap (safe to re-run; idempotent writes)
 python -m src.cli bootstrap --years 1999-2024

 # Selective backfills
 python -m src.cli bootstrap --years 1999-2024 --datasets weekly,schedules
 python -m src.cli bootstrap --years 2012-2024 --datasets injuries,depth_charts,snap_counts
 python -m src.cli bootstrap --years 2002-2024 --datasets rosters

 # Promote any bronze left behind (partition-scoped, atomic)
 python -m src.cli promote --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts

 # Profile silver quality by partition
 python -m src.cli profile --layer silver --datasets weekly,schedules,rosters,injuries,depth_charts,snap_counts
 ```
 
 ## 11) Resilience and Performance Knobs
 - Retries/backoff for fetch; per-dataset timeouts
 - Parallelism: `--max-workers` (default 2–4); cap per dataset if upstream sensitive
 - Writer knobs: `row_group_mb`, `max_rows_per_file`, `use_dictionary`
 - Compaction: optionally re-write large partitions periodically to target file sizes and sort order
 - Memory: process by partition/chunk to cap memory during merges — Implemented: vectorized metadata stamping (pandas concat) to avoid fragmentation
 
 ## 12) Governance and Licensing
 - Persist `source`, `upstream_version` (from `nfl_data_py.__version__)`, and `pipeline_version`
 - Licensing:
   - nflverse data: CC-BY-4.0 (attribute when publishing)
   - FTN subset: CC-BY-SA 4.0; consider `enabled: false` by default
 
 ## 13) Optional Extensions (Later)
 - Switch storage root to S3 with `s3fs` without changing code paths
 - Pre-commit with black/ruff/mypy; CI to run validation on PRs
 - Dev container for reproducible environment
 - Prefect if you need SLAs/observability beyond cron
 
 ## 14) Minimal Implementation Delta
 - Add `enabled`, `sort_by`, `max_rows_per_file` to `datasets.yml`; optionally enable `cache` for PBP
 - Add partition-aware lineage (per-partition counts + fingerprints)
 - Update promote-to-silver to operate only on changed partitions
 - Add CLI options: `--datasets`, `--max-workers`, retries with backoff, file lock
 - Add `src/lake/transforms/<dataset>.py` hooks and apply in silver
 - Add Pandera schemas and persist quality reports
 - Add `queries/` and a couple of gold builders
 
 ## 15) Quick Start
 ```bash
 # 1) Setup
 python -m venv .venv && source .venv/bin/activate
 pip install -r requirements.txt
 
 # 2) Bootstrap history
 python -m src.cli bootstrap --years 1999-2024
 
 # 2b) Promote existing bronze partitions to silver (e.g., weekly)
 python -m src.cli promote --datasets weekly
 
 # 3) In-season update
 python -m src.cli update --season 2025
 
 # 4) Query with DuckDB
 duckdb
 SELECT COUNT(*) FROM read_parquet('data/silver/pbp/year=2025/*.parquet');
 -- Quick verification of weekly row counts per season
 SELECT season, COUNT(*) AS rows
 FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
 GROUP BY season
 ORDER BY season;
 ```
 
 ## Why this improves the current plan
 - Faster promotes via incremental, partition-aware merges
 - Stable, query-friendly files with better writer controls and optional sort order
 - Stronger data contracts and quality checks, persisted for audit
 - Clear lineage at partition granularity for debugging and rebuilds
 - Operational resilience (retries, locks, selective datasets, parallelism)

  ## 16) Implementation Improvements (additions)
  - **Config validation and typing**:
    - Validate `catalog/datasets.yml` with Pydantic models on load; enforce key/partition consistency and option types
    - Maintain canonical per-dataset Arrow schemas and cast on write to stabilize dtypes across seasons
  - **Partition-scoped promotes with atomic writes**:
    - Track `changed_partitions` during bronze ingest and promote only those partitions to silver
    - Write silver to `data/silver/_staging/<dataset>/...` and rename into place atomically to avoid partial results
  - **Faster transforms/dedupe**:
    - Prefer Polars for CPU-bound normalization and dedupe
  ```python
  import polars as pl
  df_merged = (
    pl.concat([silver_existing, bronze_part], how="vertical", rechunk=True)
      .sort("ingested_at")
      .unique(subset=key_cols, keep="last")
  )
  ```
    - Or use DuckDB windowed de-duplication per-partition
  ```sql
  CREATE TEMP TABLE silver_part AS
    SELECT * FROM read_parquet('data/silver/<dataset>/<partition>/*.parquet');
  CREATE TEMP TABLE bronze_part AS
    SELECT * FROM read_parquet('data/bronze/<dataset>/<partition>/*.parquet');
  CREATE TEMP TABLE merged AS
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY game_id, play_id ORDER BY ingested_at DESC
    ) AS rn
    FROM (SELECT * FROM silver_part UNION ALL SELECT * FROM bronze_part)
  ) WHERE rn=1;
  COPY (SELECT * FROM merged)
  TO 'data/silver/<dataset>/' (FORMAT PARQUET, PARTITION_BY (partition_cols), OVERWRITE_OR_IGNORE 1);
  ```
  - **Writer optimizations**:
    - Prefer `pyarrow.dataset.write_dataset` with `max_rows_per_group`, `max_rows_per_file`, `compression='zstd'`, and a tuned `compression_level` (5–7)
    - Enable `use_dictionary` for low-cardinality columns and sort within partition by `sort_by` before write to improve pruning — Implemented: silver writer supports `sort_by` pre-sorting
  - **Resilient fetch + CLI/logging**:
    - Retries with exponential backoff + jitter (`tenacity`); thread pool for network-bound fetch
    - Additional CLI flags: `--since` (YYYY-MM-DD), `--dry-run`, `--no-validate`, `--force-rewrite`
    - Structured JSONL logs via `structlog` including a `run_id`; continue to use file lock to avoid overlaps
  - **Data quality and schema drift**:
    - Expand Pandera checks (uniqueness on keys, enums as categoricals, nullability); fail run on schema regressions
    - Reindex to canonical column list, fill missing with nulls; record column additions/removals in lineage/logs
    - Optionally adopt Great Expectations for a UI-first quality layer
  - **Performance and maintenance**:
    - Process by partition/chunk to cap memory during merges; process pool for CPU-bound transforms
    - Periodic compaction to target file sizes and sort order; optional Bloom filters when supported
    - Implemented: replace per-row pandas assignments with vectorized concat to reduce fragmentation
  - **Optional modeling/format upgrades**:
    - dbt-duckdb for silver/gold SQL modeling with built-in tests (unique, not_null, accepted_values), docs, and exposures
    - If you need ACID/time travel: Delta Lake (delta-rs) or Apache Iceberg (via DuckDB extensions). Adds schema evolution, VACUUM, time travel at the cost of complexity

  ## 17) DevX and CI
  - Pre-commit with ruff, black, mypy for consistent formatting, linting, and typing
  - CI runs unit tests and a tiny e2e that bootstraps 1–2 seasons into a temp `root` and validates silver key uniqueness and dtypes
  - Attach `logs/<run_id>.jsonl` as CI artifacts for failed runs
  - Optional dev container for reproducible local environment

  ## 18) Recommended defaults
  - Keep Parquet + DuckDB as the default local stack (simple, fast)
  - Add Polars for transforms and implement partition-scoped promotes with atomic writes
  - Add Pydantic config validation, retries with `tenacity`, and structured logging
  - Medium term: adopt dbt-duckdb for silver/gold modeling and tests
  - Consider Delta/Iceberg only if you truly need ACID/time travel or multi-writer concurrency

  ## 19) Data sources and coverage
  - **Primary (via nflverse / nfl_data_py)**:
    - `pbp` (1999→present): play-by-play with EPA/WP fields, penalties, personnel
    - `weekly`, `seasonal`: player-week and season stats
    - `schedules`: game metadata and results
    - `rosters` (weekly), `ids` (crosswalk of external IDs)
    - `injuries`, `depth_charts`, `snap_counts`
    - `ngs` weekly (`passing`, `rushing`, `receiving`)
    - `pfr_weekly`, `pfr_seasonal` (pass/rush/rec)
    - `draft_picks`, `combine`, `officials`, `win_totals`, `scoring_lines`
  - **Optional/adjacent sources** (for enrichment or cross-checks):
    - Pro-Football-Reference (historical stats, coaches, draft)
    - NFL Savant (PBP CSVs; strong 2013–2022 coverage)
    - Next Gen Stats (advanced metrics; also exposed via `ngs` weekly)
  - **Update cadence** (in-season norms): nightly for PBP/player stats; schedules update minutes-level; rosters daily; NGS nightly; snap counts periodic
  - **Licensing**: General nflverse data under CC-BY-4.0; FTN subset CC-BY-SA 4.0

  ## 20) Tables and transformations by layer
  - The lake exposes consistent tables per layer. Bronze mirrors upstream; Silver enforces keys/dtypes; Gold provides marts.

  ### 20.1 Dataset catalog (keys, partitions, usage)
  
  | Dataset | Primary keys | Partitions | Bronze steps | Silver steps | Gold usage |
  |---|---|---|---|---|---|
  | pbp | (game_id, play_id) | year | Coerce types, add metadata | Dedupe by keys (keep newest), normalize columns/dtypes, unify team codes, ensure UTC | Fact play, aggregates to team/player-week |
  | weekly | (season, week, player_id, team) | season | As-ingested | Dedupe, cast IDs, align columns, join `ids` to add cross-IDs | `fact_player_week` |
  | schedules | (season, game_id) | season | As-ingested | Normalize dates/times (UTC), unify home/away/team codes | `fact_game`, join key for other marts |
  | rosters | (season, week, player_id, team) | season | As-ingested | Latest per player/week, normalize positions, names | `dim_player`, enrich facts |
  | injuries | (season, week, team, player_id, report_date) | season, week | As-ingested | Keep latest per (keys), standardize statuses | Injury flags in `fact_player_week` |
  | depth_charts | (season, week, team, position, player_id) | season, week | As-ingested | Latest slot per team/pos | Starter/role features in gold |
  | snap_counts | (season, week, team, player_id) | season, week | As-ingested | Dedupe, cast | Utilization features in `fact_player_week` |
  | ngs (weekly) | (season, week, player_id, stat_type) | season, stat_type | Stack per stat_type | Dedupe, cast, align stat fields | Advanced features (e.g., CPOE) in player/week |
  | pfr_weekly | (season, week, player_id, stat_type) | season, stat_type | Stack per s_type | Dedupe, cast | Supplemental features in player/week |
  | pfr_seasonal | (season, player_id, stat_type) | season, stat_type | Stack per s_type | Dedupe, cast | Season-level features |
  | draft_picks | (season, round, overall, team) | season | As-ingested | Normalize team codes | Prospect context in dims |
  | combine | (season, player_id) | season | As-ingested | Cast metrics, units | `dim_player` attributes |
  | ids | (pfr_id, gsis_id, pff_id, espn_id, ...) | — | As-ingested | Build canonical `player_id` crosswalk | Join for consistent IDs |
  | officials | (game_id, official_id) | season | As-ingested | Dedupe | `fact_game` enrichment |
  | win_totals | (season, team) | season | As-ingested | Cast | Team priors in `fact_team_week` |
  | scoring_lines | (season, game_id) | season | As-ingested | Cast, dedupe | Odds lines in `fact_game` |

  ### 20.2 Bronze → Silver transforms (canonical rules)
  - Enforce snake_case, stable column order
  - Coerce IDs to string; timestamps to UTC; nullable logical dtypes (`Int64`, `Float64`)
  - Dedupe on keys, keep newest by `ingested_at`
  - Team codes: normalize aliases (e.g., `JAX` vs `JAC`, `LA` vs `LAR`), use a canonical mapping
  - Player IDs: join `ids` to attach canonical `player_id` and external IDs; prefer `gsis_id` if available
  - Schema drift: reindex to canonical columns; track adds/drops in lineage

  ### 20.3 Silver → Gold marts
  - `gold/fact_player_week` (grain: season, week, player_id, team)
    - Sources: silver.weekly + features from silver.ngs, silver.pfr_weekly, silver.snap_counts, silver.injuries, silver.depth_charts
    - Measures: rushing_yards, receiving_yards, passing_yards, TDs, targets, attempts, snaps, injury_status flags, advanced (e.g., CPOE)
    - Dimensions: `dim_player`, `dim_team`, `dim_game`
  - `gold/fact_team_week` (grain: season, week, team)
    - Sources: aggregates from silver.pbp + schedules + odds
    - Measures: points_for/against, EPA per play (off/def), success rate, pace, penalties
  - `gold/fact_game` (grain: game_id)
    - Sources: silver.schedules + silver.scoring_lines + silver.officials
    - Attributes: kickoff_utc, home/away teams, scores, spreads/totals, officiating crew
  - Dimensions
    - `gold/dim_player`: from silver.rosters + ids (latest per player/season/week)
    - `gold/dim_team`: team metadata and aliases
    - `gold/dim_game`: selected schedule fields for joins

  ## 21) Visualizations
  
  ### 21.1 End-to-end pipeline (per dataset)
  ```mermaid
  graph LR
      subgraph Ingestion
          I1[pbp]:::ds
          I2[weekly]:::ds
          I3[schedules]:::ds
          I4[rosters]:::ds
          I5[injuries/depth/snap]:::ds
          I6[ngs/pfr]:::ds
          I7[lines/officials]:::ds
      end
      subgraph Bronze
          B1[bronze/pbp]
          B2[bronze/weekly]
          B3[bronze/schedules]
          B4[bronze/rosters]
          B5[bronze/injuries_depth_snap]
          B6[bronze/ngs_pfr]
          B7[bronze/lines_officials]
      end
      subgraph Silver
          S1[silver/pbp]
          S2[silver/weekly]
          S3[silver/schedules]
          S4[silver/rosters]
          S5[silver/injuries_depth_snap]
          S6[silver/ngs_pfr]
          S7[silver/lines_officials]
      end
      subgraph Gold
          G1[gold/fact_player_week]
          G2[gold/fact_team_week]
          G3[gold/fact_game]
          D1[gold/dim_player]
          D2[gold/dim_team]
          D3[gold/dim_game]
      end
      classDef ds fill:#e0f3ff,stroke:#7fbfff,color:#000;
      I1-->B1-->S1-->G2
      I2-->B2-->S2-->G1
      I3-->B3-->S3-->G3
      I4-->B4-->S4-->D1
      I5-->B5-->S5-->G1
      I6-->B6-->S6-->G1
      I7-->B7-->S7-->G3
      S3-->D3
      S2-->D1
      S1-->G1
      S1-->G2
  ```
  
  ### 21.2 Gold model ERD
  ```mermaid
  erDiagram
      DIM_PLAYER {
          string player_id PK
          string name
          string position
          string team_id
      }
      DIM_TEAM {
          string team_id PK
          string team_abbr
          string conference
          string division
      }
      DIM_GAME {
          string game_id PK
          int season
          int week
          string home_team_id
          string away_team_id
          datetime kickoff_utc
      }
      FACT_PLAYER_WEEK {
          string player_id FK
          string team_id FK
          string game_id FK
          int season
          int week
          int snaps
          int attempts
          int targets
          float yards
          float tds
          float cpoe
      }
      FACT_TEAM_WEEK {
          string team_id FK
          string game_id FK
          int season
          int week
          float epa_per_play_off
          float epa_per_play_def
          int points_for
          int points_against
      }
      FACT_GAME {
          string game_id FK
          float spread_close
          float total_close
      }
      DIM_PLAYER ||--o{ FACT_PLAYER_WEEK : player_id
      DIM_TEAM ||--o{ FACT_PLAYER_WEEK : team_id
      DIM_TEAM ||--o{ FACT_TEAM_WEEK : team_id
      DIM_GAME ||--o{ FACT_PLAYER_WEEK : game_id
      DIM_GAME ||--o{ FACT_TEAM_WEEK : game_id
      DIM_GAME ||--o{ FACT_GAME : game_id
  ```


