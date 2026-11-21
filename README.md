# Local NFL Data Lake

A local NFL lakehouse: Parquet files on disk in bronze and silver layers, queried with DuckDB, with a Typer CLI, a YAML dataset catalog and Streamlit pages.

## Status

The repo is being rebuilt in place. The steps, in order:

1. Source inventory. Done: `docs/SOURCE_INVENTORY.md`.
2. Clean project. Done: pinned Python and dependencies, dead code and committed run output removed, tests run with plain `pytest`.
3. Rebuild ingest.
4. Core tables.
5. Features.
6. First projection.
7. Rebuild the app.

Until step 3 lands, the pipeline under `src/` is the previous team's code. It installs and its tests pass, but it is known to lose corrections, duplicate rows and record failed runs as successes, so do not rely on data it produces. The Streamlit pages import `app/lib/data.py`, which was never committed, so no page loads until step 7.

## Install and test

You need [uv](https://docs.astral.sh/uv/) and nothing else: no bash, no Make and no DuckDB command-line tool. The commands are the same in PowerShell, cmd and a Unix shell. Run them from the repo root.

```
uv sync
uv run pytest
```

`uv sync` creates `.venv` from `uv.lock`, and downloads Python 3.14 first if you do not have it (the version is set in `.python-version`). The expected test result is 26 passed and 1 skipped. The skipped module is `tests/test_defense_report.py`, which needs the missing `app/lib/data.py`.

To add or upgrade a dependency, edit `pyproject.toml` and run `uv lock`, then commit both files.

## Commands

These replace the old Makefile. Each one runs inside the locked environment.

| Old Makefile target | Command |
|---|---|
| `make install` | `uv sync` |
| `make bootstrap` | `uv run python -m src.cli bootstrap --years 1999-2025` |
| `make update` | `uv run python -m src.cli update --season 2026` |
| `make recache` | `uv run python -m src.cli recache-pbp --season 2026` |
| `make app` | `uv run streamlit run app/Home.py` |
| `make fmt` | `uvx black src` |

The CLI has two more commands, and `--help` lists the options of each:

```
uv run python -m src.cli --help

# Promote existing bronze to silver without fetching
uv run python -m src.cli promote --datasets weekly,schedules

# Write partition metrics for a layer
uv run python -m src.cli profile --layer silver --datasets weekly
```

`bootstrap`, `update` and `recache-pbp` download from nflverse and run the old pipeline described under Status. `update` also tries to build the reports in `queries/reports/` by calling `bash scripts/run_query.sh`, which needs bash and the DuckDB command-line tool. Without them it logs `report_materialization_failed` and carries on.

### Querying

Query the lake from Python. DuckDB runs in-process:

```python
import duckdb

rows = duckdb.sql("""
    SELECT season, COUNT(*) AS n
    FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
    GROUP BY season
    ORDER BY season
""").fetchall()
for season, n in rows:
    print(season, n)
```

Save it as a file and run it with `uv run python your_file.py`.

## What's inside

- Source contracts for every nflverse dataset: `docs/SOURCE_INVENTORY.md`
- Technical overview of the old pipeline: `docs/TECHNICAL_OVERVIEW.md`
- Dataset catalog: `catalog/datasets.yml`
- DraftKings rule files: `catalog/draftkings/`
- Lineage and quality output: `catalog/lineage.json` and `catalog/quality/`. Pipeline runs write them and git ignores them.
- DraftKings Best Ball rules dataset: `data/silver/dk_bestball/`

## Scheduling

Wait for the rebuilt ingest (step 3) before scheduling anything. The examples show the shape of a job, with `/path/to/nfl_data` standing for your clone.

`uv run --directory` sets the working directory, which the CLI needs because it reads `catalog/` and writes `data/` and `logs/` relative to it. `bootstrap` and `update` take their own lock on `.lake.lock`, so do not wrap them in `flock` on that file. The earlier examples did, and such a job blocks on its own lock forever.

cron:

```
# 03:30 nightly, September to February
30 3 * 9-12,1-2 * uv run --directory /path/to/nfl_data python -m src.cli update --season 2026 >> /path/to/nfl_data/logs/cron_update.log 2>&1
# Thursday re-pull of play-by-play corrections
0 6 * 9-12,1-2 4 uv run --directory /path/to/nfl_data python -m src.cli recache-pbp --season 2026 >> /path/to/nfl_data/logs/cron_recache.log 2>&1
# Schedules daily
5 4 * * * uv run --directory /path/to/nfl_data python -m src.cli update --season 2026 --datasets schedules >> /path/to/nfl_data/logs/cron_schedules.log 2>&1
```

Windows Task Scheduler: create a task whose action has Program `uv` and Arguments `run --directory C:\path\to\nfl_data python -m src.cli update --season 2026`.

## Notes

- Optional datasets are scaffolded (`officials`, `win_totals`, `scoring_lines`, `draft_picks`, `combine`); enable them in `catalog/datasets.yml` when needed.


## DraftKings Best Ball (rules, playoffs, and analytics)

The repository includes a static dataset `dk_bestball` that captures DraftKings NFL Best Ball rules (scoring, lineup/roster constraints, draft timing, and tournament rounds/tie-breakers). It is written from `catalog/draftkings/bestball.yml` and materialized to Parquet under `data/silver/dk_bestball/section=...`.

### Tournament playoffs logic (Rounds 2–4)

- Round 1: NFL Weeks 1–14
- Round 2: NFL Week 15
- Round 3: NFL Week 16
- Round 4: NFL Week 17

Advancement and seeding
- At the end of each round, a specified number of entries advance from each contest based on placement; remaining wild-card spots are filled by the highest-scoring non-advancing entries across the round.
- Advancing entries keep the same drafted lineup (no redraft between rounds).

Tie-breakers (applied in order within a round)
1) Highest single-week score in that round
2) Next-highest single-week, and so on through the lowest-scoring week in the round
3) Highest scoring single player in that round
4) Next-highest scoring single player, and so on
5) Highest cumulative score through all completed rounds
6) Earliest draft slot

All of the above is captured in `dk_bestball`:
- `section=tournaments_rounds`: round numbers and week lists
- `section=tournaments`: tie-breakers description
- `section=lineup` and `section=roster`: weekly slot limits (QB=1, RB=2, WR=3, TE=1, FLEX=1 from RB/WR/TE); roster size (20), bench (12), max QBs/TEs, and auto-draft caps
- `section=scoring`: atomic scoring rules (PPR receptions, yardage, TDs, bonuses, turnovers, return TDs, 2PT, offensive fumble TD)

### Building analytical tables with Best Ball rules

Below are practical patterns for turning core nflverse data into Best Ball-ready analytics. Adjust column names to your local `weekly` schema if needed.

1) Player-week DK PPR points (foundation)

```sql
-- Player-week DraftKings PPR points (approximate fields; adjust to match your weekly table)
CREATE OR REPLACE VIEW dkbb_player_week AS
SELECT
  season,
  week,
  player_id,
  COALESCE(position, pos) AS pos,
  team,
  -- Yardage + TDs + receptions + turnovers + 2PT + return/offensive FR TDs
  0.04 * COALESCE(passing_yards, 0) +
  4    * COALESCE(passing_tds, 0) -
  1    * COALESCE(interceptions, 0) +
  0.10 * (COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0)) +
  6    * (COALESCE(rushing_tds, 0) + COALESCE(receiving_tds, 0)) +
  1    * COALESCE(receptions, 0) +
  6    * (COALESCE(kick_return_tds, 0) + COALESCE(punt_return_tds, 0) + COALESCE(field_goal_return_tds, 0)) -
  1    * COALESCE(fumbles_lost, 0) +
  2    * COALESCE(two_point_conversions, 0) +
  6    * COALESCE(offensive_fumble_recovery_tds, 0) +
  -- Bonuses
  CASE WHEN COALESCE(passing_yards, 0)  >= 300 THEN 3 ELSE 0 END +
  CASE WHEN COALESCE(rushing_yards, 0)  >= 100 THEN 3 ELSE 0 END +
  CASE WHEN COALESCE(receiving_yards, 0)>= 100 THEN 3 ELSE 0 END
  AS dk_points
FROM read_parquet('data/silver/weekly/season=*/**/*.parquet');
```

2) Round-level player aggregates (for playoff scouting and round modeling)

```sql
-- Map season/week to BK rounds and aggregate points
CREATE OR REPLACE VIEW dkbb_player_round AS
WITH with_round AS (
  SELECT *,
    CASE
      WHEN week BETWEEN 1 AND 14 THEN 1
      WHEN week = 15 THEN 2
      WHEN week = 16 THEN 3
      WHEN week = 17 THEN 4
    END AS round
  FROM dkbb_player_week
)
SELECT season, round, player_id, pos,
       SUM(dk_points)        AS round_points,
       MAX(dk_points)        AS best_week_in_round,
       AVG(dk_points)        AS avg_week_in_round,
       COUNT(*)              AS games_in_round
FROM with_round
WHERE round IS NOT NULL
GROUP BY season, round, player_id, pos;
```

3) Entry/week Best Ball lineup selection (template)

If you track your own Best Ball entries in a local table `entries(entry_id, player_id, pos)`:

```sql
-- Join entries to points and rank within positions
WITH epw AS (
  SELECT e.entry_id, w.season, w.week, w.player_id, w.pos, w.dk_points,
         ROW_NUMBER() OVER (PARTITION BY e.entry_id, w.season, w.week, w.pos ORDER BY w.dk_points DESC) AS rpos
  FROM entries e
  JOIN dkbb_player_week w USING (player_id)
),
qb AS (
  SELECT * FROM epw WHERE pos='QB' AND rpos<=1
),
rb AS (
  SELECT * FROM epw WHERE pos='RB' AND rpos<=2
),
wr AS (
  SELECT * FROM epw WHERE pos='WR' AND rpos<=3
),
te AS (
  SELECT * FROM epw WHERE pos='TE' AND rpos<=1
),
flex_candidates AS (
  SELECT * FROM epw WHERE (pos IN ('RB','WR','TE')) AND (
      (pos='RB' AND rpos>2) OR (pos='WR' AND rpos>3) OR (pos='TE' AND rpos>1)
  )
),
flex AS (
  SELECT entry_id, season, week, player_id, pos, dk_points
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY entry_id, season, week ORDER BY dk_points DESC) AS r
    FROM flex_candidates
  ) WHERE r=1
)
SELECT entry_id, season, week,
       SUM(dk_points) AS dk_points_week
FROM (
  SELECT entry_id, season, week, dk_points FROM qb
  UNION ALL SELECT entry_id, season, week, dk_points FROM rb
  UNION ALL SELECT entry_id, season, week, dk_points FROM wr
  UNION ALL SELECT entry_id, season, week, dk_points FROM te
  UNION ALL SELECT entry_id, season, week, dk_points FROM flex
)
GROUP BY entry_id, season, week;
```

4) Entry round scores + tie-breaker features (template)

```sql
-- Sum weekly scores into tournament rounds and compute tie-break helpers
WITH ew AS (
  SELECT *,
    CASE
      WHEN week BETWEEN 1 AND 14 THEN 1
      WHEN week = 15 THEN 2
      WHEN week = 16 THEN 3
      WHEN week = 17 THEN 4
    END AS round
  FROM entry_week_scores  -- result of step (3)
), agg AS (
  SELECT entry_id, season, round,
         SUM(dk_points_week) AS round_points,
         MAX(dk_points_week) AS round_best_week
  FROM ew
  WHERE round IS NOT NULL
  GROUP BY entry_id, season, round
)
SELECT * FROM agg;
-- To emulate full tie-break chains, keep the per-week distribution within the round
-- and sort lexicographically by week scores DESC, then by best individual player if needed.
```

This setup lets you analyze playoff advancement odds, wild-card thresholds, and roster constructions for Weeks 15–17 while remaining consistent with DK’s rules stored in `dk_bestball`.

