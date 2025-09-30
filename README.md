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

### Running tests

```bash
python -m pytest
```

## What’s inside

- Architecture and plan: `plan.md`
- Technical overview: `docs/TECHNICAL_OVERVIEW.md`
- Dataset catalog/config: `catalog/datasets.yml`
- Lineage and quality outputs: `catalog/lineage.json`, `catalog/quality/`
- DraftKings Best Ball rules dataset: `data/silver/dk_bestball/`

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

