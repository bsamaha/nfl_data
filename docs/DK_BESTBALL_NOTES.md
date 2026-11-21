# DraftKings Best Ball notes

Moved here from the README during the ingest rebuild. The rules are still in `catalog/draftkings/bestball.yml`.
The SQL below was written against the previous pipeline: it reads `data/silver/weekly`, which no longer exists, uses the
legacy column names (`interceptions`, `fumbles_lost`), and mentions a `dk_bestball` dataset that the old importer wrote.
Treat it as a sketch to rebuild on the core tables, not as runnable queries.


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

