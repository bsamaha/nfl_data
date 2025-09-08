## Feature Request: Utilization Report (Data + Queries Only)

### Executive summary
Build a comprehensive, data-only utilization reporting suite that blends classic usage metrics (snap share, route participation, TPRR/YPRR, situational shares) with DFS-focused, research-backed edges (explosive rates, personnel/formation splits, xFP-lite, PROE/pace context, stacking correlations, and leverage). Deliver reproducible Gold-layer tables and parameterized DuckDB queries; no UI is in scope.

### Scope (in-season, current year only)
- Primary focus is the active NFL season for in-season reporting and DFS workflows.
- All queries default to a single `:season` parameter; recommended default is the latest ingested season.
- Gold marts will be materialized for the current season only by default; historical multi-season backfills are out-of-scope for this phase.
- Roster-change complexities across seasons are avoided by constraining to the single active season; per-week roster joins are still supported within-season.
 - Default `season_type` is `REG` with the ability to override via params when needed (e.g., `POST`).
 - The default `:season` in queries should auto-detect the latest active season from ingested data (e.g., `schedules`), while still allowing explicit overrides via runner flags.
 - Gold materialization runs automatically at the end of the `update --season <current>` workflow (no separate trigger required).
 - Default season detection is non-destructive: when `-s` is omitted, the runner selects the default season as the maximum `season` in Silver `schedules` (fallback to maximum `year` in Silver `pbp`). This only sets defaults for queries/materialization and does not delete prior seasons' data.
 - Re-materialization overwrites only the current-season partitions being built; historical partitions are not pruned or deleted by this job.
 - Default season detection logic: `MAX(season)` from Silver `schedules`; fallback to `MAX(year)` from Silver `pbp` if needed.

### Objective
Create gold-layer tables and parameterized DuckDB queries to power a weekly utilization report similar in spirit to Dwain McFarland's. Focus is limited to generating the analytical tables and the SQL to retrieve metrics; no UI work is in scope.

### Data sources (existing silver)
- `weekly` — per player-week box score and advanced shares: `targets`, `receptions`, `receiving_yards`, `receiving_air_yards`, `target_share`, `air_yards_share`, `wopr`, `carries`, `rushing_yards`, etc.
- `snap_counts` — per player-game/week offensive snaps and share: `offense_snaps`, `offense_pct`, `team`, `week`, `season`.
- `pbp` — play-by-play with context for situational slices and dropbacks: `qb_dropback`, `pass`, `rush`, `receiver_player_id`, `rusher_player_id`, `posteam`, `defteam`, `down`, `ydstogo`, `yardline_100`, `air_yards`, `play_action`, `season`, `season_type`, `week`, `game_id`.
- `ngs_weekly` (optional) — per player-week Next Gen Stats. Needed for `routes_run`. If unavailable/disabled, route-based metrics are `NULL`.

Notes:
- DuckDB is the target engine (see repo examples). All queries follow the `WITH params AS (...)` style to work with `scripts/run_query.sh`.

### Metrics (definitions)
- Snap share: percentage of team offensive snaps for the player.
  - Source: `snap_counts.offense_pct` averaged per week or recomputed as `offense_snaps / team_offense_snaps`.
- Route participation (WR/TE/RB in routes): `routes_run / team_dropbacks`.
  - Source: `ngs_weekly.routes_run` and team dropbacks from `pbp` where `qb_dropback=1`.
- Targets per route run (TPRR): `targets / routes_run`.
- Yards per route run (YPRR): `receiving_yards / routes_run`.
- Target share: from `weekly.target_share`.
- Air yards share: from `weekly.air_yards_share`.
- WOPR: from `weekly.wopr`.
- End-zone targets and share: count of targets where `yardline_100 - air_yards <= 0`, plus share vs team.
- Red-zone targets and share: targets with `yardline_100 <= 20` (also include inside-10 and inside-5 variants).
- Situational target shares:
  - LDD targets share: `down IN (3,4) AND ydstogo >= 5`.
  - SDD targets share: `down IN (1,2,3,4) AND ydstogo <= 2`.
  - 3rd/4th down target share: `down IN (3,4)`.
  - Two-minute target share: `half_seconds_remaining <= 120`.
  - Four-minute target share: `half_seconds_remaining <= 240`.
  - Play-action target share: `play_action = TRUE`.
- RB opportunity/carry shares:
  - Carry share: `player_carries / team_carries`.
  - Goal-line carry share: carries with `yardline_100 <= 5` over team carries inside 5.
  - Red-zone carry shares: inside-10 and inside-20 analogs.

First-read target share is not included due to inconsistent upstream availability in `pbp`. If added later, compute `first_read_targets / team_first_read_targets` using a per-play boolean.

### Additional advanced concepts to include (edge for DFS)
- aDOT and explosive rates:
  - Player aDOT: `SUM(air_yards)/NULLIF(targets,0)` from PBP target events.
  - Explosive reception rate: `COUNT(receptions with yards_gained>=20)/NULLIF(receptions,0)`; explosive target rate uses targets as denom.
  - Explosive rush rates at 10+ and 15+ yards for RBs.
- First downs per route run (1D/RR): `receiving_first_downs / routes_run`.
- High-value touches (HVT): `receptions + carries (yardline_100 <= 10)`; track counts and share vs team HVT.
- Neutral situation pace and PROE context joins (team-level):
  - Use existing league/team aggregates (neutral sec/play, no_huddle, shotgun, PROE) to annotate utilization rows with game environment.
- Personnel grouping splits:
  - Offensive personnel from `pbp.offense_personnel` (e.g., `11`, `12`, `21`); compute player target shares within each grouping per week/season.
- Formation/tempo splits:
  - No-huddle target share; shotgun target share using `no_huddle` and `shotgun` flags.
- Play-action efficiency overlay:
  - Play-action target share already above; optionally compute YPRR on play-action vs non-PA using `air_yards` and `routes_run`.
- xFP-lite (expected fantasy points proxy):
  - Receiving xFP: weight targets by down/yardline/air_yards buckets (open-source nflfastR style) to approximate expected points.
  - Rushing xFP: weight carries by yardline buckets (20/10/5) to capture TD expectation.

### Research-driven novel concepts (from Grok Util Report)
- Coverage splits (optional future integration): YPRR/TPRR vs man/zone and vs press. Requires external charting/NGS coverage dataset; add as optional source and compute per player-week and season splits.
- Opponent-adjusted efficiency context: join opponent defensive strength using DVOA/DYAR (if licensed) or EPA-based opponent adjustments as a proxy.
- Stacking and correlation analytics: compute correlation of teammate scoring (e.g., QB–WR, WR–TE) using weekly fantasy points.
- Ownership leverage (optional integration): If DFS ownership/salary feeds are available, compute leverage metrics (projected points vs ownership, salary-adjusted values) to guide tournament decisions.
- ML-ready feature marts: export a wide feature table (per player-week) combining utilization, context, and matchup features for downstream predictive models.

### New gold tables to materialize
1) `data/gold/utilization/team_week_context/` (team-level denominators)
- Keys: `(season, week, season_type, team)`
- Columns:
  - `team_offense_snaps` — from `snap_counts` (max per team-game/week)
  - `team_dropbacks` — from `pbp` (`COUNT(*) WHERE qb_dropback=1 AND posteam=team`)
  - `team_pass_attempts` — from `pbp` (`COUNT(*) WHERE pass=1`)
  - `team_carries` — from `pbp` (`COUNT(*) WHERE rush=1`)
  - `proe_neutral` — pass rate over expected in neutral situations using `xpass` where available
  - `sec_per_play_neutral` — neutral-pace seconds per play proxy
  - Note: initial scope materializes only for the active `:season`.

2) `data/gold/utilization/player_week_utilization/`
- Grain/keys: `(season, week, season_type, player_id, team)`
- Selected columns:
  - Identity: `player_name`, `position`
  - Snap metrics: `snap_share`
  - Route metrics (optional if NGS disabled): `routes_run`, `route_participation`, `tprr`, `yprr`
  - Volume/market-share: `targets`, `target_share`, `receiving_yards`, `receiving_air_yards`, `air_yards_share`, `wopr`
  - Receiving situational: `end_zone_targets`, `end_zone_target_share`, `rz20_targets`, `rz20_target_share`, `rz10_targets`, `rz10_target_share`, `rz5_targets`, `rz5_target_share`, `ldd_target_share`, `sdd_target_share`, `third_fourth_down_target_share`, `two_minute_target_share`, `play_action_target_share`
  - RB rushing situational: `carry_share`, `rz20_carry_share`, `rz10_carry_share`, `rz5_carry_share`

3) `data/gold/utilization/player_season_utilization/`
- Grain/keys: `(season, season_type, player_id, team)`
- Aggregates across weeks for all columns that meaningfully roll up (e.g., averages for shares, sums for counts).

4) `data/gold/utilization/player_pair_correlation/` (stacking insights)
- Grain/keys: `(season, season_type, team, pair_type, player_id_a, player_id_b)`
- Columns: `games`, `corr_dk_points`, `cov_dk_points`, `mean_a`, `mean_b`
- Use `data/gold/player_week_fantasy` as the scoring source (DK PPR), constrained to teammates or game-level pairs.

5) `data/gold/player_week_fantasy/` (DK PPR scoring)
- Grain/keys: `(season, week, season_type, player_id, team)`
- Columns: `dk_ppr_points` (and optionally scoring components), computed using DraftKings PPR rules defined in `catalog/draftkings/bestball.yml` (including yardage bonuses); constrained to the current season.

6) `data/gold/utilization/ml_feature_mart/` (optional)
- Grain/keys: `(season, week, season_type, player_id)`
- Wide feature table combining utilization (from player_week), team context (from team_week_context), explosive/adot/personnel/tempo splits, opponent aggregates, and optional ownership/salary if present.

All above gold datasets will be written only for the current `:season` during in-season runs.

### SQL templates (DuckDB)

#### Team week context (denominators)
```sql
-- queries/utilization/team_week_context.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), snaps AS (
  SELECT season, week, team,
         MAX(offense_snaps) AS team_offense_snaps
  FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
  GROUP BY season, week, team
), pbp AS (
  SELECT year AS season, week, season_type, posteam AS team,
         SUM(CASE WHEN qb_dropback=1 THEN 1 ELSE 0 END) AS team_dropbacks,
         SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END)        AS team_pass_attempts,
         SUM(CASE WHEN rush=1 THEN 1 ELSE 0 END)        AS team_carries
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
  GROUP BY season, week, season_type, team
)
SELECT p.season, p.week, p.season_type, p.team,
       COALESCE(s.team_offense_snaps, NULL) AS team_offense_snaps,
       p.team_dropbacks,
       p.team_pass_attempts,
       p.team_carries
FROM pbp p
LEFT JOIN snaps s
  ON s.season=p.season AND s.week=p.week AND s.team=p.team;
```

Usage (current season example):
```bash
scripts/run_query.sh -f queries/utilization/team_week_context.sql -s $(date +%Y) -t REG -- -csv
```

#### Team PROE and neutral pace
```sql
-- queries/utilization/team_proe_and_pace_by_week.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), base AS (
  SELECT year AS season, week, season_type, game_id, posteam AS team,
         pass::INT AS is_pass,
         xpass,
         no_huddle::INT AS is_no_huddle,
         shotgun::INT AS is_shotgun,
         half_seconds_remaining
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND qb_dropback = 1
    AND half_seconds_remaining > 120  -- approximate neutral, exclude hurry-up
)
SELECT season, week, season_type, team,
       AVG(is_pass) - AVG(xpass) AS proe_neutral,
       -- neutral pace proxy: inverse of plays per half-minute scaled to seconds
       30.0 / NULLIF(COUNT(*),0) * 60.0 AS sec_per_play_neutral
FROM base
GROUP BY season, week, season_type, team
ORDER BY season, week, team;
```

Usage (current season example):
```bash
scripts/run_query.sh -f queries/utilization/team_proe_and_pace_by_week.sql -s $(date +%Y) -t REG -- -csv
```

#### Routes run by player-week (NGS optional)
```sql
-- queries/utilization/routes_run_by_player_week.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
)
SELECT season, week, player_id,
       MAX(player_name) AS player_name,
       MAX(team) AS team,
       MAX(position) AS position,
       SUM(COALESCE(routes_run,0)) AS routes_run
FROM read_parquet('data/silver/ngs_weekly/season=*/**/*.parquet', union_by_name=true)
WHERE season = (SELECT season FROM params)
  AND stat_type = 'receiving'
GROUP BY season, week, player_id;
```

#### Receiving target events (per player-week from PBP)
```sql
-- queries/utilization/receiving_events_by_player_week.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), plays AS (
  SELECT year AS season, week, season_type, game_id, posteam AS team,
         receiver_player_id AS player_id,
         air_yards, yardline_100, play_action,
         down, ydstogo,
         half_seconds_remaining,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
)
SELECT season, week, season_type, team, player_id,
       SUM(is_target)                                                    AS targets,
       SUM(CASE WHEN air_yards IS NOT NULL AND yardline_100 - air_yards <= 0 THEN 1 ELSE 0 END) AS end_zone_targets,
       SUM(CASE WHEN yardline_100 <= 20 AND is_target=1 THEN 1 ELSE 0 END) AS rz20_targets,
       SUM(CASE WHEN yardline_100 <= 10 AND is_target=1 THEN 1 ELSE 0 END) AS rz10_targets,
       SUM(CASE WHEN yardline_100 <=  5 AND is_target=1 THEN 1 ELSE 0 END) AS rz5_targets,
       SUM(CASE WHEN down IN (3,4) AND is_target=1 THEN 1 ELSE 0 END)      AS third_fourth_down_targets,
       SUM(CASE WHEN down IN (3,4) AND ydstogo >= 5 AND is_target=1 THEN 1 ELSE 0 END) AS ldd_targets,
       SUM(CASE WHEN down IN (1,2,3,4) AND ydstogo <= 2 AND is_target=1 THEN 1 ELSE 0 END) AS sdd_targets,
       SUM(CASE WHEN half_seconds_remaining <= 120 AND is_target=1 THEN 1 ELSE 0 END) AS two_minute_targets,
       SUM(CASE WHEN half_seconds_remaining <= 240 AND is_target=1 THEN 1 ELSE 0 END) AS four_minute_targets,
       SUM(CASE WHEN play_action=TRUE AND is_target=1 THEN 1 ELSE 0 END) AS play_action_targets
FROM plays
GROUP BY season, week, season_type, team, player_id;
```

#### Rushing carry events (per player-week from PBP)
```sql
-- queries/utilization/rushing_events_by_player_week.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
)
SELECT year AS season, week, season_type, posteam AS team,
       rusher_player_id AS player_id,
       COUNT(*) AS carries,
       SUM(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END) AS rz20_carries,
       SUM(CASE WHEN yardline_100 <= 10 THEN 1 ELSE 0 END) AS rz10_carries,
       SUM(CASE WHEN yardline_100 <=  5 THEN 1 ELSE 0 END) AS rz5_carries
FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
WHERE year = (SELECT season FROM params)
  AND season_type = (SELECT season_type FROM params)
  AND rush = 1
GROUP BY season, week, season_type, team, player_id;
```

#### Explosive plays and aDOT (per player-week)
```sql
-- queries/utilization/explosive_and_adot_by_player_week.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), base AS (
  SELECT year AS season, week, season_type, posteam AS team,
         receiver_player_id AS player_id,
         CASE WHEN pass=1 AND complete_pass=1 THEN 1 ELSE 0 END AS is_reception,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target,
         yards_gained, air_yards
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
)
SELECT season, week, season_type, team, player_id,
       SUM(is_target) AS targets,
       SUM(CASE WHEN is_reception=1 THEN 1 ELSE 0 END) AS receptions,
       AVG(NULLIF(air_yards, NULL)) AS adot,
       SUM(CASE WHEN is_reception=1 AND yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_rec_20,
       SUM(CASE WHEN is_target=1    AND yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_tgt_20
FROM base
GROUP BY season, week, season_type, team, player_id;
```

#### Personnel grouping target shares (per player-week)
```sql
-- queries/utilization/personnel_target_shares.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
)
, plays AS (
  SELECT year AS season, week, season_type, posteam AS team,
         offense_personnel,
         receiver_player_id AS player_id,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND offense_personnel IS NOT NULL
)
, team_by_group AS (
  SELECT season, week, season_type, team, offense_personnel,
         SUM(is_target) AS team_targets
  FROM plays
  GROUP BY 1,2,3,4,5
)
SELECT p.season, p.week, p.season_type, p.team, p.offense_personnel,
       p.player_id,
       SUM(p.is_target) AS targets,
       SUM(p.is_target)::DOUBLE / NULLIF(MAX(t.team_targets),0) AS target_share_in_personnel
FROM plays p
JOIN team_by_group t
  ON t.season=p.season AND t.week=p.week AND t.season_type=p.season_type AND t.team=p.team AND t.offense_personnel=p.offense_personnel
GROUP BY 1,2,3,4,5,6;
```

#### Formation/tempo target shares (per player-week)
```sql
-- queries/utilization/formation_tempo_target_shares.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), plays AS (
  SELECT year AS season, week, season_type, posteam AS team,
         receiver_player_id AS player_id,
         shotgun::INT AS is_shotgun,
         no_huddle::INT AS is_no_huddle,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
)
, team AS (
  SELECT season, week, season_type, team,
         SUM(is_target) AS team_targets,
         SUM(CASE WHEN is_shotgun=1 THEN is_target ELSE 0 END) AS team_targets_shotgun,
         SUM(CASE WHEN is_no_huddle=1 THEN is_target ELSE 0 END) AS team_targets_no_huddle
  FROM plays
  GROUP BY 1,2,3,4
)
SELECT p.season, p.week, p.season_type, p.team, p.player_id,
       SUM(CASE WHEN p.is_shotgun=1 THEN p.is_target ELSE 0 END)::DOUBLE / NULLIF(MAX(t.team_targets_shotgun),0) AS shotgun_target_share,
       SUM(CASE WHEN p.is_no_huddle=1 THEN p.is_target ELSE 0 END)::DOUBLE / NULLIF(MAX(t.team_targets_no_huddle),0) AS no_huddle_target_share
FROM plays p
JOIN team t ON t.season=p.season AND t.week=p.week AND t.season_type=p.season_type AND t.team=p.team
GROUP BY 1,2,3,4,5;
```

#### xFP-lite (expected fantasy points proxy)
```sql
-- queries/utilization/xfp_lite_by_player_week.sql
-- We approximate expected points by summing weights from coarse buckets.
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), events AS (
  SELECT year AS season, week, season_type, posteam AS team,
         receiver_player_id AS rec_id,
         rusher_player_id   AS rush_id,
         pass, rush, complete_pass,
         yardline_100, air_yards
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
)
, recv AS (
  SELECT season, week, season_type, team, rec_id AS player_id,
         SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS targets,
         -- coarse weights: deep targets (ay>=15) worth ~0.6 pts, short targets ~0.4
         SUM(CASE WHEN pass=1 AND air_yards >= 15 THEN 0.6 WHEN pass=1 THEN 0.4 ELSE 0 END) AS xfp_rec
  FROM events
  GROUP BY 1,2,3,4,5
)
, rush AS (
  SELECT season, week, season_type, team, rush_id AS player_id,
         COUNT(*) AS carries,
         -- goal-to-go carries are weighted higher
         SUM(CASE WHEN rush=1 AND yardline_100 <= 5 THEN 0.8 WHEN rush=1 AND yardline_100 <= 10 THEN 0.5 WHEN rush=1 AND yardline_100 <= 20 THEN 0.3 ELSE 0.15 END) AS xfp_rush
  FROM events
  WHERE rush=1
  GROUP BY 1,2,3,4,5
)
SELECT COALESCE(r.season, v.season) AS season,
       COALESCE(r.week, v.week) AS week,
       COALESCE(r.season_type, v.season_type) AS season_type,
       COALESCE(r.team, v.team) AS team,
       COALESCE(r.player_id, v.player_id) AS player_id,
       COALESCE(r.xfp_rush, 0) + COALESCE(v.xfp_rec, 0) AS xfp_lite
FROM rush r
FULL OUTER JOIN recv v USING (season, week, season_type, team, player_id);
```

#### Player pair correlation (stacking)
```sql
-- queries/utilization/player_pair_correlation.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type,
         CAST(:team AS VARCHAR) AS team
), src AS (
  SELECT season, week, season_type, team, player_id, dk_ppr_points
  FROM read_parquet('data/gold/player_week_fantasy/season=*/week=*/**/*.parquet', union_by_name=true)
  WHERE season=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND team=(SELECT team FROM params)
)
, pairs AS (
  SELECT a.season, a.season_type, a.team,
         a.player_id AS player_id_a,
         b.player_id AS player_id_b,
         a.dk_ppr_points AS dk_a,
         b.dk_ppr_points AS dk_b
  FROM src a
  JOIN src b USING (season, week, season_type, team)
  WHERE a.player_id < b.player_id
)
SELECT season, season_type, team, 'teammates' AS pair_type,
       player_id_a, player_id_b,
       COUNT(*) AS games,
       AVG(dk_a) AS mean_a,
       AVG(dk_b) AS mean_b,
       COVAR_POP(dk_a, dk_b) AS cov_dk_points,
       CORR(dk_a, dk_b) AS corr_dk_points
FROM pairs
GROUP BY 1,2,3,4,5,6
ORDER BY corr_dk_points DESC, games DESC;
```

#### Ownership leverage (optional, requires ownership feed)
```sql
-- queries/utilization/ownership_leverage.sql
-- Requires external parquet: data/silver/dfs_ownership/season=*/**/*.parquet with
-- (season, week, slate, player_id, projected_ownership)
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:week AS INTEGER) AS week
), pts AS (
  SELECT season, week, player_id, dk_ppr_points AS proj_points
  FROM read_parquet('data/gold/player_week_fantasy/season=*/week=*/**/*.parquet', union_by_name=true)
  WHERE season=(SELECT season FROM params) AND week=(SELECT week FROM params)
), own AS (
  SELECT season, week, player_id, MAX(projected_ownership) AS proj_own
  FROM read_parquet('data/silver/dfs_ownership/season=*/**/*.parquet', union_by_name=true)
  WHERE season=(SELECT season FROM params) AND week=(SELECT week FROM params)
  GROUP BY 1,2,3
)
SELECT p.season, p.week, p.player_id,
       p.proj_points,
       o.proj_own,
       p.proj_points * (1.0 - COALESCE(o.proj_own,0)) AS leverage_score
FROM pts p
LEFT JOIN own o USING (season, week, player_id)
ORDER BY leverage_score DESC;
```

#### Player-week utilization mart
```sql
-- queries/utilization/player_week_utilization.sql
WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season,
         CAST(:season_type AS VARCHAR) AS season_type
), w AS (
  SELECT * FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
), sc AS (
  SELECT season, week, team, player_id,
         AVG(offense_pct) AS snap_share
  FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
  GROUP BY season, week, team, player_id
), ctx AS (
  SELECT * FROM (
    -- inline team_week_context query
    WITH snaps AS (
      SELECT season, week, team, MAX(offense_snaps) AS team_offense_snaps
      FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
      WHERE season = (SELECT season FROM params)
      GROUP BY season, week, team
    ), pbp AS (
      SELECT year AS season, week, season_type, posteam AS team,
             SUM(CASE WHEN qb_dropback=1 THEN 1 ELSE 0 END) AS team_dropbacks,
             SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END)        AS team_pass_attempts,
             SUM(CASE WHEN rush=1 THEN 1 ELSE 0 END)        AS team_carries
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params)
        AND season_type = (SELECT season_type FROM params)
      GROUP BY season, week, season_type, team
    )
    SELECT p.season, p.week, p.season_type, p.team,
           s.team_offense_snaps, p.team_dropbacks, p.team_pass_attempts, p.team_carries
    FROM pbp p LEFT JOIN snaps s
      ON s.season=p.season AND s.week=p.week AND s.team=p.team
  )
), rr AS (
  -- NGS routes (optional). If table absent, this CTE can be replaced with SELECT ... NULLs.
  SELECT season, week, player_id, team,
         SUM(routes_run) AS routes_run
  FROM read_parquet('data/silver/ngs_weekly/season=*/**/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND stat_type = 'receiving'
  GROUP BY season, week, player_id, team
), rec_ev AS (
  SELECT * FROM (
    -- inline receiving_events_by_player_week
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS player_id,
             air_yards, yardline_100, play_action,
             down, ydstogo,
             half_seconds_remaining,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params)
        AND season_type = (SELECT season_type FROM params)
    )
    SELECT season, week, season_type, team, player_id,
           SUM(is_target) AS targets,
           SUM(CASE WHEN air_yards IS NOT NULL AND yardline_100 - air_yards <= 0 THEN 1 ELSE 0 END) AS end_zone_targets,
           SUM(CASE WHEN yardline_100 <= 20 AND is_target=1 THEN 1 ELSE 0 END) AS rz20_targets,
           SUM(CASE WHEN yardline_100 <= 10 AND is_target=1 THEN 1 ELSE 0 END) AS rz10_targets,
           SUM(CASE WHEN yardline_100 <=  5 AND is_target=1 THEN 1 ELSE 0 END) AS rz5_targets,
           SUM(CASE WHEN down IN (3,4) AND is_target=1 THEN 1 ELSE 0 END) AS third_fourth_down_targets,
           SUM(CASE WHEN down IN (3,4) AND ydstogo >= 5 AND is_target=1 THEN 1 ELSE 0 END) AS ldd_targets,
           SUM(CASE WHEN down IN (1,2,3,4) AND ydstogo <= 2 AND is_target=1 THEN 1 ELSE 0 END) AS sdd_targets,
           SUM(CASE WHEN half_seconds_remaining <= 120 AND is_target=1 THEN 1 ELSE 0 END) AS two_minute_targets,
           SUM(CASE WHEN half_seconds_remaining <= 240 AND is_target=1 THEN 1 ELSE 0 END) AS four_minute_targets,
           SUM(CASE WHEN play_action=TRUE AND is_target=1 THEN 1 ELSE 0 END) AS play_action_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  )
), rush_ev AS (
  SELECT year AS season, week, season_type, posteam AS team,
         rusher_player_id AS player_id,
         COUNT(*) AS carries,
         SUM(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END) AS rz20_carries,
         SUM(CASE WHEN yardline_100 <= 10 THEN 1 ELSE 0 END) AS rz10_carries,
         SUM(CASE WHEN yardline_100 <=  5 THEN 1 ELSE 0 END) AS rz5_carries
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND rush = 1
  GROUP BY season, week, season_type, team, player_id
)
SELECT
  w.season, w.week, w.season_type,
  w.player_id, MAX(w.player_name) AS player_name, MAX(w.position) AS position,
  COALESCE(w.team, sc.team) AS team,
  -- Snap share
  MAX(sc.snap_share) AS snap_share,
  -- Routes and participation (nullable if NGS disabled)
  MAX(rr.routes_run) AS routes_run,
  CASE WHEN MAX(ctx.team_dropbacks) > 0 THEN MAX(rr.routes_run)::DOUBLE / NULLIF(MAX(ctx.team_dropbacks),0) END AS route_participation,
  -- TPRR/YPRR (nullable if routes are NULL)
  CASE WHEN SUM(COALESCE(rr.routes_run,0)) > 0 THEN SUM(COALESCE(w.targets,0))::DOUBLE / NULLIF(SUM(COALESCE(rr.routes_run,0)),0) END AS tprr,
  CASE WHEN SUM(COALESCE(rr.routes_run,0)) > 0 THEN SUM(COALESCE(w.receiving_yards,0))::DOUBLE / NULLIF(SUM(COALESCE(rr.routes_run,0)),0) END AS yprr,
  -- Market shares (from weekly)
  SUM(COALESCE(w.targets,0)) AS targets,
  AVG(NULLIF(w.target_share, NULL)) AS target_share,
  SUM(COALESCE(w.receiving_yards,0)) AS receiving_yards,
  SUM(COALESCE(w.receiving_air_yards,0)) AS receiving_air_yards,
  AVG(NULLIF(w.air_yards_share, NULL)) AS air_yards_share,
  AVG(NULLIF(w.wopr, NULL)) AS wopr,
  -- Receiving situational (shares vs team)
  SUM(COALESCE(rec_ev.end_zone_targets,0)) AS end_zone_targets,
  SUM(COALESCE(rec_ev.rz20_targets,0)) AS rz20_targets,
  SUM(COALESCE(rec_ev.rz10_targets,0)) AS rz10_targets,
  SUM(COALESCE(rec_ev.rz5_targets,0))  AS rz5_targets,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.end_zone_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS end_zone_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz20_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz20_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz10_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz10_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz5_targets,0))::DOUBLE  / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz5_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.ldd_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS ldd_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.sdd_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS sdd_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.third_fourth_down_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS third_fourth_down_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.two_minute_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS two_minute_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.four_minute_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS four_minute_target_share,
  CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.play_action_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS play_action_target_share,
  -- RB rushing situational shares vs team carries
  CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.carries,0))::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS carry_share,
  CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz20_carries,0))::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz20_carry_share,
  CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz10_carries,0))::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz10_carry_share,
  CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz5_carries,0))::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS rz5_carry_share
FROM w
LEFT JOIN sc    ON sc.season=w.season AND sc.week=w.week AND sc.team=COALESCE(w.team, sc.team) AND sc.player_id=w.player_id
LEFT JOIN ctx   ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=COALESCE(w.team, ctx.team)
LEFT JOIN rr    ON rr.season=w.season AND rr.week=w.week AND rr.player_id=w.player_id
LEFT JOIN rec_ev ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=COALESCE(w.team, rec_ev.team) AND rec_ev.player_id=w.player_id
LEFT JOIN rush_ev ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.team=COALESCE(w.team, rush_ev.team) AND rush_ev.player_id=w.player_id
GROUP BY 1,2,3,4,7
ORDER BY season, week, position, player_name;
```

To materialize as Parquet (partitioned):
```sql
COPY (
  SELECT * FROM (
    -- Include the full SELECT from player_week_utilization.sql here
  )
) TO 'data/gold/utilization/player_week/season=' || CAST(season AS VARCHAR) || '/week=' || CAST(week AS VARCHAR) || '/part.parquet' (FORMAT PARQUET);
```

Materialize current season only:
```bash
duckdb -c "\i queries/utilization/player_week_utilization.sql"  # with -s $(date +%Y)
```

#### Player-season utilization (rollup)
```sql
-- queries/utilization/player_season_utilization.sql
WITH src AS (
  SELECT *
  FROM read_parquet('data/gold/utilization/player_week/season=*/week=*/**/*.parquet', union_by_name=true)
)
SELECT
  season, season_type, player_id,
  MAX(player_name) AS player_name,
  MAX(position)    AS position,
  MAX(team)        AS primary_team,
  COUNT(*)         AS games,
  AVG(snap_share)  AS snap_share,
  SUM(COALESCE(routes_run,0)) AS routes_run,
  AVG(NULLIF(route_participation, NULL)) AS route_participation,
  AVG(NULLIF(tprr, NULL)) AS tprr,
  AVG(NULLIF(yprr, NULL)) AS yprr,
  SUM(targets) AS targets,
  AVG(NULLIF(target_share, NULL)) AS target_share,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receiving_air_yards) AS receiving_air_yards,
  AVG(NULLIF(air_yards_share, NULL)) AS air_yards_share,
  AVG(NULLIF(wopr, NULL)) AS wopr,
  SUM(end_zone_targets) AS end_zone_targets,
  AVG(NULLIF(end_zone_target_share, NULL)) AS end_zone_target_share,
  SUM(rz20_targets) AS rz20_targets,
  AVG(NULLIF(rz20_target_share, NULL)) AS rz20_target_share,
  SUM(rz10_targets) AS rz10_targets,
  AVG(NULLIF(rz10_target_share, NULL)) AS rz10_target_share,
  SUM(rz5_targets)  AS rz5_targets,
  AVG(NULLIF(rz5_target_share, NULL))  AS rz5_target_share,
  AVG(NULLIF(ldd_target_share, NULL))  AS ldd_target_share,
  AVG(NULLIF(sdd_target_share, NULL))  AS sdd_target_share,
  AVG(NULLIF(third_fourth_down_target_share, NULL)) AS third_fourth_down_target_share,
  AVG(NULLIF(two_minute_target_share, NULL)) AS two_minute_target_share,
  AVG(NULLIF(four_minute_target_share, NULL)) AS four_minute_target_share,
  AVG(NULLIF(play_action_target_share, NULL)) AS play_action_target_share,
  AVG(NULLIF(carry_share, NULL)) AS carry_share,
  AVG(NULLIF(rz20_carry_share, NULL)) AS rz20_carry_share,
  AVG(NULLIF(rz10_carry_share, NULL)) AS rz10_carry_share,
  AVG(NULLIF(rz5_carry_share, NULL))  AS rz5_carry_share
FROM src
GROUP BY 1,2,3
ORDER BY season, position, player_name;
```

### Dependencies and notes
- If `ngs_weekly` is disabled, create a stub for `routes_run_by_player_week.sql` that yields `routes_run=NULL` so downstream columns become `NULL`.
- `weekly` already includes `target_share`, `air_yards_share`, and `wopr`, so we reuse them rather than recomputing.
- End-zone target logic uses `yardline_100 - air_yards <= 0` as a practical approximation when an explicit flag is not present.
- Two-minute is approximated with `half_seconds_remaining <= 120`.
- Coverage splits, DVOA/DYAR, and ownership leverage require optional external datasets; integrate when available and mark columns nullable when absent.
 - In-season exclusions: injuries (no 2025 feed) and post-2023 FTN participation (available only after the season) are excluded; any metrics requiring them are omitted or left NULL.
 - xFP-lite weights can be parameterized via a small YAML; defaults used in the provided query are constants (0.6/0.4 for targets; 0.8/0.5/0.3/0.15 for rush).

### Deliverables
- New gold tables populated under:
  - `data/gold/utilization/team_week_context/`
  - `data/gold/utilization/player_week/`
  - `data/gold/utilization/player_season/`
  - `data/gold/utilization/player_pair_correlation/`
  - `data/gold/utilization/ml_feature_mart/` (optional)
- Checked-in query templates under `queries/utilization/` for reproducibility.
 - In-season default: only the current `:season` is materialized and queried.

### Success criteria
- Functional: metric correctness (spot-checks <1% error), coverage (2016+ full for routes; older seasons with approximations), query performance (<5s single-season single-player; <30s multi-season aggregates), reproducibility, automated validation of ranges and nulls, and advanced metric coverage when optional feeds exist.
- Non-functional: seamless ETL integration, scalability with partitioning, maintainable code and schemas, comprehensive documentation, and tests.
- Acceptance: unit tests for metric math, integration tests on 2024 subset, parity with public reports for a sample of players, successful CLI runs (`bootstrap`, `update`, `promote`).

### Current state and gaps (summary)
- Silver datasets available: `pbp`, `weekly`, `snap_counts`, `rosters[_seasonal]`, `schedules`, with optional `ngs_weekly`.
- Gaps: Gold marts not yet materialized; coverage/ownership feeds optional; some metrics require approximations.

### Implementation plan (high-level)
- Add Gold-layer marts for team context, player-week/season utilization, pair correlations, and optional ML feature mart.
- Extend `promote` to materialize Gold tables from Silver deltas; add transforms and schemas.
- Add the queries in this document; wire into `scripts/run_query.sh` parameterization.
- Profile and validate outputs; document usage in `DATA_USAGE_GUIDE.md`.
 - In-season constraint: scope CLI and transforms to `--season <current>`, materializing only current-season partitions.
 - Orchestration: automatically run Gold materialization at the end of `update --season <current>`; detect the active season when not explicitly provided (default to latest ingested season for queries).

### Out-of-scope for this phase
- Historical multi-season backfills and longitudinal roster reconciliation.
- Coverage splits, ownership leverage, and DVOA/DYAR unless those optional data feeds are available.


