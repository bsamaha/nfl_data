-- queries/utilization/player_week_fantasy.sql
-- DraftKings PPR (with yardage bonuses) per docs/catalog rules
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), base AS (
  SELECT w.season, w.week, w.season_type, w.team, w.player_id,
         w.receiving_yards, w.rushing_yards, w.passing_yards,
         w.receiving_tds, w.rushing_tds, w.passing_tds,
         w.receptions, w.interceptions, w.fumbles_lost,
         w.return_tds, w.two_pt_conversions
  FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true) w
  WHERE w.season=(SELECT season FROM params) AND w.season_type=(SELECT season_type FROM params)
)
SELECT season, week, season_type, player_id, MAX(team) AS team,
       -- Points
       COALESCE(
         6.0 * SUM(receiving_tds)
         + 0.1 * SUM(receiving_yards)
         + 3.0 * CASE WHEN MAX(receiving_yards) >= 100 THEN 1 ELSE 0 END
         + 6.0 * SUM(rushing_tds)
         + 0.1 * SUM(rushing_yards)
         + 3.0 * CASE WHEN MAX(rushing_yards) >= 100 THEN 1 ELSE 0 END
         + 4.0 * SUM(passing_tds)
         + 0.04 * SUM(passing_yards)
         + 3.0 * CASE WHEN MAX(passing_yards) >= 300 THEN 1 ELSE 0 END
         + 1.0 * SUM(receptions)
         - 1.0 * SUM(interceptions)
         - 1.0 * SUM(fumbles_lost)
         + 6.0 * SUM(return_tds)
         + 2.0 * SUM(two_pt_conversions)
       , 0) AS dk_ppr_points
FROM base
GROUP BY season, week, season_type, player_id;


