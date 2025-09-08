-- queries/utilization/routes_run_by_player_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
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


