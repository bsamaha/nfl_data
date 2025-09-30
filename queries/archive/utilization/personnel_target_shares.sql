-- queries/utilization/personnel_target_shares.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), plays AS (
  SELECT year AS season, week, season_type, posteam AS team,
         offense_personnel,
         receiver_player_id AS player_id,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND offense_personnel IS NOT NULL
), team_by_group AS (
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


