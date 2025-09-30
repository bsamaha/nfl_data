-- queries/utilization/explosive_and_adot_by_player_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
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


