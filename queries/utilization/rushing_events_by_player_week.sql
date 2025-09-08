-- queries/utilization/rushing_events_by_player_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
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
GROUP BY year, week, season_type, posteam, rusher_player_id;


