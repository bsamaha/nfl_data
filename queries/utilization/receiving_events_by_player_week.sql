-- queries/utilization/receiving_events_by_player_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), plays AS (
  SELECT year AS season, week, season_type, game_id, posteam AS team,
         receiver_player_id AS player_id,
         air_yards, yardline_100,
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
       SUM(CASE WHEN half_seconds_remaining <= 240 AND is_target=1 THEN 1 ELSE 0 END) AS four_minute_targets
FROM plays
GROUP BY season, week, season_type, team, player_id;


