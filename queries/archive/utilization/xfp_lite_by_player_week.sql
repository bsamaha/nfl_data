-- queries/utilization/xfp_lite_by_player_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), events AS (
  SELECT year AS season, week, season_type, posteam AS team,
         receiver_player_id AS rec_id,
         rusher_player_id   AS rush_id,
         pass, rush, complete_pass,
         yardline_100, air_yards
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
), recv AS (
  SELECT season, week, season_type, team, rec_id AS player_id,
         SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS targets,
         SUM(CASE WHEN pass=1 AND air_yards >= 15 THEN 0.6 WHEN pass=1 THEN 0.4 ELSE 0 END) AS xfp_rec
  FROM events
  GROUP BY 1,2,3,4,5
), rush AS (
  SELECT season, week, season_type, team, rush_id AS player_id,
         COUNT(*) AS carries,
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


