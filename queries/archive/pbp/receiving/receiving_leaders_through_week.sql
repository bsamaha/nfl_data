-- Receiving leaders through a given week
-- Usage: duckdb -c "\i queries/receiving_leaders_through_week.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week, 'REG' AS season_type
),
pbp AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
agg AS (
  SELECT
    COALESCE(receiver_player_id, receiver_id) AS player_id,
    COALESCE(receiver_player_name, receiver) AS player_name,
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS rec,
    SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END) AS tgt,
    SUM(COALESCE(receiving_yards,0)) AS yds,
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS td,
    AVG(air_yards) AS avg_ay,
    SUM(CASE WHEN complete_pass = 1 THEN COALESCE(yards_after_catch,0) ELSE 0 END) AS yac
  FROM pbp
  WHERE COALESCE(receiver_player_id, receiver_id) IS NOT NULL
  GROUP BY 1,2
)
SELECT *
FROM agg
ORDER BY yds DESC
LIMIT 100;


