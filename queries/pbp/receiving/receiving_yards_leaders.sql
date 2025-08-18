-- Receiving yards leaders through a given week (built from PBP)
-- Usage: duckdb -c "\i queries/receiving_yards_leaders.sql"
WITH params AS (
  SELECT 2024 AS season, 3 AS thru_week, 'REG' AS season_type
),
plays AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND pass = 1
)
SELECT
  COALESCE(receiver_player_id, receiver_id) AS player_id,
  MAX(COALESCE(receiver_player_name, receiver)) AS player_name,
  SUM(COALESCE(receiving_yards,0)) AS yds
FROM plays
WHERE COALESCE(receiver_player_id, receiver_id) IS NOT NULL
GROUP BY 1
ORDER BY yds DESC
LIMIT 50;

