-- Punt efficiency (inside 20, touchbacks, returns) by team
-- Usage: duckdb -c "\i queries/punt_efficiency.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND punt_attempt = 1
)
SELECT
  posteam AS team,
  COUNT(*) AS punts,
  SUM(CASE WHEN punt_inside_twenty = 1 THEN 1 ELSE 0 END) AS inside_20,
  SUM(CASE WHEN punt_in_endzone = 1 OR touchback = 1 THEN 1 ELSE 0 END) AS touchbacks,
  SUM(CASE WHEN return_yards IS NOT NULL AND return_yards > 0 THEN 1 ELSE 0 END) AS returned,
  inside_20 / NULLIF(punts, 0) AS i20_rate,
  touchbacks / NULLIF(punts, 0) AS touchback_rate,
  returned / NULLIF(punts, 0) AS return_rate
FROM plays
WHERE posteam IS NOT NULL
GROUP BY 1
ORDER BY i20_rate DESC;


