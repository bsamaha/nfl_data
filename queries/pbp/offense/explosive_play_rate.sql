-- Explosive play rates by team (20+ yard passes, 10+ yard rushes)
-- Usage: duckdb -c "\i queries/explosive_play_rate.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1
    AND COALESCE(aborted_play, 0) = 0
)
SELECT
  posteam AS team,
  COUNT(*) AS plays,
  SUM(CASE WHEN pass = 1 AND yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_passes,
  SUM(CASE WHEN rush = 1 AND yards_gained >= 10 THEN 1 ELSE 0 END) AS explosive_rushes,
  (explosive_passes + explosive_rushes) AS explosive_total,
  explosive_passes / NULLIF(COUNT(*), 0) AS explosive_pass_rate,
  explosive_rushes / NULLIF(COUNT(*), 0) AS explosive_rush_rate,
  (explosive_total) / NULLIF(COUNT(*), 0) AS explosive_play_rate
FROM plays
WHERE posteam IS NOT NULL
GROUP BY 1
ORDER BY explosive_play_rate DESC;


