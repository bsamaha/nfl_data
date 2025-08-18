-- Third down conversion rates by team and distance bucket
-- Usage: duckdb -c "\i queries/third_down_conversion.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT posteam, CAST(down AS INT) AS down, ydstogo, first_down, pass, rush
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND CAST(down AS INT) = 3
)
SELECT
  posteam AS team,
  CASE
    WHEN ydstogo <= 2 THEN '1-2'
    WHEN ydstogo <= 5 THEN '3-5'
    WHEN ydstogo <= 8 THEN '6-8'
    WHEN ydstogo <= 12 THEN '9-12'
    ELSE '13+'
  END AS togo_bucket,
  COUNT(*) AS attempts,
  SUM(CASE WHEN first_down = 1 THEN 1 ELSE 0 END) AS conversions,
  conversions / NULLIF(attempts, 0) AS conversion_rate,
  AVG(pass) AS pass_rate
FROM plays
GROUP BY 1,2
ORDER BY team, togo_bucket;


