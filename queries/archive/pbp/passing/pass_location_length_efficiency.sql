-- Passing efficiency by length (short/middle/deep) and location (left/middle/right)
-- Usage: duckdb -c "\i queries/pass_location_length_efficiency.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT pass_length, pass_location, epa, success, pass, season
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND pass = 1
)
SELECT
  pass_length,
  pass_location,
  COUNT(*) AS attempts,
  AVG(success) AS success_rate,
  AVG(epa) AS epa_per_att
FROM plays
GROUP BY 1,2
ORDER BY pass_length, pass_location;


