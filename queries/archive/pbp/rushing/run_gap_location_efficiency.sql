-- Rushing efficiency by run location and gap
-- Usage: duckdb -c "\i queries/run_gap_location_efficiency.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT run_location, run_gap, epa, success, rush
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND rush = 1
)
SELECT
  run_location,
  run_gap,
  COUNT(*) AS attempts,
  AVG(success) AS success_rate,
  AVG(epa) AS epa_per_rush
FROM plays
GROUP BY 1,2
ORDER BY run_location, run_gap;


