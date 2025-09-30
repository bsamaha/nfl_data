-- Offensive personnel usage rates (e.g., 11, 12, 21)
-- Usage: duckdb -c "\i queries/personnel_usage_rates.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT posteam, week, offense_personnel, play
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND offense_personnel IS NOT NULL
)
SELECT
  posteam AS team,
  offense_personnel,
  COUNT(*) AS plays,
  100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY posteam) AS usage_pct
FROM plays
GROUP BY 1,2
ORDER BY team, usage_pct DESC;


