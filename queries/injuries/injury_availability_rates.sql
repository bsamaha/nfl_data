-- Player availability: counts of questionable/doubtful/out by week
-- Usage: duckdb -c "\i queries/injury_availability_rates.sql"
WITH params AS (
  SELECT 2024 AS season
),
inj AS (
  SELECT * FROM read_parquet('data/silver/injuries/season=*/*.parquet')
  WHERE season = (SELECT season FROM params)
)
SELECT
  player_id,
  MAX(full_name) AS player_name,
  MAX(position) AS position,
  MAX(team) AS team,
  SUM(CASE WHEN LOWER(report_status) LIKE '%out%' THEN 1 ELSE 0 END) AS out_reports,
  SUM(CASE WHEN LOWER(report_status) LIKE '%doubt%' THEN 1 ELSE 0 END) AS doubtful_reports,
  SUM(CASE WHEN LOWER(report_status) LIKE '%question%' THEN 1 ELSE 0 END) AS questionable_reports
FROM inj
GROUP BY 1
ORDER BY out_reports DESC
LIMIT 200;


