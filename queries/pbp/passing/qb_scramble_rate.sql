-- QB scramble rate and efficiency by season
-- Usage: duckdb -c "\i queries/qb_scramble_rate.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
pbp AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1
    AND COALESCE(aborted_play, 0) = 0
)
SELECT
  passer_player_id AS player_id,
  COALESCE(passer_player_name, passer) AS player_name,
  SUM(CASE WHEN qb_dropback = 1 THEN 1 ELSE 0 END) AS dropbacks,
  SUM(CASE WHEN qb_scramble = 1 THEN 1 ELSE 0 END) AS scrambles,
  scrambles / NULLIF(dropbacks, 0) AS scramble_rate,
  SUM(CASE WHEN qb_scramble = 1 THEN epa ELSE 0 END) / NULLIF(scrambles, 0) AS epa_per_scramble,
  SUM(CASE WHEN qb_scramble = 1 THEN rushing_yards ELSE 0 END) / NULLIF(scrambles, 0) AS yds_per_scramble
FROM pbp
WHERE passer_player_id IS NOT NULL
GROUP BY 1,2
HAVING dropbacks >= 50 AND scrambles >= 10
ORDER BY scramble_rate DESC;


