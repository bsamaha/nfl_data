-- QB efficiency by season (EPA per dropback, CPOE)
-- Usage: duckdb -c "\i queries/qb_epa_cpoe_season.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
pbp AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1
    AND coalesce(aborted_play, 0) = 0
)
SELECT
  passer_player_id AS player_id,
  COALESCE(passer_player_name, passer) AS player_name,
  SUM(CASE WHEN qb_dropback = 1 THEN 1 ELSE 0 END) AS dropbacks,
  SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END) AS attempts,
  SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS completions,
  SUM(CASE WHEN sack = 1 THEN 1 ELSE 0 END) AS sacks,
  SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS interceptions,
  SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS pass_tds,
  SUM(epa) / NULLIF(SUM(CASE WHEN qb_dropback = 1 THEN 1 ELSE 0 END), 0) AS epa_per_db,
  AVG(CASE WHEN pass_attempt = 1 THEN cpoe END) AS avg_cpoe,
  AVG(CASE WHEN pass_attempt = 1 THEN cp END) AS avg_cp
FROM pbp
WHERE passer_player_id IS NOT NULL
GROUP BY 1,2
HAVING dropbacks >= 100
ORDER BY epa_per_db DESC
LIMIT 100;


