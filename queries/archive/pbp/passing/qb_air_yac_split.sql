-- QB passing efficiency split into air and YAC components
-- Usage: duckdb -c "\i queries/qb_air_yac_split.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
)
SELECT
  passer_player_id AS player_id,
  COALESCE(passer_player_name, passer) AS player_name,
  SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END) AS attempts,
  SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS completions,
  SUM(air_epa) / NULLIF(SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END), 0) AS air_epa_per_att,
  SUM(yac_epa) / NULLIF(SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END), 0) AS yac_epa_per_att,
  SUM(epa) / NULLIF(SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END), 0) AS total_epa_per_att,
  AVG(air_yards) AS avg_air_yards,
  AVG(CASE WHEN complete_pass = 1 THEN yards_after_catch END) AS avg_yac_on_comp
FROM plays
WHERE passer_player_id IS NOT NULL
GROUP BY 1,2
HAVING attempts >= 100
ORDER BY total_epa_per_att DESC;


