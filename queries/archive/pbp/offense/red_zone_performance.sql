-- Red zone performance by team (inside opponent 20)
-- Usage: duckdb -c "\i queries/red_zone_performance.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
rz AS (
  SELECT * FROM plays WHERE yardline_100 <= 20
)
SELECT
  posteam AS team,
  COUNT(*) AS rz_plays,
  SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END) AS rz_tds,
  SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_result = 'good' THEN 1 ELSE 0 END) AS made_fgs,
  AVG(success) AS rz_success_rate,
  SUM(epa) / NULLIF(COUNT(*), 0) AS rz_epa_per_play
FROM rz
WHERE posteam IS NOT NULL
GROUP BY 1
ORDER BY rz_epa_per_play DESC;


