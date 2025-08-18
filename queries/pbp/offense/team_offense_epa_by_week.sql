-- Team offensive efficiency by week (EPA/play, success rate, pass rate)
-- Usage: duckdb -c "\i queries/team_offense_epa_by_week.sql"
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
  week,
  COUNT(*) AS plays,
  SUM(epa) / NULLIF(COUNT(*), 0) AS epa_per_play,
  AVG(success) AS success_rate,
  AVG(pass) AS pass_rate,
  AVG(rush) AS rush_rate
FROM plays
WHERE posteam IS NOT NULL
GROUP BY 1,2
ORDER BY week, epa_per_play DESC;


