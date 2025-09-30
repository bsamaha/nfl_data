-- Team defensive efficiency by week (EPA/play allowed, success rate allowed)
-- Usage: duckdb -c "\i queries/team_defense_epa_by_week.sql"
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
  defteam AS team,
  week,
  COUNT(*) AS plays,
  SUM(epa) / NULLIF(COUNT(*), 0) AS epa_per_play_allowed,
  AVG(success) AS success_rate_allowed,
  AVG(pass) AS opponent_pass_rate,
  AVG(rush) AS opponent_rush_rate
FROM plays
WHERE defteam IS NOT NULL
GROUP BY 1,2
ORDER BY week, epa_per_play_allowed ASC;


