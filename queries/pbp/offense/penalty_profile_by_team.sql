-- Penalty counts and yards by type for each team
-- Usage: duckdb -c "\i queries/penalty_profile_by_team.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND penalty = 1
)
SELECT
  COALESCE(penalty_team, posteam, defteam) AS team,
  penalty_type,
  COUNT(*) AS penalties,
  SUM(COALESCE(penalty_yards,0)) AS penalty_yards
FROM plays
GROUP BY 1,2
ORDER BY team, penalties DESC;


