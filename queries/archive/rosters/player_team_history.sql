-- Player career team history (by seasons and weeks observed in rosters)
-- Usage: duckdb -c "\i queries/player_team_history.sql"
WITH rost AS (
  SELECT season, week, team, player_id, player_name
  FROM read_parquet('data/silver/rosters/season=*/*.parquet')
)
SELECT
  player_id,
  MAX(player_name) AS player_name,
  team,
  MIN(season) AS first_season,
  MAX(season) AS last_season,
  COUNT(DISTINCT season) AS seasons_seen,
  COUNT(DISTINCT CONCAT(CAST(season AS VARCHAR), '-', CAST(week AS VARCHAR))) AS weeks_seen
FROM rost
GROUP BY 1,3
ORDER BY player_name, first_season;


