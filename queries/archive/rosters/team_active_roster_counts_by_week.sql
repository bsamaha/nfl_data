-- Active roster counts by team and week
-- Usage: duckdb -c "\i queries/team_active_roster_counts_by_week.sql"
WITH params AS (
  SELECT 2024 AS season
),
rost AS (
  SELECT season, week, team, player_id FROM read_parquet('data/silver/rosters/season=*/*.parquet')
  WHERE season = (SELECT season FROM params)
)
SELECT
  team,
  week,
  COUNT(DISTINCT player_id) AS active_players
FROM rost
GROUP BY 1,2
ORDER BY week, team;


