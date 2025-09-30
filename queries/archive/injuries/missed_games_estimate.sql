-- Estimate missed games using injuries plus schedules (players marked OUT in a given week)
-- Usage: duckdb -c "\i queries/missed_games_estimate.sql"
WITH params AS (
  SELECT 2024 AS season
),
inj AS (
  SELECT team, week, player_id, full_name, position, report_status
  FROM read_parquet('data/silver/injuries/season=*/*.parquet')
  WHERE season = (SELECT season FROM params)
),
sched AS (
  SELECT try_cast(season AS INT) AS season, week, home_team, away_team, game_id, game_type
  FROM read_parquet('data/silver/schedules/season=*/*.parquet', union_by_name=true)
  WHERE try_cast(season AS INT) = (SELECT season FROM params) AND game_type = 'REG'
),
weekly_weeks AS (
  SELECT DISTINCT week FROM sched
),
outs AS (
  SELECT
    i.player_id,
    MAX(i.full_name) AS player_name,
    MAX(i.position) AS position,
    i.week,
    MAX(i.team) AS team,
    1 AS missed
  FROM inj i
  JOIN weekly_weeks w USING(week)
  WHERE LOWER(i.report_status) LIKE '%out%'
  GROUP BY i.player_id, i.week
)
SELECT
  player_id,
  MAX(player_name) AS player_name,
  MAX(position) AS position,
  MAX(team) AS team,
  COUNT(*) AS est_missed_games
FROM outs
GROUP BY 1
ORDER BY est_missed_games DESC
LIMIT 200;


