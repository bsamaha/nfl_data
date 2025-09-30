-- Strength of schedule via opponent point differential (simple proxy)
-- Usage: duckdb -c "\i queries/strength_of_schedule_rolling.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
sched AS (
  SELECT * FROM read_parquet('data/silver/schedules/season=*/*.parquet', union_by_name=true)
  WHERE try_cast(season AS INT) = (SELECT season FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND game_type = 'REG'
),
game_points AS (
  SELECT game_id, home_team, away_team, home_score, away_score FROM sched
),
team_point_diff AS (
  SELECT team, SUM(point_diff) AS point_diff
  FROM (
    SELECT home_team AS team, (home_score - away_score) AS point_diff FROM game_points
    UNION ALL
    SELECT away_team AS team, (away_score - home_score) AS point_diff FROM game_points
  )
  GROUP BY 1
),
opponents AS (
  SELECT home_team AS team, away_team AS opponent FROM sched
  UNION ALL
  SELECT away_team AS team, home_team AS opponent FROM sched
)
SELECT
  o.team,
  AVG(tp.point_diff) AS avg_opponent_point_diff
FROM opponents o
JOIN team_point_diff tp ON o.opponent = tp.team
GROUP BY 1
ORDER BY avg_opponent_point_diff DESC;


