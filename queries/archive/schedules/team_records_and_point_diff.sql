-- Team record and point differential by season and week-to-date
-- Usage: duckdb -c "\i queries/team_records_and_point_diff.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
sched AS (
  SELECT * FROM read_parquet('data/silver/schedules/season=*/*.parquet', union_by_name=true)
  WHERE try_cast(season AS INT) = (SELECT season FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND game_type = 'REG'
),
games AS (
  SELECT
    home_team AS team,
    CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
    CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss,
    CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS tie,
    (home_score - away_score) AS point_diff
  FROM sched
  UNION ALL
  SELECT
    away_team AS team,
    CASE WHEN away_score > home_score THEN 1 ELSE 0 END AS win,
    CASE WHEN away_score < home_score THEN 1 ELSE 0 END AS loss,
    CASE WHEN away_score = home_score THEN 1 ELSE 0 END AS tie,
    (away_score - home_score) AS point_diff
  FROM sched
)
SELECT
  team,
  SUM(win) AS wins,
  SUM(loss) AS losses,
  SUM(tie) AS ties,
  SUM(point_diff) AS point_diff,
  SUM(win) + SUM(tie) * 0.5 AS win_points
FROM games
GROUP BY 1
ORDER BY wins DESC, point_diff DESC;


