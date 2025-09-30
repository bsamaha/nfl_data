-- Drive-level summaries: points, EPA, success
-- Usage: duckdb -c "\i queries/drive_summary_epa.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
drive_agg AS (
  SELECT
    game_id,
    drive AS drive_num,
    MAX(posteam) FILTER (WHERE posteam IS NOT NULL) AS offense,
    SUM(epa) AS drive_epa,
    AVG(success) AS success_rate,
    SUM(CASE WHEN touchdown = 1 THEN 7
             WHEN field_goal_attempt = 1 AND field_goal_result = 'good' THEN 3
             ELSE 0 END) AS naive_points
  FROM plays
  GROUP BY 1,2
)
SELECT *
FROM drive_agg
ORDER BY game_id, drive_num;


