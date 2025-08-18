-- Kickoff touchback rate by team
-- Usage: duckdb -c "\i queries/kickoff_touchback_rate.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND kickoff_attempt = 1
)
SELECT
  posteam AS kicking_team,
  COUNT(*) AS kickoffs,
  SUM(CASE WHEN kickoff_in_endzone = 1 THEN 1 ELSE 0 END) AS in_endzone,
  SUM(CASE WHEN touchback = 1 THEN 1 ELSE 0 END) AS touchbacks,
  touchbacks / NULLIF(kickoffs, 0) AS touchback_rate,
  in_endzone / NULLIF(kickoffs, 0) AS in_endzone_rate
FROM plays
WHERE posteam IS NOT NULL
GROUP BY 1
ORDER BY touchback_rate DESC;


