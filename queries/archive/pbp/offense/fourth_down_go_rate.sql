-- Fourth down decision-making: go-for-it rate and success
-- Usage: duckdb -c "\i queries/fourth_down_go_rate.sql"
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
    AND CAST(down AS INT) = 4
)
SELECT
  posteam AS team,
  COUNT(*) AS fourth_down_plays,
  SUM(CASE WHEN (pass = 1 OR rush = 1) THEN 1 ELSE 0 END) AS go_for_it_plays,
  SUM(CASE WHEN (pass = 1 OR rush = 1) AND first_down = 1 THEN 1 ELSE 0 END) AS successful_go_plays,
  go_for_it_plays / NULLIF(COUNT(*), 0) AS go_rate,
  successful_go_plays / NULLIF(NULLIF(go_for_it_plays, 0), 0) AS go_success_rate
FROM plays
WHERE posteam IS NOT NULL
GROUP BY 1
HAVING fourth_down_plays >= 5
ORDER BY go_rate DESC;


