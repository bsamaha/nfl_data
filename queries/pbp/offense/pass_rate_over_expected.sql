-- Pass Rate Over Expected (simple logistic xpass baseline proxy)
-- Usage: duckdb -c "\i queries/pass_rate_over_expected.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT week, posteam, pass, xpass, pass_oe
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
agg AS (
  SELECT posteam AS team,
         AVG(pass) AS pass_rate,
         AVG(xpass) AS exp_pass_rate,
         AVG(pass_oe) AS proe
  FROM plays
  GROUP BY 1
)
SELECT *
FROM agg
ORDER BY proe DESC;


