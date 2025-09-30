WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week, 'REG' AS season_type
),
pbp AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
agg AS (
  SELECT
    passer_player_id AS player_id,
    COALESCE(passer_player_name, passer) AS player_name,
    SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END) AS att,
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS cmp,
    SUM(COALESCE(passing_yards,0)) AS yds,
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS td,
    SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS int
  FROM pbp
  WHERE passer_player_id IS NOT NULL
  GROUP BY 1,2
)
SELECT
  player_id,
  player_name,
  att,
  cmp,
  yds,
  td,
  int,
  yds / NULLIF(att,0) AS ypa
FROM agg
ORDER BY yds DESC
LIMIT 100;


