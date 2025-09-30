-- Rushing leaders through a given week
-- Usage: duckdb -c "\i queries/rushing_leaders_through_week.sql"
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
    COALESCE(rusher_player_id, rusher_id) AS player_id,
    COALESCE(rusher_player_name, rusher) AS player_name,
    SUM(CASE WHEN rush_attempt = 1 THEN 1 ELSE 0 END) AS att,
    SUM(COALESCE(rushing_yards,0)) AS yds,
    SUM(CASE WHEN rush_touchdown = 1 THEN 1 ELSE 0 END) AS td
  FROM pbp
  WHERE COALESCE(rusher_player_id, rusher_id) IS NOT NULL
  GROUP BY 1,2
)
SELECT
  player_id,
  player_name,
  att,
  yds,
  td,
  yds / NULLIF(att,0) AS yds_per_att
FROM agg
ORDER BY yds DESC
LIMIT 100;


