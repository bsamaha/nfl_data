-- League efficiency trends year-over-year: EPA/play, success rate, pass rate
-- Usage: scripts/run_query.sh -f queries/league_efficiency_trends.sql -s 2024 -t REG

WITH params AS (
  SELECT 1999 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
pbp AS (
  SELECT season, season_type, play, aborted_play, epa, success, pass
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season_type = (SELECT season_type FROM params)
    AND season BETWEEN (SELECT season_start FROM params) AND (SELECT season_end FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
)
SELECT
  season,
  AVG(epa) AS league_epa_per_play,
  AVG(success) AS league_success_rate,
  AVG(pass) AS league_pass_rate
FROM pbp
GROUP BY 1
ORDER BY season;


