-- Aggregate points/plays by roof type per season
-- Usage: scripts/run_query.sh -f queries/league_aggregates_by_roof.sql -s 2024 -t REG

WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
pbp AS (
  SELECT season, season_type, game_id, roof, play, aborted_play,
         total_home_score, total_away_score
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
),
games AS (
  SELECT season, game_id,
         COALESCE(NULLIF(LOWER(roof), ''), 'unknown') AS roof,
         MAX(total_home_score) AS home_pts,
         MAX(total_away_score) AS away_pts
  FROM pbp
  GROUP BY 1,2,3
)
SELECT
  season,
  roof,
  AVG(home_pts + away_pts) AS avg_total_points,
  median(home_pts + away_pts) AS med_total_points
FROM games
GROUP BY 1,2
ORDER BY season, roof;


