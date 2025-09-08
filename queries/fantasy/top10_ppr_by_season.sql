-- Parameters (optional): :limit (default from UI), :playoffs_only (0 or 1)

WITH params AS (
  SELECT CAST(:season AS INTEGER) AS season, CAST(:position AS VARCHAR) AS position, CAST(:playoffs_only AS INTEGER) AS playoffs_only
), src AS (
  SELECT *
  FROM read_parquet('data/gold/player_week_fantasy/season=*/week=*/**/*.parquet')
)
SELECT
  s.season,
  MAX(s.player_name) AS player_name,
  MAX(s.position) AS position,
  SUM(s.ppr_points) AS ppr_total,
  SUM(s.dk_ppr_points) AS dk_total,
  SUM(s.ppr_points) - SUM(s.dk_ppr_points) AS delta
FROM src s, params p
WHERE s.season = p.season
  AND s.season_type = 'REG'
  AND (p.position = 'ALL' OR s.position = p.position)
  AND (p.playoffs_only = 0 OR (s.week BETWEEN 15 AND 17))
GROUP BY s.season, s.player_id
ORDER BY ppr_total DESC, player_name
LIMIT :limit;


