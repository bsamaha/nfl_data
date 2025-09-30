-- Fantasy PPR points leaders through a week (simple PPR scoring)
-- Usage: duckdb -c "\i queries/fantasy_points_leaders_ppr.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
weekly AS (
  SELECT * FROM read_parquet('data/silver/weekly/season=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND week <= (SELECT thru_week FROM params)
)
SELECT
  player_id,
  MAX(player_name) AS player_name,
  MAX(position) AS position,
  MAX(team) AS team,
  SUM(
      COALESCE(passing_yards,0) * 0.04
    + COALESCE(passing_tds,0) * 4
    - COALESCE(interceptions,0) * 2
    + COALESCE(rushing_yards,0) * 0.1
    + COALESCE(rushing_tds,0) * 6
    + COALESCE(receiving_yards,0) * 0.1
    + COALESCE(receiving_tds,0) * 6
    + COALESCE(receptions,0) * 1
    - (COALESCE(rushing_fumbles_lost,0) + COALESCE(receiving_fumbles_lost,0) + COALESCE(sack_fumbles_lost,0)) * 2
  ) AS ppr_points
FROM weekly
GROUP BY 1
ORDER BY ppr_points DESC
LIMIT 100;


