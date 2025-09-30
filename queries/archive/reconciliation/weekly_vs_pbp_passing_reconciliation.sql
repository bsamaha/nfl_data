-- Reconcile passing yards from weekly vs sum of pbp by passer
-- Usage: duckdb -c "\i queries/weekly_vs_pbp_passing_reconciliation.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
weekly AS (
  SELECT player_id, player_name, team, week, passing_yards
  FROM read_parquet('data/silver/weekly/season=*/*.parquet')
  WHERE season = (SELECT season FROM params) AND week <= (SELECT thru_week FROM params)
),
pbp AS (
  SELECT passer_player_id AS player_id, passer_player_name AS player_name, posteam AS team,
         week, SUM(COALESCE(passing_yards,0)) AS pbp_passing_yards
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params) AND play = 1 AND COALESCE(aborted_play, 0) = 0
  GROUP BY 1,2,3,4
)
SELECT
  COALESCE(w.player_id, p.player_id) AS player_id,
  COALESCE(w.player_name, p.player_name) AS player_name,
  COALESCE(w.team, p.team) AS team,
  SUM(w.passing_yards) AS weekly_yards,
  SUM(p.pbp_passing_yards) AS pbp_yards,
  SUM(p.pbp_passing_yards) - SUM(w.passing_yards) AS diff
FROM weekly w
FULL JOIN pbp p USING(player_id, player_name, team, week)
GROUP BY 1,2,3
ORDER BY ABS(diff) DESC
LIMIT 200;


