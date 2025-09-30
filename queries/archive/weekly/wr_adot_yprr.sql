-- WR usage: aDOT and yards per route run (approx via targets as proxy for routes if routes unavailable)
-- Usage: duckdb -c "\i queries/wr_adot_yprr.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
w AS (
  SELECT season, week, team, player_id, player_name, position,
         targets, receptions, receiving_yards, air_yards_share
  FROM read_parquet('data/silver/weekly/season=*/*.parquet')
  WHERE season = (SELECT season FROM params) AND week <= (SELECT thru_week FROM params)
),
agg AS (
  SELECT
    player_id,
    MAX(player_name) AS player_name,
    MAX(team) AS team,
    SUM(COALESCE(targets,0)) AS targets,
    SUM(COALESCE(receiving_yards,0)) AS rec_yds,
    AVG(NULLIF(air_yards_share,0)) AS avg_air_yards_per_game,
    SUM(COALESCE(air_yards_share,0)) / NULLIF(SUM(NULLIF(targets,0)), 0) AS adot
  FROM w
  WHERE position IN ('WR','TE')
  GROUP BY 1
)
SELECT
  player_id,
  player_name,
  team,
  adot,
  rec_yds / NULLIF(targets, 0) AS yprr_proxy
FROM agg
WHERE targets >= 30
ORDER BY yprr_proxy DESC;


