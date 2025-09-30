-- Simple opponent-adjusted EPA per play (subtract opp average allowed)
-- Usage: duckdb -c "\i queries/team_epa_opponent_adjusted.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT game_id, week, posteam, defteam, epa
  FROM read_parquet('data/silver/pbp/year=*/*.parquet')
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
opp_def AS (
  SELECT defteam AS team, AVG(epa) AS opp_allowed_epa_per_play
  FROM plays
  GROUP BY 1
),
team_raw AS (
  SELECT posteam AS team, AVG(epa) AS raw_epa_per_play
  FROM plays
  GROUP BY 1
),
team_adj AS (
  SELECT p.posteam AS team,
         AVG(p.epa - od.opp_allowed_epa_per_play) AS adj_epa_per_play
  FROM plays p
  JOIN opp_def od ON od.team = p.defteam
  GROUP BY 1
)
SELECT tr.team,
       tr.raw_epa_per_play,
       ta.adj_epa_per_play
FROM team_raw tr
JOIN team_adj ta USING(team)
ORDER BY adj_epa_per_play DESC;


