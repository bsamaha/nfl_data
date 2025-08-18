WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week, 'REG' AS season_type
),
pbp AS (
  SELECT week,
         posteam AS team,
         COALESCE(rusher_player_id, rusher_id) AS player_id,
         COALESCE(rusher_player_name, rusher) AS player_name,
         rush_attempt
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
team_tot AS (
  SELECT week, team, SUM(CASE WHEN rush_attempt = 1 THEN 1 ELSE 0 END) AS team_rush_att
  FROM pbp
  GROUP BY 1,2
),
player_tot AS (
  SELECT player_id, MAX(player_name) AS player_name, team,
         SUM(CASE WHEN rush_attempt = 1 THEN 1 ELSE 0 END) AS rush_att
  FROM pbp
  WHERE player_id IS NOT NULL
  GROUP BY 1,3
)
SELECT
  p.player_id,
  p.player_name,
  p.team,
  p.rush_att,
  p.rush_att / NULLIF(t.team_rush_att, 0) AS carry_share
FROM player_tot p
JOIN (
  SELECT team, SUM(team_rush_att) AS team_rush_att FROM team_tot GROUP BY 1
) t USING(team)
WHERE p.rush_att >= 50
ORDER BY carry_share DESC
LIMIT 200;


