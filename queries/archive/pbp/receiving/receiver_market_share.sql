WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week, 'REG' AS season_type
),
plays AS (
  SELECT week, posteam AS team,
         COALESCE(receiver_player_id, receiver_id) AS player_id,
         COALESCE(receiver_player_name, receiver) AS player_name,
         air_yards,
         pass_attempt
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND week <= (SELECT thru_week FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND pass_attempt = 1
),
team_tot AS (
  SELECT week, team,
         COUNT(*) AS team_targets,
         SUM(COALESCE(air_yards,0)) AS team_air_yards
  FROM plays
  GROUP BY 1,2
),
player_tot AS (
  SELECT player_id, MAX(player_name) AS player_name, team,
         COUNT(*) AS targets,
         SUM(COALESCE(air_yards,0)) AS air_yards
  FROM plays
  WHERE player_id IS NOT NULL
  GROUP BY 1,3
),
joined AS (
  SELECT p.player_id, p.player_name, p.team, p.targets, p.air_yards,
         t.team_targets, t.team_air_yards
  FROM player_tot p
  JOIN (
    SELECT team,
           SUM(team_targets) AS team_targets,
           SUM(team_air_yards) AS team_air_yards
    FROM team_tot
    GROUP BY 1
  ) t USING(team)
)
SELECT
  player_id,
  player_name,
  team,
  targets,
  air_yards,
  targets / NULLIF(team_targets, 0) AS target_share,
  air_yards / NULLIF(team_air_yards, 0) AS air_yards_share
FROM joined
WHERE targets >= 30
ORDER BY target_share DESC
LIMIT 200;


