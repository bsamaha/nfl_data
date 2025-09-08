-- queries/utilization/formation_tempo_target_shares.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), plays AS (
  SELECT year AS season, week, season_type, posteam AS team,
         receiver_player_id AS player_id,
         shotgun::INT AS is_shotgun,
         no_huddle::INT AS is_no_huddle,
         CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
), team AS (
  SELECT season, week, season_type, team,
         SUM(is_target) AS team_targets,
         SUM(CASE WHEN is_shotgun=1 THEN is_target ELSE 0 END) AS team_targets_shotgun,
         SUM(CASE WHEN is_no_huddle=1 THEN is_target ELSE 0 END) AS team_targets_no_huddle
  FROM plays
  GROUP BY 1,2,3,4
)
SELECT p.season, p.week, p.season_type, p.team, p.player_id,
       SUM(CASE WHEN p.is_shotgun=1 THEN p.is_target ELSE 0 END)::DOUBLE / NULLIF(MAX(t.team_targets_shotgun),0) AS shotgun_target_share,
       SUM(CASE WHEN p.is_no_huddle=1 THEN p.is_target ELSE 0 END)::DOUBLE / NULLIF(MAX(t.team_targets_no_huddle),0) AS no_huddle_target_share
FROM plays p
JOIN team t ON t.season=p.season AND t.week=p.week AND t.season_type=p.season_type AND t.team=p.team
GROUP BY 1,2,3,4,5;


