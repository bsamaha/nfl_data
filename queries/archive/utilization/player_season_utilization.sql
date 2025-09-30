-- queries/utilization/player_season_utilization.sql
WITH src AS (
  SELECT *
  FROM read_parquet('data/gold/utilization/player_week/season=*/week=*/**/*.parquet', union_by_name=true)
)
SELECT
  season, season_type, player_id,
  MAX(player_name) AS player_name,
  MAX(position)    AS position,
  MAX(team)        AS primary_team,
  COUNT(*)         AS games,
  AVG(snap_share)  AS snap_share,
  SUM(COALESCE(routes_run,0)) AS routes_run,
  AVG(NULLIF(route_participation, NULL)) AS route_participation,
  AVG(NULLIF(tprr, NULL)) AS tprr,
  AVG(NULLIF(yprr, NULL)) AS yprr,
  SUM(targets) AS targets,
  AVG(NULLIF(target_share, NULL)) AS target_share,
  SUM(receiving_yards) AS receiving_yards,
  SUM(receiving_air_yards) AS receiving_air_yards,
  AVG(NULLIF(air_yards_share, NULL)) AS air_yards_share,
  AVG(NULLIF(wopr, NULL)) AS wopr,
  SUM(end_zone_targets) AS end_zone_targets,
  AVG(NULLIF(end_zone_target_share, NULL)) AS end_zone_target_share,
  SUM(rz20_targets) AS rz20_targets,
  AVG(NULLIF(rz20_target_share, NULL)) AS rz20_target_share,
  SUM(rz10_targets) AS rz10_targets,
  AVG(NULLIF(rz10_target_share, NULL)) AS rz10_target_share,
  SUM(rz5_targets)  AS rz5_targets,
  AVG(NULLIF(rz5_target_share, NULL))  AS rz5_target_share,
  AVG(NULLIF(ldd_target_share, NULL))  AS ldd_target_share,
  AVG(NULLIF(sdd_target_share, NULL))  AS sdd_target_share,
  AVG(NULLIF(third_fourth_down_target_share, NULL)) AS third_fourth_down_target_share,
  AVG(NULLIF(two_minute_target_share, NULL)) AS two_minute_target_share,
  AVG(NULLIF(four_minute_target_share, NULL)) AS four_minute_target_share,
  AVG(NULLIF(play_action_target_share, NULL)) AS play_action_target_share,
  AVG(NULLIF(carry_share, NULL)) AS carry_share,
  AVG(NULLIF(rz20_carry_share, NULL)) AS rz20_carry_share,
  AVG(NULLIF(rz10_carry_share, NULL)) AS rz10_carry_share,
  AVG(NULLIF(rz5_carry_share, NULL))  AS rz5_carry_share
FROM src
GROUP BY 1,2,3
ORDER BY season, position, player_name;


