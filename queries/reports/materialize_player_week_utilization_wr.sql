-- reports/materialize_player_week_utilization_wr.sql
-- Wide WR utilization per player-week

COPY (
  WITH params AS (
    SELECT 2025 AS season,
           'REG' AS season_type
  ), w_raw AS (
    SELECT *
    FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
  ), w AS (
    SELECT * EXCLUDE (rn)
    FROM (
      SELECT w_raw.*, ROW_NUMBER() OVER (
        PARTITION BY w_raw.season, w_raw.week, w_raw.season_type, w_raw.team, w_raw.player_id
        ORDER BY w_raw.source NULLS LAST
      ) AS rn
      FROM w_raw
    )
    WHERE rn = 1 AND position='WR'
  ), routes AS (
    SELECT w.season, w.week, w.team, w.player_id, CAST(NULL AS BIGINT) AS routes_run
    FROM w
  ), rec_ev AS (
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS player_id,
             air_yards, yardline_100,
             down, ydstogo,
             half_seconds_remaining,
             offense_personnel,
             shotgun::INT AS is_shotgun,
             no_huddle::INT AS is_no_huddle,
             0::INT AS is_play_action,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
    )
    SELECT season, week, season_type, team, player_id,
           SUM(is_target) AS targets,
           SUM(CASE WHEN air_yards IS NOT NULL AND yardline_100 - air_yards <= 0 THEN 1 ELSE 0 END) AS end_zone_targets,
           SUM(CASE WHEN yardline_100 <= 20 AND is_target=1 THEN 1 ELSE 0 END) AS rz20_targets,
           SUM(CASE WHEN yardline_100 <= 10 AND is_target=1 THEN 1 ELSE 0 END) AS rz10_targets,
           SUM(CASE WHEN yardline_100 <=  5 AND is_target=1 THEN 1 ELSE 0 END) AS rz5_targets,
           SUM(CASE WHEN down IN (3,4) AND is_target=1 THEN 1 ELSE 0 END) AS third_fourth_down_targets,
           SUM(CASE WHEN down IN (3,4) AND ydstogo >= 5 AND is_target=1 THEN 1 ELSE 0 END) AS ldd_targets,
           SUM(CASE WHEN down IN (1,2,3,4) AND ydstogo <= 2 AND is_target=1 THEN 1 ELSE 0 END) AS sdd_targets,
           SUM(CASE WHEN half_seconds_remaining <= 120 AND is_target=1 THEN 1 ELSE 0 END) AS two_minute_targets,
           SUM(CASE WHEN half_seconds_remaining <= 240 AND is_target=1 THEN 1 ELSE 0 END) AS four_minute_targets,
           SUM(CASE WHEN is_play_action=1 AND is_target=1 THEN 1 ELSE 0 END) AS play_action_targets,
           SUM(CASE WHEN is_shotgun=1 AND is_target=1 THEN 1 ELSE 0 END) AS shotgun_targets,
           SUM(CASE WHEN is_no_huddle=1 AND is_target=1 THEN 1 ELSE 0 END) AS no_huddle_targets,
           SUM(CASE WHEN offense_personnel = '11' AND is_target=1 THEN 1 ELSE 0 END) AS p11_targets,
           SUM(CASE WHEN offense_personnel = '12' AND is_target=1 THEN 1 ELSE 0 END) AS p12_targets,
           SUM(CASE WHEN offense_personnel = '21' AND is_target=1 THEN 1 ELSE 0 END) AS p21_targets,
           SUM(CASE WHEN is_target=1 AND air_yards IS NOT NULL THEN air_yards ELSE 0 END) AS sum_air_yards,
           SUM(CASE WHEN is_target=1 AND air_yards IS NOT NULL THEN 1 ELSE 0 END) AS cnt_air_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  ), rec_team AS (
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             offense_personnel,
             shotgun::INT AS is_shotgun,
             no_huddle::INT AS is_no_huddle,
             yardline_100, air_yards,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
    )
    SELECT season, week, season_type, team,
           SUM(is_target) AS team_targets,
           SUM(CASE WHEN is_shotgun=1 THEN is_target ELSE 0 END) AS team_targets_shotgun,
           SUM(CASE WHEN is_no_huddle=1 THEN is_target ELSE 0 END) AS team_targets_no_huddle,
           SUM(CASE WHEN offense_personnel='11' THEN is_target ELSE 0 END) AS team_targets_p11,
           SUM(CASE WHEN offense_personnel='12' THEN is_target ELSE 0 END) AS team_targets_p12,
           SUM(CASE WHEN offense_personnel='21' THEN is_target ELSE 0 END) AS team_targets_p21,
           SUM(CASE WHEN yardline_100 <= 20 AND is_target=1 THEN 1 ELSE 0 END) AS team_targets_rz20,
           SUM(CASE WHEN yardline_100 <= 10 AND is_target=1 THEN 1 ELSE 0 END) AS team_targets_rz10,
           SUM(CASE WHEN yardline_100 <=  5 AND is_target=1 THEN 1 ELSE 0 END) AS team_targets_rz5,
           SUM(CASE WHEN air_yards IS NOT NULL AND yardline_100 - air_yards <= 0 THEN 1 ELSE 0 END) AS team_end_zone_targets
    FROM plays
    GROUP BY season, week, season_type, team
  ), ctx AS (
    SELECT year AS season, week, season_type, posteam AS team,
           SUM(CASE WHEN pass_attempt=1 AND sack=0 THEN 1 ELSE 0 END) AS team_pass_attempts,
           SUM(CASE WHEN pass=1 AND yardline_100 <= 20 THEN 1 ELSE 0 END) AS team_rz20_pass_attempts,
           SUM(CASE WHEN pass=1 AND yardline_100 <= 10 THEN 1 ELSE 0 END) AS team_rz10_pass_attempts,
           SUM(CASE WHEN pass=1 AND yardline_100 <=  5 THEN 1 ELSE 0 END) AS team_rz5_pass_attempts
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
    GROUP BY year, week, season_type, posteam
  ), team_style AS (
    WITH base AS (
      SELECT year AS season, week, season_type, posteam AS team,
             game_id, drive,
             pass::INT AS is_pass, xpass,
             half_seconds_remaining
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
        AND qb_dropback = 1 AND half_seconds_remaining > 120
    ), neutral_drives AS (
      SELECT season, week, season_type, team, game_id, drive
      FROM base
      GROUP BY season, week, season_type, team, game_id, drive
    ), drive_info AS (
      SELECT year AS season, week, season_type, posteam AS team,
             game_id, drive,
             MAX(drive_play_count) AS drive_play_count,
             -- drive_time_of_possession is formatted 'MM:SS'; parse to seconds
             MAX(CASE WHEN drive_time_of_possession IS NOT NULL THEN (
               CAST(SPLIT_PART(drive_time_of_possession, ':', 1) AS DOUBLE) * 60 +
               CAST(SPLIT_PART(drive_time_of_possession, ':', 2) AS DOUBLE)
             ) END) AS drive_time_seconds
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
      GROUP BY year, week, season_type, posteam, game_id, drive
    )
    SELECT nd.season, nd.week, nd.season_type, nd.team,
           AVG(b.is_pass) - AVG(b.xpass) AS proe_neutral,
           AVG(CASE WHEN di.drive_play_count > 0 AND di.drive_time_seconds IS NOT NULL THEN di.drive_time_seconds::DOUBLE / NULLIF(di.drive_play_count,0) END) AS sec_per_play_neutral
    FROM neutral_drives nd
    LEFT JOIN drive_info di ON di.season=nd.season AND di.week=nd.week AND di.season_type=nd.season_type AND di.team=nd.team AND di.game_id=nd.game_id AND di.drive=nd.drive
    LEFT JOIN base b ON b.season=nd.season AND b.week=nd.week AND b.season_type=nd.season_type AND b.team=nd.team AND b.game_id=nd.game_id AND b.drive=nd.drive
    GROUP BY nd.season, nd.week, nd.season_type, nd.team
  )
  SELECT
    w.season,
    w.week,
    w.season_type,
    w.team,
    w.player_id,
    MAX(w.player_name) AS player_name,
    MAX(w.position) AS position,
    -- volume
    COALESCE(MAX(rec_ev.targets),0) AS targets,
    CASE WHEN MAX(rec_team.team_targets) > 0 THEN COALESCE(MAX(rec_ev.targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets),0) END AS target_share,
    -- per-route (nullable if routes absent)
    MAX(routes.routes_run) AS routes_run,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 AND MAX(routes.routes_run) IS NOT NULL THEN MAX(routes.routes_run)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS route_participation,
    -- YPRR/TPRR
    CASE WHEN MAX(routes.routes_run) > 0 THEN MAX(COALESCE(w.receiving_yards,0))::DOUBLE / NULLIF(MAX(routes.routes_run),0) END AS yprr,
    CASE WHEN MAX(routes.routes_run) > 0 THEN COALESCE(MAX(rec_ev.targets),0)::DOUBLE / NULLIF(MAX(routes.routes_run),0) END AS tprr,
    -- market shares from weekly when present
    MAX(NULLIF(w.air_yards_share, NULL)) AS air_yards_share,
    MAX(NULLIF(w.wopr, NULL)) AS wopr,
    -- situational target shares vs team attempts
    COALESCE(MAX(rec_ev.end_zone_targets),0) AS end_zone_targets,
    CASE WHEN MAX(rec_team.team_end_zone_targets) > 0 THEN COALESCE(MAX(rec_ev.end_zone_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_end_zone_targets),0) ELSE 0.0 END AS end_zone_target_share,
    COALESCE(MAX(rec_ev.rz20_targets),0) AS rz20_targets,
    COALESCE(MAX(rec_ev.rz10_targets),0) AS rz10_targets,
    COALESCE(MAX(rec_ev.rz5_targets),0)  AS rz5_targets,
    CASE WHEN MAX(rec_team.team_targets_rz20) > 0 THEN COALESCE(MAX(rec_ev.rz20_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_rz20),0) ELSE 0.0 END AS rz20_target_share,
    CASE WHEN MAX(rec_team.team_targets_rz10) > 0 THEN COALESCE(MAX(rec_ev.rz10_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_rz10),0) ELSE 0.0 END AS rz10_target_share,
    CASE WHEN MAX(rec_team.team_targets_rz5)  > 0 THEN COALESCE(MAX(rec_ev.rz5_targets),0)::DOUBLE  / NULLIF(MAX(rec_team.team_targets_rz5),0)  ELSE 0.0 END AS rz5_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.ldd_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS ldd_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.sdd_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS sdd_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.third_fourth_down_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS third_fourth_down_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.two_minute_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS two_minute_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.four_minute_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS four_minute_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.play_action_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS play_action_target_share,
    -- formation/personnel shares (0.0 when denom is 0)
    CASE WHEN MAX(rec_team.team_targets_shotgun) > 0 THEN COALESCE(MAX(rec_ev.shotgun_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_shotgun),0) ELSE 0.0 END AS shotgun_target_share,
    CASE WHEN MAX(rec_team.team_targets_no_huddle) > 0 THEN COALESCE(MAX(rec_ev.no_huddle_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_no_huddle),0) ELSE 0.0 END AS no_huddle_target_share,
    CASE WHEN MAX(rec_team.team_targets_p11) > 0 THEN COALESCE(MAX(rec_ev.p11_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p11),0) ELSE 0.0 END AS p11_target_share,
    CASE WHEN MAX(rec_team.team_targets_p12) > 0 THEN COALESCE(MAX(rec_ev.p12_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p12),0) ELSE 0.0 END AS p12_target_share,
    CASE WHEN MAX(rec_team.team_targets_p21) > 0 THEN COALESCE(MAX(rec_ev.p21_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p21),0) ELSE 0.0 END AS p21_target_share,
    -- aDOT
    CASE WHEN MAX(rec_ev.cnt_air_targets) > 0 THEN MAX(rec_ev.sum_air_yards)::DOUBLE / NULLIF(MAX(rec_ev.cnt_air_targets),0) END AS adot,
    -- team context
    MAX(team_style.proe_neutral) AS team_proe_neutral,
    MAX(team_style.sec_per_play_neutral) AS team_sec_per_play_neutral
  FROM w
  LEFT JOIN routes     ON routes.season=w.season AND routes.week=w.week AND routes.team=w.team AND routes.player_id=w.player_id
  LEFT JOIN rec_ev     ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=w.team AND rec_ev.player_id=w.player_id
  LEFT JOIN rec_team   ON rec_team.season=w.season AND rec_team.week=w.week AND rec_team.season_type=w.season_type AND rec_team.team=w.team
  LEFT JOIN ctx        ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=w.team
  LEFT JOIN team_style ON team_style.season=w.season AND team_style.week=w.week AND team_style.season_type=w.season_type AND team_style.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.team, w.player_id
) TO 'data/gold/reports/player_week_utilization_wr'
WITH (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


