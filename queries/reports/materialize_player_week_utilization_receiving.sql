-- reports/materialize_player_week_utilization_receiving.sql
-- Receiving-focused weekly utilization per player

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
    WHERE rn = 1
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
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL AND air_yards IS NOT NULL THEN 1 ELSE 0 END AS is_air_tgt
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
           SUM(CASE WHEN is_shotgun=1 AND is_target=1 THEN 1 ELSE 0 END) AS shotgun_targets,
           SUM(CASE WHEN is_no_huddle=1 AND is_target=1 THEN 1 ELSE 0 END) AS no_huddle_targets,
           SUM(CASE WHEN offense_personnel = '11' AND is_target=1 THEN 1 ELSE 0 END) AS p11_targets,
           SUM(CASE WHEN offense_personnel = '12' AND is_target=1 THEN 1 ELSE 0 END) AS p12_targets,
           SUM(CASE WHEN offense_personnel = '21' AND is_target=1 THEN 1 ELSE 0 END) AS p21_targets,
           SUM(CASE WHEN is_air_tgt=1 THEN air_yards ELSE 0 END) AS sum_air_yards,
           SUM(CASE WHEN is_air_tgt=1 THEN 1 ELSE 0 END) AS cnt_air_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  ), rec_team AS (
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             offense_personnel,
             shotgun::INT AS is_shotgun,
             no_huddle::INT AS is_no_huddle,
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
           SUM(CASE WHEN offense_personnel='21' THEN is_target ELSE 0 END) AS team_targets_p21
    FROM plays
    GROUP BY season, week, season_type, team
  )
  SELECT
    w.season,
    w.week,
    w.season_type,
    w.team,
    w.player_id,
    MAX(w.player_name) AS player_name,
    MAX(w.position) AS position,
    COALESCE(MAX(rec_ev.targets),0) AS targets,
    CASE WHEN MAX(rec_team.team_targets) > 0 THEN COALESCE(MAX(rec_ev.targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets),0) END AS target_share,
    COALESCE(MAX(rec_ev.end_zone_targets),0) AS end_zone_targets,
    COALESCE(MAX(rec_ev.rz20_targets),0) AS rz20_targets,
    COALESCE(MAX(rec_ev.rz10_targets),0) AS rz10_targets,
    COALESCE(MAX(rec_ev.rz5_targets),0)  AS rz5_targets,
    COALESCE(MAX(rec_ev.third_fourth_down_targets),0) AS third_fourth_down_targets,
    COALESCE(MAX(rec_ev.ldd_targets),0) AS ldd_targets,
    COALESCE(MAX(rec_ev.sdd_targets),0) AS sdd_targets,
    COALESCE(MAX(rec_ev.two_minute_targets),0) AS two_minute_targets,
    COALESCE(MAX(rec_ev.four_minute_targets),0) AS four_minute_targets,
    -- Formation/tempo shares (return 0.0 when team had no such plays)
    CASE WHEN MAX(rec_team.team_targets_shotgun) > 0 THEN COALESCE(MAX(rec_ev.shotgun_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_shotgun),0) ELSE 0.0 END AS shotgun_target_share,
    CASE WHEN MAX(rec_team.team_targets_no_huddle) > 0 THEN COALESCE(MAX(rec_ev.no_huddle_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_no_huddle),0) ELSE 0.0 END AS no_huddle_target_share,
    -- Personnel group target shares (return 0.0 when team had no such plays)
    CASE WHEN MAX(rec_team.team_targets_p11) > 0 THEN COALESCE(MAX(rec_ev.p11_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p11),0) ELSE 0.0 END AS p11_target_share,
    CASE WHEN MAX(rec_team.team_targets_p12) > 0 THEN COALESCE(MAX(rec_ev.p12_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p12),0) ELSE 0.0 END AS p12_target_share,
    CASE WHEN MAX(rec_team.team_targets_p21) > 0 THEN COALESCE(MAX(rec_ev.p21_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p21),0) ELSE 0.0 END AS p21_target_share,
    -- aDOT
    CASE WHEN MAX(rec_ev.cnt_air_targets) > 0 THEN MAX(rec_ev.sum_air_yards)::DOUBLE / NULLIF(MAX(rec_ev.cnt_air_targets),0) END AS adot
  FROM w
  LEFT JOIN rec_ev   ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=w.team AND rec_ev.player_id=w.player_id
  LEFT JOIN rec_team ON rec_team.season=w.season AND rec_team.week=w.week AND rec_team.season_type=w.season_type AND rec_team.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.team, w.player_id
) TO 'data/gold/reports/player_week_utilization_receiving'
WITH (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


