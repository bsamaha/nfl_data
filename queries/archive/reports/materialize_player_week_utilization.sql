-- reports/materialize_player_week_utilization.sql
-- Materialize comprehensive weekly utilization metrics for all players
-- Output: data/gold/reports/player_week_utilization (partitioned by season, week, team)

COPY (
  WITH params AS (
    SELECT 2025 AS season,
           'REG' AS season_type
  ), w_raw AS (
    SELECT *
    FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
  ), w AS (
    -- De-duplicate weekly to one row per player-week-team
    SELECT * EXCLUDE (rn)
    FROM (
      SELECT
        w_raw.*,
        ROW_NUMBER() OVER (
          PARTITION BY w_raw.season, w_raw.week, w_raw.season_type, w_raw.team, w_raw.player_id
          ORDER BY w_raw.source NULLS LAST
        ) AS rn
      FROM w_raw
    )
    WHERE rn = 1
  ), sc AS (
    SELECT season, week, player_id AS sc_player_id,
           AVG(offense_pct) AS snap_share
    FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
    GROUP BY season, week, sc_player_id
  ), routes AS (
    -- If NGS routes are unavailable, synthesize NULLs to keep schema
    SELECT w.season, w.week, w.team, w.player_id, CAST(NULL AS BIGINT) AS routes_run
    FROM w
  ), ctx AS (
    -- Team context for denominators
    WITH snaps AS (
      SELECT season, week, team, MAX(offense_snaps) AS team_offense_snaps
      FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
      WHERE season = (SELECT season FROM params)
      GROUP BY season, week, team
    ), pbp AS (
      SELECT year AS season, week, season_type, posteam AS team,
             SUM(CASE WHEN qb_dropback=1 THEN 1 ELSE 0 END) AS team_dropbacks,
             SUM(CASE WHEN pass_attempt=1 AND sack=0 THEN 1 ELSE 0 END) AS team_pass_attempts,
             SUM(CASE WHEN rush_attempt=1 THEN 1 ELSE 0 END) AS team_carries
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params)
        AND season_type = (SELECT season_type FROM params)
      GROUP BY year, week, season_type, posteam
    )
    SELECT p.season, p.week, p.season_type, p.team,
           s.team_offense_snaps, p.team_dropbacks, p.team_pass_attempts, p.team_carries
    FROM pbp p LEFT JOIN snaps s
      ON s.season=p.season AND s.week=p.week AND s.team=p.team
  ), rec_ev AS (
    -- Receiving situational events (targets and splits)
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
      WHERE year = (SELECT season FROM params)
        AND season_type = (SELECT season_type FROM params)
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
           -- Formation/tempo specific target counts
           SUM(CASE WHEN is_shotgun=1 AND is_target=1 THEN 1 ELSE 0 END) AS shotgun_targets,
           SUM(CASE WHEN is_no_huddle=1 AND is_target=1 THEN 1 ELSE 0 END) AS no_huddle_targets,
           -- Personnel-specific target counts (common groupings)
           SUM(CASE WHEN offense_personnel = '11' AND is_target=1 THEN 1 ELSE 0 END) AS p11_targets,
           SUM(CASE WHEN offense_personnel = '12' AND is_target=1 THEN 1 ELSE 0 END) AS p12_targets,
           SUM(CASE WHEN offense_personnel = '21' AND is_target=1 THEN 1 ELSE 0 END) AS p21_targets,
           SUM(CASE WHEN is_air_tgt=1 THEN air_yards ELSE 0 END) AS sum_air_yards,
           SUM(CASE WHEN is_air_tgt=1 THEN 1 ELSE 0 END) AS cnt_air_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  ), rec_team AS (
    -- Team denominators for formation/tempo and personnel shares
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
  ), rush_ev AS (
    SELECT year AS season, week, season_type, posteam AS team,
           rusher_player_id AS player_id,
           COUNT(*) AS carries,
           SUM(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END) AS rz20_carries,
           SUM(CASE WHEN yardline_100 <= 10 THEN 1 ELSE 0 END) AS rz10_carries,
           SUM(CASE WHEN yardline_100 <=  5 THEN 1 ELSE 0 END) AS rz5_carries
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
      AND rush = 1
    GROUP BY year, week, season_type, posteam, rusher_player_id
  ), ngs_ay AS (
    -- aDOT from PBP-derived air yards per target
    SELECT season, week, season_type, team, player_id,
           CASE WHEN cnt_air_targets > 0 THEN sum_air_yards::DOUBLE / NULLIF(cnt_air_targets,0) END AS adot
    FROM rec_ev
  ), xfp AS (
    -- Expected fantasy points lite
    WITH events AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS rec_id,
             rusher_player_id   AS rush_id,
             pass, rush, yardline_100, air_yards
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    ), recv AS (
      SELECT season, week, season_type, team, rec_id AS player_id,
             SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS targets,
             SUM(CASE WHEN pass=1 AND air_yards >= 15 THEN 0.6 WHEN pass=1 THEN 0.4 ELSE 0 END) AS xfp_rec
      FROM events
      GROUP BY 1,2,3,4,5
    ), rush AS (
      SELECT season, week, season_type, team, rush_id AS player_id,
             COUNT(*) AS carries,
             SUM(CASE WHEN rush=1 AND yardline_100 <= 5 THEN 0.8 WHEN rush=1 AND yardline_100 <= 10 THEN 0.5 WHEN rush=1 AND yardline_100 <= 20 THEN 0.3 ELSE 0.15 END) AS xfp_rush
      FROM events
      WHERE rush=1
      GROUP BY 1,2,3,4,5
    )
    SELECT COALESCE(r.season, v.season) AS season,
           COALESCE(r.week, v.week) AS week,
           COALESCE(r.season_type, v.season_type) AS season_type,
           COALESCE(r.team, v.team) AS team,
           COALESCE(r.player_id, v.player_id) AS player_id,
           COALESCE(r.xfp_rush, 0) + COALESCE(v.xfp_rec, 0) AS xfp_lite
    FROM rush r
    FULL OUTER JOIN recv v USING (season, week, season_type, team, player_id)
  ), team_style AS (
    -- Team PROE/pace proxy
    WITH base AS (
      SELECT year AS season, week, season_type, posteam AS team,
             pass::INT AS is_pass, xpass,
             half_seconds_remaining
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
        AND qb_dropback = 1 AND half_seconds_remaining > 120
    )
    SELECT season, week, season_type, team,
           AVG(is_pass) - AVG(xpass) AS proe_neutral,
           30.0 / NULLIF(COUNT(*),0) * 60.0 AS sec_per_play_neutral
    FROM base
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
    MAX(sc.snap_share) AS snap_share,
    MAX(routes.routes_run) AS routes_run,
    CASE WHEN MAX(ctx.team_dropbacks) > 0 AND MAX(routes.routes_run) IS NOT NULL THEN MAX(routes.routes_run)::DOUBLE / NULLIF(MAX(ctx.team_dropbacks),0) END AS route_participation,
    COALESCE(MAX(rec_ev.targets), 0) AS targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.targets), 0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS target_share,
    MAX(COALESCE(w.receiving_yards,0)) AS receiving_yards,
    MAX(COALESCE(w.receiving_air_yards,0)) AS receiving_air_yards,
    MAX(NULLIF(w.air_yards_share, NULL)) AS air_yards_share,
    MAX(NULLIF(w.wopr, NULL)) AS wopr,
    COALESCE(MAX(rec_ev.end_zone_targets),0) AS end_zone_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.end_zone_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS end_zone_target_share,
    COALESCE(MAX(rec_ev.rz20_targets),0) AS rz20_targets,
    COALESCE(MAX(rec_ev.rz10_targets),0) AS rz10_targets,
    COALESCE(MAX(rec_ev.rz5_targets),0)  AS rz5_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.rz20_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz20_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.rz10_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz10_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.rz5_targets),0)::DOUBLE  / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz5_target_share,
    COALESCE(MAX(rec_ev.third_fourth_down_targets),0) AS third_fourth_down_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.third_fourth_down_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS third_fourth_down_target_share,
    COALESCE(MAX(rec_ev.ldd_targets),0) AS ldd_targets,
    COALESCE(MAX(rec_ev.sdd_targets),0) AS sdd_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.ldd_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS ldd_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.sdd_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS sdd_target_share,
    COALESCE(MAX(rec_ev.two_minute_targets),0) AS two_minute_targets,
    COALESCE(MAX(rec_ev.four_minute_targets),0) AS four_minute_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.two_minute_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS two_minute_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.four_minute_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS four_minute_target_share,
    -- Formation/tempo shares
    CASE WHEN MAX(rec_team.team_targets_shotgun) > 0 THEN COALESCE(MAX(rec_ev.shotgun_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_shotgun),0) END AS shotgun_target_share,
    CASE WHEN MAX(rec_team.team_targets_no_huddle) > 0 THEN COALESCE(MAX(rec_ev.no_huddle_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_no_huddle),0) END AS no_huddle_target_share,
    -- Personnel group target shares
    CASE WHEN MAX(rec_team.team_targets_p11) > 0 THEN COALESCE(MAX(rec_ev.p11_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p11),0) END AS p11_target_share,
    CASE WHEN MAX(rec_team.team_targets_p12) > 0 THEN COALESCE(MAX(rec_ev.p12_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p12),0) END AS p12_target_share,
    CASE WHEN MAX(rec_team.team_targets_p21) > 0 THEN COALESCE(MAX(rec_ev.p21_targets),0)::DOUBLE / NULLIF(MAX(rec_team.team_targets_p21),0) END AS p21_target_share,
    -- Carry shares
    COALESCE(MAX(rush_ev.carries),0) AS carries,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.carries),0)::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz20_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz20_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz10_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz10_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz5_carries),0)::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS rz5_carry_share,
    -- Advanced per-route metrics
    CASE WHEN MAX(routes.routes_run) > 0 THEN MAX(COALESCE(w.receiving_yards,0))::DOUBLE / NULLIF(MAX(routes.routes_run),0) END AS yprr,
    CASE WHEN MAX(routes.routes_run) > 0 THEN COALESCE(MAX(rec_ev.targets),0)::DOUBLE / NULLIF(MAX(routes.routes_run),0) END AS tprr,
    MAX(ngs_ay.adot) AS adot,
    MAX(xfp.xfp_lite) AS xfp_lite,
    MAX(team_style.proe_neutral) AS team_proe_neutral,
    MAX(team_style.sec_per_play_neutral) AS team_sec_per_play_neutral,
    MAX(ctx.team_offense_snaps) AS team_offense_snaps,
    MAX(ctx.team_dropbacks) AS team_dropbacks,
    MAX(ctx.team_pass_attempts) AS team_pass_attempts,
    MAX(ctx.team_carries) AS team_carries
  FROM w
  LEFT JOIN sc        ON sc.season=w.season AND sc.week=w.week AND sc.sc_player_id=w.player_id
  LEFT JOIN routes    ON routes.season=w.season AND routes.week=w.week AND routes.team=w.team AND routes.player_id=w.player_id
  LEFT JOIN rec_ev    ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=w.team AND rec_ev.player_id=w.player_id
  LEFT JOIN rec_team  ON rec_team.season=w.season AND rec_team.week=w.week AND rec_team.season_type=w.season_type AND rec_team.team=w.team
  LEFT JOIN rush_ev   ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.team=w.team AND rush_ev.player_id=w.player_id
  LEFT JOIN ctx       ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=w.team
  LEFT JOIN ngs_ay    ON ngs_ay.season=w.season AND ngs_ay.week=w.week AND ngs_ay.season_type=w.season_type AND ngs_ay.team=w.team AND ngs_ay.player_id=w.player_id
  LEFT JOIN xfp       ON xfp.season=w.season AND xfp.week=w.week AND xfp.season_type=w.season_type AND xfp.team=w.team AND xfp.player_id=w.player_id
  LEFT JOIN team_style ON team_style.season=w.season AND team_style.week=w.week AND team_style.season_type=w.season_type AND team_style.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.team, w.player_id
) TO 'data/gold/reports/player_week_utilization'
WITH (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));




