-- queries/utilization/materialize_player_week_from_pbp.sql
-- Fallback: materialize player-week utilization using only PBP + rosters when weekly feed is unavailable
COPY (
  WITH params AS (
    SELECT CAST(2025 AS INTEGER) AS season,
           CAST('REG' AS VARCHAR) AS season_type
  ), pbp AS (
    SELECT year AS season, week, season_type, game_id, posteam AS team,
           pass, complete_pass,
           receiver_player_id AS rec_id,
           rusher_player_id   AS rush_id,
           receiving_yards,
           air_yards,
           yardline_100,
           down, ydstogo, half_seconds_remaining
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
  ), rec_by_player AS (
    SELECT season, week, season_type, team,
           rec_id AS player_id,
           SUM(CASE WHEN pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS targets,
           SUM(CASE WHEN pass=1 AND complete_pass=1 THEN COALESCE(receiving_yards,0) ELSE 0 END) AS receiving_yards,
           SUM(CASE WHEN pass=1 AND rec_id IS NOT NULL THEN COALESCE(air_yards,0) ELSE 0 END) AS receiving_air_yards
    FROM pbp
    GROUP BY season, week, season_type, team, player_id
  ), team_pass_denoms AS (
    SELECT season, week, season_type, team,
           SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS team_pass_attempts,
           SUM(CASE WHEN pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS team_targets,
           SUM(CASE WHEN pass=1 AND rec_id IS NOT NULL THEN COALESCE(air_yards,0) ELSE 0 END) AS team_air_yards
    FROM pbp
    GROUP BY season, week, season_type, team
  ), ctx AS (
    SELECT d.season, d.week, d.season_type, d.team,
           d.team_pass_attempts,
           SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS team_dropbacks,
           SUM(CASE WHEN rush_id IS NOT NULL THEN 1 ELSE 0 END) AS team_carries
    FROM pbp d
    GROUP BY d.season, d.week, d.season_type, d.team, d.team_pass_attempts
  ), rec_ev AS (
    SELECT season, week, season_type, team, rec_id AS player_id,
           SUM(CASE WHEN pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS targets,
           SUM(CASE WHEN air_yards IS NOT NULL AND yardline_100 - air_yards <= 0 AND pass=1 THEN 1 ELSE 0 END) AS end_zone_targets,
           SUM(CASE WHEN yardline_100 <= 20 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS rz20_targets,
           SUM(CASE WHEN yardline_100 <= 10 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS rz10_targets,
           SUM(CASE WHEN yardline_100 <=  5 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS rz5_targets,
           SUM(CASE WHEN down IN (3,4) AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS third_fourth_down_targets,
           SUM(CASE WHEN down IN (3,4) AND ydstogo >= 5 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS ldd_targets,
           SUM(CASE WHEN down IN (1,2,3,4) AND ydstogo <= 2 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS sdd_targets,
           SUM(CASE WHEN half_seconds_remaining <= 120 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS two_minute_targets,
           SUM(CASE WHEN half_seconds_remaining <= 240 AND pass=1 AND rec_id IS NOT NULL THEN 1 ELSE 0 END) AS four_minute_targets
    FROM pbp
    GROUP BY season, week, season_type, team, player_id
  ), rush_ev AS (
    SELECT season, week, season_type, team,
           rush_id AS player_id,
           SUM(CASE WHEN rush_id IS NOT NULL THEN 1 ELSE 0 END) AS carries,
           SUM(CASE WHEN yardline_100 <= 20 AND rush_id IS NOT NULL THEN 1 ELSE 0 END) AS rz20_carries,
           SUM(CASE WHEN yardline_100 <= 10 AND rush_id IS NOT NULL THEN 1 ELSE 0 END) AS rz10_carries,
           SUM(CASE WHEN yardline_100 <=  5 AND rush_id IS NOT NULL THEN 1 ELSE 0 END) AS rz5_carries
    FROM pbp
    GROUP BY season, week, season_type, team, player_id
  ), names AS (
    SELECT season, player_id,
           MAX(COALESCE(full_name, player_name)) AS player_name,
           MAX(position) AS position
    FROM read_parquet('data/silver/rosters_seasonal/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
    GROUP BY season, player_id
  ), w AS (
    SELECT r.season, r.week, r.season_type,
           r.team, r.player_id,
           COALESCE(n.player_name, r.player_id) AS player_name,
           n.position,
           r.targets,
           r.receiving_yards,
           r.receiving_air_yards,
           CASE WHEN tp.team_targets > 0 THEN r.targets::DOUBLE / NULLIF(tp.team_targets,0) END AS target_share,
           CASE WHEN tp.team_air_yards > 0 THEN r.receiving_air_yards::DOUBLE / NULLIF(tp.team_air_yards,0) END AS air_yards_share,
           CASE WHEN tp.team_targets > 0 AND tp.team_air_yards > 0 THEN 0.7 * (r.targets::DOUBLE / NULLIF(tp.team_targets,0)) + 0.3 * (r.receiving_air_yards::DOUBLE / NULLIF(tp.team_air_yards,0)) END AS wopr
    FROM rec_by_player r
    LEFT JOIN team_pass_denoms tp ON tp.season=r.season AND tp.week=r.week AND tp.season_type=r.season_type AND tp.team=r.team
    LEFT JOIN names n ON n.season=r.season AND n.player_id=r.player_id
  )
  SELECT
    w.season AS season, w.week AS week, w.season_type AS season_type,
    w.player_id, MAX(w.player_name) AS player_name, MAX(w.position) AS position,
    w.team AS team,
    CAST(NULL AS DOUBLE) AS snap_share,
    CAST(NULL AS DOUBLE) AS routes_run,
    CAST(NULL AS DOUBLE) AS route_participation,
    CAST(NULL AS DOUBLE) AS tprr,
    CAST(NULL AS DOUBLE) AS yprr,
    SUM(COALESCE(w.targets,0)) AS targets,
    AVG(NULLIF(w.target_share, NULL)) AS target_share,
    SUM(COALESCE(w.receiving_yards,0)) AS receiving_yards,
    SUM(COALESCE(w.receiving_air_yards,0)) AS receiving_air_yards,
    AVG(NULLIF(w.air_yards_share, NULL)) AS air_yards_share,
    AVG(NULLIF(w.wopr, NULL)) AS wopr,
    SUM(COALESCE(rec_ev.end_zone_targets,0)) AS end_zone_targets,
    SUM(COALESCE(rec_ev.rz20_targets,0)) AS rz20_targets,
    SUM(COALESCE(rec_ev.rz10_targets,0)) AS rz10_targets,
    SUM(COALESCE(rec_ev.rz5_targets,0))  AS rz5_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.end_zone_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS end_zone_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz20_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz20_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz10_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz10_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.rz5_targets,0))::DOUBLE  / NULLIF(MAX(ctx.team_pass_attempts),0) END AS rz5_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.ldd_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS ldd_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.sdd_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS sdd_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.third_fourth_down_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS third_fourth_down_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.two_minute_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS two_minute_target_share,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN SUM(COALESCE(rec_ev.four_minute_targets,0))::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS four_minute_target_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.carries,0))::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz20_carries,0))::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz20_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz10_carries,0))::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz10_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN SUM(COALESCE(rush_ev.rz5_carries,0))::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS rz5_carry_share
  FROM w
  LEFT JOIN rec_ev ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=w.team AND rec_ev.player_id=w.player_id
  LEFT JOIN rush_ev ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.team=w.team AND rush_ev.player_id=w.player_id
  LEFT JOIN ctx    ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.player_id, w.team
) TO 'data/gold/utilization/player_week/part.parquet' (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


