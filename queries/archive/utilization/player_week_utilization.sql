-- queries/utilization/player_week_utilization.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), w AS (
  SELECT * FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
), sc AS (
  SELECT season, week, player_id AS sc_player_id,
         AVG(offense_pct) AS snap_share
  FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
  GROUP BY season, week, sc_player_id
), ctx AS (
  SELECT * FROM (
    WITH snaps AS (
      SELECT season, week, team, MAX(offense_snaps) AS team_offense_snaps
      FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
      WHERE season = (SELECT season FROM params)
      GROUP BY season, week, team
    ), pbp AS (
      SELECT year AS season, week, season_type, posteam AS team,
             SUM(CASE WHEN qb_dropback=1 THEN 1 ELSE 0 END) AS team_dropbacks,
             SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END)        AS team_pass_attempts,
             SUM(CASE WHEN rush=1 THEN 1 ELSE 0 END)        AS team_carries
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params)
        AND season_type = (SELECT season_type FROM params)
      GROUP BY year, week, season_type, posteam
    )
    SELECT p.season, p.week, p.season_type, p.team,
           s.team_offense_snaps, p.team_dropbacks, p.team_pass_attempts, p.team_carries
    FROM pbp p LEFT JOIN snaps s
      ON s.season=p.season AND s.week=p.week AND s.team=p.team
  )
), rec_ev AS (
  SELECT * FROM (
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS player_id,
             air_yards, yardline_100,
             down, ydstogo,
             half_seconds_remaining,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
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
           SUM(CASE WHEN half_seconds_remaining <= 240 AND is_target=1 THEN 1 ELSE 0 END) AS four_minute_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  )
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
)
SELECT
  w.season AS season, w.week AS week, w.season_type AS season_type,
  w.player_id, MAX(w.player_name) AS player_name, MAX(w.position) AS position,
  COALESCE(w.team, rec_ev.team, rush_ev.team) AS team,
  MAX(sc.snap_share) AS snap_share,
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
LEFT JOIN sc     ON sc.season=w.season AND sc.week=w.week AND sc.sc_player_id=w.player_id
LEFT JOIN rec_ev ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.player_id=w.player_id
LEFT JOIN rush_ev ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.player_id=w.player_id
LEFT JOIN ctx    ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=COALESCE(w.team, rec_ev.team, rush_ev.team)
GROUP BY w.season, w.week, w.season_type, w.player_id, COALESCE(w.team, rec_ev.team, rush_ev.team);


