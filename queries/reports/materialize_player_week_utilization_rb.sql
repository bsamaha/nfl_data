-- reports/materialize_player_week_utilization_rb.sql
-- RB utilization (rushing + receiving) per player-week

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
    WHERE rn = 1 AND position='RB'
  ), rush_ev AS (
    SELECT year AS season, week, season_type, posteam AS team,
           rusher_player_id AS player_id,
           COUNT(*) AS carries,
           SUM(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END) AS rz20_carries,
           SUM(CASE WHEN yardline_100 <= 10 THEN 1 ELSE 0 END) AS rz10_carries,
           SUM(CASE WHEN yardline_100 <=  5 THEN 1 ELSE 0 END) AS rz5_carries,
           SUM(CASE WHEN down IN (3,4) THEN 1 ELSE 0 END) AS third_fourth_down_carries
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
      AND rush = 1
    GROUP BY year, week, season_type, posteam, rusher_player_id
  ), rec_ev AS (
    WITH plays AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS player_id,
             half_seconds_remaining,
             CASE WHEN pass=1 AND receiver_player_id IS NOT NULL THEN 1 ELSE 0 END AS is_target
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
    )
    SELECT season, week, season_type, team, player_id,
           SUM(is_target) AS targets,
           SUM(CASE WHEN half_seconds_remaining <= 120 AND is_target=1 THEN 1 ELSE 0 END) AS two_minute_targets
    FROM plays
    GROUP BY season, week, season_type, team, player_id
  ), ctx AS (
    SELECT year AS season, week, season_type, posteam AS team,
           SUM(CASE WHEN pass_attempt=1 AND sack=0 THEN 1 ELSE 0 END) AS team_pass_attempts,
           SUM(CASE WHEN rush_attempt=1 THEN 1 ELSE 0 END) AS team_carries
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
    GROUP BY year, week, season_type, posteam
  ), team_style AS (
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
    -- rushing utilization
    COALESCE(MAX(rush_ev.carries),0) AS carries,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS carry_share,
    COALESCE(MAX(rush_ev.rz20_carries),0) AS rz20_carries,
    COALESCE(MAX(rush_ev.rz10_carries),0) AS rz10_carries,
    COALESCE(MAX(rush_ev.rz5_carries),0)  AS rz5_carries,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz20_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz20_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz10_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS rz10_carry_share,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.rz5_carries),0)::DOUBLE  / NULLIF(MAX(ctx.team_carries),0) END AS rz5_carry_share,
    COALESCE(MAX(rush_ev.third_fourth_down_carries),0) AS third_fourth_down_carries,
    -- receiving involvement for RBs
    COALESCE(MAX(rec_ev.targets),0) AS targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS target_share,
    COALESCE(MAX(rec_ev.two_minute_targets),0) AS two_minute_targets,
    CASE WHEN MAX(ctx.team_pass_attempts) > 0 THEN COALESCE(MAX(rec_ev.two_minute_targets),0)::DOUBLE / NULLIF(MAX(ctx.team_pass_attempts),0) END AS two_minute_target_share,
    -- team context
    MAX(team_style.proe_neutral) AS team_proe_neutral,
    MAX(team_style.sec_per_play_neutral) AS team_sec_per_play_neutral
  FROM w
  LEFT JOIN rush_ev    ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.team=w.team AND rush_ev.player_id=w.player_id
  LEFT JOIN rec_ev     ON rec_ev.season=w.season AND rec_ev.week=w.week AND rec_ev.season_type=w.season_type AND rec_ev.team=w.team AND rec_ev.player_id=w.player_id
  LEFT JOIN ctx        ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=w.team
  LEFT JOIN team_style ON team_style.season=w.season AND team_style.week=w.week AND team_style.season_type=w.season_type AND team_style.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.team, w.player_id
) TO 'data/gold/reports/player_week_utilization_rb'
WITH (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


