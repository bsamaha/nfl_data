-- reports/materialize_player_week_utilization_rushing.sql
-- Rushing-focused weekly utilization per player

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
  ), ctx AS (
    SELECT year AS season, week, season_type, posteam AS team,
           SUM(CASE WHEN rush_attempt=1 THEN 1 ELSE 0 END) AS team_carries,
           SUM(CASE WHEN rush_attempt=1 AND yardline_100 <= 20 THEN 1 ELSE 0 END) AS team_rz20_carries,
           SUM(CASE WHEN rush_attempt=1 AND yardline_100 <= 10 THEN 1 ELSE 0 END) AS team_rz10_carries,
           SUM(CASE WHEN rush_attempt=1 AND yardline_100 <=  5 THEN 1 ELSE 0 END) AS team_rz5_carries
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
    GROUP BY year, week, season_type, posteam
  )
  SELECT
    w.season,
    w.week,
    w.season_type,
    w.team,
    w.player_id,
    MAX(w.player_name) AS player_name,
    MAX(w.position) AS position,
    COALESCE(MAX(rush_ev.carries),0) AS carries,
    CASE WHEN MAX(ctx.team_carries) > 0 THEN COALESCE(MAX(rush_ev.carries),0)::DOUBLE / NULLIF(MAX(ctx.team_carries),0) END AS carry_share,
    COALESCE(MAX(rush_ev.rz20_carries),0) AS rz20_carries,
    COALESCE(MAX(rush_ev.rz10_carries),0) AS rz10_carries,
    COALESCE(MAX(rush_ev.rz5_carries),0)  AS rz5_carries,
    CASE WHEN MAX(ctx.team_rz20_carries) > 0 THEN COALESCE(MAX(rush_ev.rz20_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_rz20_carries),0) ELSE 0.0 END AS rz20_carry_share,
    CASE WHEN MAX(ctx.team_rz10_carries) > 0 THEN COALESCE(MAX(rush_ev.rz10_carries),0)::DOUBLE / NULLIF(MAX(ctx.team_rz10_carries),0) ELSE 0.0 END AS rz10_carry_share,
    CASE WHEN MAX(ctx.team_rz5_carries)  > 0 THEN COALESCE(MAX(rush_ev.rz5_carries),0)::DOUBLE  / NULLIF(MAX(ctx.team_rz5_carries),0)  ELSE 0.0 END AS rz5_carry_share
  FROM w
  LEFT JOIN rush_ev ON rush_ev.season=w.season AND rush_ev.week=w.week AND rush_ev.season_type=w.season_type AND rush_ev.team=w.team AND rush_ev.player_id=w.player_id
  LEFT JOIN ctx     ON ctx.season=w.season AND ctx.week=w.week AND ctx.season_type=w.season_type AND ctx.team=w.team
  GROUP BY w.season, w.week, w.season_type, w.team, w.player_id
) TO 'data/gold/reports/player_week_utilization_rushing'
WITH (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


