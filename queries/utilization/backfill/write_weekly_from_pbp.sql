-- queries/utilization/backfill/write_weekly_from_pbp.sql
-- Minimal weekly backfill derived from PBP (receiving-oriented). Shares/WOPR left NULL.
COPY (
  WITH params AS (
    SELECT CAST(2025 AS INTEGER) AS season,
           CAST('REG' AS VARCHAR) AS season_type
  ), rec AS (
    SELECT year AS season, week, season_type,
           posteam AS team,
           receiver_player_id AS player_id,
           COUNT(*) AS targets,
           SUM(COALESCE(receiving_yards, CASE WHEN pass=1 THEN yards_gained END)) AS receiving_yards,
           SUM(COALESCE(air_yards, 0)) AS receiving_air_yards
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
      AND pass = 1 AND receiver_player_id IS NOT NULL
    GROUP BY year, week, season_type, posteam, receiver_player_id
  ), rost AS (
    SELECT season, week, team, player_id,
           COALESCE(player_name, football_name, first_name || ' ' || last_name) AS player_name,
           position
    FROM read_parquet('data/silver/rosters/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
  )
  SELECT r.season, r.week, r.season_type, r.team, r.player_id,
         ro.player_name, ro.position,
         r.targets, r.receiving_yards, r.receiving_air_yards,
         CAST(NULL AS DOUBLE) AS target_share,
         CAST(NULL AS DOUBLE) AS air_yards_share,
         CAST(NULL AS DOUBLE) AS wopr
  FROM rec r
  LEFT JOIN rost ro
    ON ro.season=r.season AND ro.week=r.week AND ro.team=r.team AND ro.player_id=r.player_id
) TO 'data/silver/weekly/part.parquet' (FORMAT PARQUET, PARTITION_BY (season));


