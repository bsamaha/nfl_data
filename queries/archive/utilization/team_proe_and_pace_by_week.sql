-- queries/utilization/team_proe_and_pace_by_week.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type
), base AS (
  SELECT year AS season, week, season_type, game_id, posteam AS team,
         pass::INT AS is_pass,
         xpass,
         no_huddle::INT AS is_no_huddle,
         shotgun::INT AS is_shotgun,
         half_seconds_remaining
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
  WHERE year=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND qb_dropback = 1
    AND half_seconds_remaining > 120
)
SELECT season, week, season_type, team,
       AVG(is_pass) - AVG(xpass) AS proe_neutral,
       30.0 / NULLIF(COUNT(*),0) * 60.0 AS sec_per_play_neutral
FROM base
GROUP BY season, week, season_type, team
ORDER BY season, week, team;


