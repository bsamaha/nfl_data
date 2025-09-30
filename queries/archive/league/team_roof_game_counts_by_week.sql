-- Team × week × roof game counts per season (home/away sides)
-- Output: season, week, team, side ('HOME'|'AWAY'), roof_class, games
-- Notes:
-- - Uses schedules as the base (available earlier), with PBP roof as a preferred override when present
-- - For future-looking schedules (season >= 2025), treat any retractable roof (OPEN/CLOSED) as INDOOR

WITH params AS (
  SELECT 2005 AS season_start, 2025 AS season_end, 'REG' AS season_type
),
sched AS (
  SELECT
    CAST(s.season AS INT) AS season,
    CAST(s.week AS INT) AS week,
    s.game_id,
    s.home_team,
    s.away_team,
    UPPER(COALESCE(s.roof, '')) AS roof_sched
  FROM read_parquet('data/silver/schedules/season=*/**/*.parquet', union_by_name=true, filename=true) s, params par
  WHERE s.filename NOT ILIKE '%__HIVE_DEFAULT_PARTITION__%'
    AND CAST(s.season AS INT) BETWEEN par.season_start AND par.season_end
    AND s.game_type = par.season_type
),
pbp_roof AS (
  SELECT
    p.season,
    p.week,
    p.game_id,
    UPPER(MAX(COALESCE(p.roof, ''))) AS roof_pbp
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true) p, params par
  WHERE p.season BETWEEN par.season_start AND par.season_end
    AND p.season_type = par.season_type
    AND p.play = 1
  GROUP BY p.season, p.week, p.game_id
),
games AS (
  SELECT
    s.season,
    s.week,
    s.game_id,
    s.home_team,
    s.away_team,
    COALESCE(pr.roof_pbp, s.roof_sched) AS roof_raw
  FROM sched s
  LEFT JOIN pbp_roof pr USING (season, game_id)
),
team_games AS (
  SELECT season, week, game_id, home_team AS team, 'HOME' AS side, roof_raw FROM games
  UNION ALL
  SELECT season, week, game_id, away_team AS team, 'AWAY' AS side, roof_raw FROM games
),
classified AS (
  SELECT
    season,
    week,
    game_id,
    team,
    side,
    CASE
      WHEN season >= 2025 AND (
        UPPER(COALESCE(roof_raw, '')) IN (
          'RETRACTABLE','RETRACTABLE-ROOF-OPEN','RETRACTABLE_OPEN','OPEN',
          'RETRACTABLE-ROOF-CLOSED','RETRACTABLE_CLOSED','CLOSED'
        )
      ) THEN 'INDOOR'
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('INDOORS','INDOOR','DOME','RETRACTABLE-ROOF-CLOSED','RETRACTABLE_CLOSED','CLOSED') THEN 'INDOOR'
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('RETRACTABLE','RETRACTABLE-ROOF-OPEN','RETRACTABLE_OPEN','OPEN') THEN 'RETRACTABLE'
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('OUTDOORS','OUTDOOR') THEN 'OUTDOOR'
      ELSE 'UNKNOWN'
    END AS roof_class
  FROM team_games
),
counts AS (
  SELECT season, week, team, side, roof_class, COUNT(DISTINCT game_id) AS games
  FROM classified
  GROUP BY season, week, team, side, roof_class
)
SELECT * FROM counts
ORDER BY season, week, team, side, roof_class;


