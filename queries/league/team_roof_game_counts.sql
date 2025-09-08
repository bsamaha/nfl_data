-- Team × roof game counts per season
-- Output: season, team, roof_class, games
WITH params AS (
  SELECT 2005 AS season_start, 2025 AS season_end, 'REG' AS season_type
),
-- Schedules source (always available earlier for new seasons)
sched AS (
  SELECT
    CAST(s.season AS INT) AS season,
    s.game_id,
    s.home_team,
    s.away_team,
    UPPER(COALESCE(s.roof, '')) AS roof_sched
  FROM read_parquet('data/silver/schedules/season=*/**/*.parquet', union_by_name=true, filename=true) s, params par
  WHERE s.filename NOT ILIKE '%__HIVE_DEFAULT_PARTITION__%'
    AND CAST(s.season AS INT) BETWEEN par.season_start AND par.season_end
    AND s.game_type = par.season_type
),
-- PBP roof labels when available; prefer these when present
pbp_roof AS (
  SELECT
    p.season,
    p.game_id,
    UPPER(MAX(COALESCE(p.roof, ''))) AS roof_pbp
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true) p, params par
  WHERE p.season BETWEEN par.season_start AND par.season_end
    AND p.season_type = par.season_type
    AND p.play = 1
  GROUP BY p.season, p.game_id
),
games AS (
  SELECT
    s.season,
    s.game_id,
    s.home_team,
    s.away_team,
    COALESCE(pr.roof_pbp, s.roof_sched) AS roof_raw
  FROM sched s
  LEFT JOIN pbp_roof pr USING (season, game_id)
),
team_games AS (
  SELECT season, game_id, home_team AS team, roof_raw FROM games
  UNION ALL
  SELECT season, game_id, away_team AS team, roof_raw FROM games
),
classified AS (
  SELECT
    season,
    game_id,
    team,
    CASE
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('INDOORS','INDOOR','DOME','RETRACTABLE-ROOF-CLOSED','RETRACTABLE_CLOSED','CLOSED') THEN 'INDOOR'
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('RETRACTABLE','RETRACTABLE-ROOF-OPEN','RETRACTABLE_OPEN','OPEN') THEN 'RETRACTABLE'
      WHEN UPPER(COALESCE(roof_raw, '')) IN ('OUTDOORS','OUTDOOR') THEN 'OUTDOOR'
      ELSE 'UNKNOWN'
    END AS roof_class
  FROM team_games
),
team_game_ids AS (
  SELECT DISTINCT season, game_id, team, roof_class
  FROM classified
),
counts AS (
  SELECT season, team, roof_class, COUNT(*) AS games
  FROM team_game_ids
  GROUP BY season, team, roof_class
)
SELECT * FROM counts
ORDER BY season, team, roof_class;


