-- NFL environment splits per season: dome vs outdoor, turf vs grass
-- Usage: assumes a DuckDB view `pbp` exists; otherwise replace `FROM pbp` with
-- read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true).

WITH plays AS (
  SELECT
    CAST(season AS INT) AS season,
    game_id,
    CAST(pass AS INT) AS is_pass,
    CAST(rush AS INT) AS is_rush,
    epa,
    CAST(success AS INT) AS is_success,
    yards_gained,
    COALESCE(roof, game_stadium, stadium, '') AS roof_like,
    surface
  FROM pbp
  WHERE season >= 1999 AND season_type = 'REG' AND COALESCE(play_deleted,0)=0 AND play=1
), by_game AS (
  SELECT
    season,
    game_id,
    AVG(CASE WHEN roof_like ILIKE '%dome%' OR roof_like ILIKE '%closed%' THEN 1 ELSE 0 END) AS is_dome,
    CASE WHEN surface ILIKE '%turf%' OR surface ILIKE '%artificial%' THEN 'turf' ELSE 'grass' END AS surface_class,
    COUNT(*) AS plays,
    AVG(epa) AS epa_play,
    AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush,
    AVG(is_success) AS success_rate,
    AVG(CASE WHEN yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_rate
  FROM plays
  GROUP BY season, game_id, surface_class
), by_game_scoring AS (
  SELECT
    CAST(season AS INT) AS season,
    game_id,
    MAX(total_home_score) + MAX(total_away_score) AS total_points
  FROM pbp
  WHERE season >= 1999 AND season_type='REG'
  GROUP BY season, game_id
)
SELECT
  season,
  CASE WHEN is_dome >= 0.5 THEN 'dome' ELSE 'outdoor' END AS roof_class,
  surface_class,
  COUNT(*) AS games,
  AVG(plays) AS plays_per_game,
  AVG(epa_play) AS epa_play,
  AVG(epa_pass) AS epa_pass,
  AVG(epa_rush) AS epa_rush,
  AVG(success_rate) AS success_rate,
  AVG(explosive_rate) AS explosive_rate,
  AVG(s.total_points) AS points_per_game
FROM by_game g
LEFT JOIN by_game_scoring s USING (season, game_id)
GROUP BY season, roof_class, surface_class
ORDER BY season, roof_class, surface_class;


