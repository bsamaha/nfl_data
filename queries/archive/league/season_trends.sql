-- NFL league season trends (pace, PROE, formation, efficiency, scoring)
-- Usage: run inside an environment where a DuckDB view `pbp` is registered
-- (e.g., via the notebook data-connection cell), or replace `FROM pbp` with
-- read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true) if needed.

WITH plays AS (
  SELECT
    CAST(season AS INT) AS season,
    game_id,
    play_id,
    order_sequence,
    CAST(pass AS INT) AS is_pass,
    CAST(rush AS INT) AS is_rush,
    CAST(shotgun AS INT) AS is_shotgun,
    CAST(no_huddle AS INT) AS is_no_huddle,
    air_yards,
    epa,
    CAST(success AS INT) AS is_success,
    yards_gained,
    xpass,
    pass_oe
  FROM pbp
  WHERE season >= 1999 AND season_type = 'REG' AND COALESCE(play_deleted, 0) = 0 AND play = 1
), by_game AS (
  SELECT
    season,
    game_id,
    COUNT(*) AS plays,
    SUM(is_pass) AS pass_plays,
    SUM(is_rush) AS rush_plays,
    AVG(is_shotgun) AS shotgun_rate,
    AVG(is_no_huddle) AS no_huddle_rate,
    AVG(air_yards) AS air_yards,
    AVG(epa) AS epa_play,
    AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush,
    AVG(is_success) AS success_rate,
    AVG(CASE WHEN is_pass=1 THEN is_success END) AS pass_success,
    AVG(CASE WHEN is_rush=1 THEN is_success END) AS rush_success,
    AVG(xpass) AS xpass,
    AVG(pass_oe) AS pass_oe,
    AVG(CASE WHEN yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_rate
  FROM plays
  GROUP BY season, game_id
), pace AS (
  SELECT
    p.season,
    p.game_id,
    MEDIAN(delta) FILTER (WHERE delta BETWEEN 0 AND 45) AS median_sec_per_play,
    AVG(delta)    FILTER (WHERE delta BETWEEN 0 AND 45) AS mean_sec_per_play
  FROM (
    SELECT
      season,
      game_id,
      order_sequence,
      LAG(game_seconds_remaining) OVER (PARTITION BY season, game_id ORDER BY order_sequence) - game_seconds_remaining AS delta
    FROM (
      SELECT CAST(season AS INT) AS season, game_id, order_sequence, game_seconds_remaining
      FROM pbp
      WHERE season >= 1999 AND season_type='REG' AND COALESCE(play_deleted,0)=0 AND play=1
    ) s
  ) p
  GROUP BY p.season, p.game_id
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
  g.season,
  COUNT(DISTINCT g.game_id) AS games,
  AVG(g.plays) AS avg_plays_per_game,
  AVG(g.pass_plays::DOUBLE)/NULLIF(AVG(g.plays::DOUBLE),0) AS pass_rate,
  AVG(g.shotgun_rate) AS shotgun_rate,
  AVG(g.no_huddle_rate) AS no_huddle_rate,
  AVG(g.air_yards) AS air_yards,
  AVG(g.epa_play) AS epa_play,
  AVG(g.epa_pass) AS epa_pass,
  AVG(g.epa_rush) AS epa_rush,
  AVG(g.success_rate) AS success_rate,
  AVG(g.pass_success) AS pass_success,
  AVG(g.rush_success) AS rush_success,
  AVG(g.xpass) AS xpass,
  AVG(g.pass_oe) AS pass_oe,
  AVG(g.explosive_rate) AS explosive_rate,
  AVG(p.median_sec_per_play) AS median_sec_per_play,
  AVG(p.mean_sec_per_play) AS mean_sec_per_play,
  AVG(s.total_points) AS points_per_game
FROM by_game g
LEFT JOIN pace p USING (season, game_id)
LEFT JOIN by_game_scoring s USING (season, game_id)
GROUP BY g.season
ORDER BY g.season;


