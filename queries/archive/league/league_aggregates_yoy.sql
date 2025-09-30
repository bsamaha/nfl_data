-- Season-level league aggregates (YoY). Summaries from weekly league metrics.
WITH params AS (
  SELECT 2005 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
weekly AS (
  -- Reuse the weekly query via DuckDB inclusion would be ideal, but we'll inline a lightweight subset:
  SELECT * FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
),
plays AS (
  SELECT p.*
  FROM weekly p, params par
  WHERE p.season BETWEEN par.season_start AND par.season_end
    AND p.season_type = par.season_type
),
filtered AS (
  SELECT *,
         COALESCE(pass, 0)::INT AS is_pass,
         COALESCE(rush_attempt, 0)::INT AS is_rush,
         COALESCE(qb_spike, 0)::INT AS is_spike,
         COALESCE(qb_kneel, 0)::INT AS is_kneel,
         COALESCE(play_deleted, 0)::INT AS is_deleted
  FROM plays
),
qual AS (
  SELECT * FROM filtered
  WHERE (is_pass = 1 OR is_rush = 1) AND is_spike = 0 AND is_kneel = 0 AND is_deleted = 0
),
game_points AS (
  SELECT season, game_id, MAX(total_home_score)+MAX(total_away_score) AS pts
  FROM plays
  GROUP BY season, game_id
),
season_points AS (
  SELECT season, AVG(pts) AS ppg
  FROM game_points
  GROUP BY season
)
SELECT
  q.season,
  COUNT(*) FILTER (WHERE is_pass=1) / CAST(NULLIF(COUNT(*),0) AS DOUBLE) AS pass_rate,
  AVG(epa) AS epa_all,
  AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
  AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush,
  SUM(CASE WHEN is_pass=1 AND yards_gained>=20 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(SUM(CASE WHEN is_pass=1 THEN 1 ELSE 0 END),0) AS explosive_pass_rate,
  SUM(CASE WHEN is_rush=1 AND yards_gained>=10 THEN 1 ELSE 0 END)::DOUBLE / NULLIF(SUM(CASE WHEN is_rush=1 THEN 1 ELSE 0 END),0) AS explosive_rush10_rate,
  sp.ppg
FROM qual q
JOIN season_points sp USING (season)
GROUP BY q.season, sp.ppg
ORDER BY q.season;

-- League macro-level aggregates year-over-year
-- Usage examples:
--   scripts/run_query.sh -f queries/league_aggregates_yoy.sql -s 2024 -t REG

WITH params AS (
  SELECT 1999 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
pbp AS (
  SELECT season, season_type, game_id, week, posteam, defteam,
         pass, rush, epa, success, yards_gained,
         touchdown, pass_touchdown, rush_touchdown,
         play, aborted_play,
         home_team, away_team, total_home_score, total_away_score,
         roof
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season_type = (SELECT season_type FROM params)
    AND season BETWEEN (SELECT season_start FROM params) AND (SELECT season_end FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
games AS (
  SELECT DISTINCT season, game_id,
         MAX(total_home_score) AS home_pts,
         MAX(total_away_score) AS away_pts,
         MAX(roof) AS roof
  FROM pbp
  GROUP BY 1,2
),
drives AS (
  SELECT season, game_id, posteam AS team,
         COUNT(*) AS plays,
         SUM(CASE WHEN pass = 1 THEN 1 ELSE 0 END) AS pass_plays,
         SUM(CASE WHEN rush = 1 THEN 1 ELSE 0 END) AS rush_plays,
         SUM(CASE WHEN pass = 1 AND yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_passes,
         SUM(CASE WHEN rush = 1 AND yards_gained >= 10 THEN 1 ELSE 0 END) AS explosive_rushes,
         SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END) AS tds,
         SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS pass_tds,
         SUM(CASE WHEN rush_touchdown = 1 THEN 1 ELSE 0 END) AS rush_tds
  FROM pbp
  GROUP BY 1,2,3
),
team_game AS (
  SELECT season, game_id, team,
         plays, pass_plays, rush_plays,
         explosive_passes, explosive_rushes,
         tds, pass_tds, rush_tds
  FROM drives
),
per_season AS (
  SELECT
    season,
    -- per team-game distributions
    AVG(plays) AS avg_team_plays_per_game,
    median(plays) AS med_team_plays_per_game,
    AVG(pass_plays) AS avg_pass_plays_per_game,
    median(pass_plays) AS med_pass_plays_per_game,
    AVG(explosive_passes) AS avg_explosive_passes_per_game,
    median(explosive_passes) AS med_explosive_passes_per_game,
    AVG(explosive_rushes) AS avg_explosive_rushes_per_game,
    median(explosive_rushes) AS med_explosive_rushes_per_game,
    AVG(tds) AS avg_team_tds_per_game,
    median(tds) AS med_team_tds_per_game,
    -- game-level
    AVG(home_pts) AS avg_home_points,
    median(home_pts) AS med_home_points,
    AVG(away_pts) AS avg_away_points,
    median(away_pts) AS med_away_points,
    AVG(home_pts + away_pts) AS avg_total_points,
    median(home_pts + away_pts) AS med_total_points,
    -- roof breakdown
    AVG(CASE WHEN LOWER(roof) IN ('dome', 'closed') THEN home_pts + away_pts END) AS avg_points_dome,
    median(CASE WHEN LOWER(roof) IN ('dome', 'closed') THEN home_pts + away_pts END) AS med_points_dome,
    AVG(CASE WHEN LOWER(roof) NOT IN ('dome', 'closed') THEN home_pts + away_pts END) AS avg_points_outdoor,
    median(CASE WHEN LOWER(roof) NOT IN ('dome', 'closed') THEN home_pts + away_pts END) AS med_points_outdoor
  FROM team_game tg
  JOIN (
    SELECT season, game_id, home_pts, away_pts, roof FROM games
  ) g USING(season, game_id)
  GROUP BY season
),
extra AS (
  -- Add more macro trends
  SELECT
    season,
    AVG(pass_plays * 1.0 / NULLIF(plays,0)) AS avg_pass_rate,
    median(pass_plays * 1.0 / NULLIF(plays,0)) AS med_pass_rate,
    AVG(explosive_passes * 1.0 / NULLIF(plays,0)) AS avg_explosive_play_rate,
    AVG(explosive_rushes * 1.0 / NULLIF(plays,0)) AS avg_explosive_rush_rate
  FROM team_game
  GROUP BY season
)
SELECT
  p.season,
  p.avg_team_plays_per_game,
  p.med_team_plays_per_game,
  p.avg_home_points,
  p.med_home_points,
  p.avg_away_points,
  p.med_away_points,
  p.avg_total_points,
  p.med_total_points,
  p.avg_points_dome,
  p.med_points_dome,
  p.avg_points_outdoor,
  p.med_points_outdoor,
  p.avg_pass_plays_per_game,
  p.med_pass_plays_per_game,
  p.avg_explosive_passes_per_game,
  p.med_explosive_passes_per_game,
  p.avg_explosive_rushes_per_game,
  p.med_explosive_rushes_per_game,
  p.avg_team_tds_per_game,
  p.med_team_tds_per_game,
  e.avg_pass_rate,
  e.med_pass_rate,
  e.avg_explosive_play_rate,
  e.avg_explosive_rush_rate
FROM per_season p
JOIN extra e USING(season)
ORDER BY season;


