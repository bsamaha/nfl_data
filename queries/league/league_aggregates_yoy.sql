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


