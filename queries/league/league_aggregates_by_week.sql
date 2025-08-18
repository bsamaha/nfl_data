-- League macro-level aggregates by week
-- Usage:
--   scripts/run_query.sh -f queries/league_aggregates_by_week.sql -s 2024 -t REG

WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
pbp AS (
  SELECT season, season_type, game_id, week, posteam, defteam,
         pass, rush, epa, success, yards_gained,
         touchdown, pass_touchdown, rush_touchdown,
         play, aborted_play,
         home_team, away_team, total_home_score, total_away_score,
         roof, CAST(down AS INT) AS down
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season_type = (SELECT season_type FROM params)
    AND season = (SELECT season FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
),
games AS (
  SELECT DISTINCT season, week, game_id,
         MAX(total_home_score) AS home_pts,
         MAX(total_away_score) AS away_pts,
         MAX(roof) AS roof
  FROM pbp
  GROUP BY 1,2,3
),
team_game AS (
  SELECT season, week, game_id, posteam AS team,
         COUNT(*) AS plays,
         SUM(CASE WHEN pass = 1 THEN 1 ELSE 0 END) AS pass_plays,
         SUM(CASE WHEN rush = 1 THEN 1 ELSE 0 END) AS rush_plays,
         SUM(CASE WHEN pass = 1 AND yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_passes,
         SUM(CASE WHEN rush = 1 AND yards_gained >= 10 THEN 1 ELSE 0 END) AS explosive_rushes,
         SUM(CASE WHEN touchdown = 1 THEN 1 ELSE 0 END) AS tds
  FROM pbp
  GROUP BY 1,2,3,4
)
SELECT
  tg.season,
  tg.week,
  AVG(tg.plays) AS avg_team_plays_per_game,
  median(tg.plays) AS med_team_plays_per_game,
  AVG(tg.pass_plays) AS avg_pass_plays_per_game,
  median(tg.pass_plays) AS med_pass_plays_per_game,
  AVG(tg.explosive_passes) AS avg_explosive_passes_per_game,
  median(tg.explosive_passes) AS med_explosive_passes_per_game,
  AVG(tg.explosive_rushes) AS avg_explosive_rushes_per_game,
  median(tg.explosive_rushes) AS med_explosive_rushes_per_game,
  AVG(tg.tds) AS avg_team_tds_per_game,
  median(tg.tds) AS med_team_tds_per_game,
  AVG(g.home_pts) AS avg_home_points,
  median(g.home_pts) AS med_home_points,
  AVG(g.away_pts) AS avg_away_points,
  median(g.away_pts) AS med_away_points,
  AVG(g.home_pts + g.away_pts) AS avg_total_points,
  median(g.home_pts + g.away_pts) AS med_total_points,
  AVG(CASE WHEN LOWER(g.roof) IN ('dome','closed') THEN g.home_pts + g.away_pts END) AS avg_points_dome,
  median(CASE WHEN LOWER(g.roof) IN ('dome','closed') THEN g.home_pts + g.away_pts END) AS med_points_dome,
  AVG(CASE WHEN LOWER(g.roof) NOT IN ('dome','closed') THEN g.home_pts + g.away_pts END) AS avg_points_outdoor,
  median(CASE WHEN LOWER(g.roof) NOT IN ('dome','closed') THEN g.home_pts + g.away_pts END) AS med_points_outdoor
FROM team_game tg
JOIN games g USING(season, week, game_id)
GROUP BY 1,2
ORDER BY week;


