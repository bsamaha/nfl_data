-- queries/utilization/player_pair_correlation.sql
WITH params AS (
  SELECT CAST(2025 AS INTEGER) AS season,
         CAST('REG' AS VARCHAR) AS season_type,
         CAST('DAL' AS VARCHAR) AS team
), src AS (
  SELECT season, week, season_type, team, player_id, dk_ppr_points
  FROM read_parquet('data/gold/player_week_fantasy/season=*/week=*/**/*.parquet', union_by_name=true)
  WHERE season=(SELECT season FROM params) AND season_type=(SELECT season_type FROM params)
    AND team=(SELECT team FROM params)
), pairs AS (
  SELECT a.season, a.season_type, a.team,
         a.player_id AS player_id_a,
         b.player_id AS player_id_b,
         a.dk_ppr_points AS dk_a,
         b.dk_ppr_points AS dk_b
  FROM src a
  JOIN src b USING (season, week, season_type, team)
  WHERE a.player_id < b.player_id
)
SELECT season, season_type, team, 'teammates' AS pair_type,
       player_id_a, player_id_b,
       COUNT(*) AS games,
       AVG(dk_a) AS mean_a,
       AVG(dk_b) AS mean_b,
       COVAR_POP(dk_a, dk_b) AS cov_dk_points,
       CORR(dk_a, dk_b) AS corr_dk_points
FROM pairs
GROUP BY 1,2,3,4,5,6
ORDER BY corr_dk_points DESC, games DESC;


