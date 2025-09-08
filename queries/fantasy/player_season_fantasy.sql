-- Player-season fantasy aggregates with advanced metrics (all seasons)
-- Depends on player_week_fantasy.sql CTE logic; re-implemented inline to avoid dependency chains in DuckDB
-- Usage:
--   scripts/run_query.sh -f queries/fantasy/player_season_fantasy.sql -- -csv | head -50

WITH params AS (
  SELECT
    1999 AS season_start,
    2100 AS season_end,
    'REG' AS season_type
),
weekly AS (
  SELECT *
  FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
),
base AS (
  SELECT
    w.season,
    w.week,
    w.season_type,
    w.player_id,
    w.player_name,
    w.position,
    w.team,
    w.opponent_team,
    w.completions,
    w.attempts,
    w.passing_yards,
    w.passing_tds,
    w.interceptions,
    w.sacks,
    w.sack_yards,
    w.sack_fumbles,
    w.sack_fumbles_lost,
    w.passing_air_yards,
    w.passing_yards_after_catch,
    w.passing_first_downs,
    w.passing_epa,
    w.passing_2pt_conversions,
    w.pacr,
    w.dakota,
    w.carries,
    w.rushing_yards,
    w.rushing_tds,
    w.rushing_fumbles,
    w.rushing_fumbles_lost,
    w.rushing_first_downs,
    w.rushing_epa,
    w.rushing_2pt_conversions,
    w.receptions,
    w.targets,
    w.receiving_yards,
    w.receiving_tds,
    w.receiving_fumbles,
    w.receiving_fumbles_lost,
    w.receiving_air_yards,
    w.receiving_yards_after_catch,
    w.receiving_first_downs,
    w.receiving_epa,
    w.receiving_2pt_conversions,
    w.racr,
    w.target_share,
    w.air_yards_share,
    w.wopr,
    w.special_teams_tds
  FROM weekly w, params par
  WHERE w.season BETWEEN par.season_start AND par.season_end
    AND w.season_type = par.season_type
),
enriched AS (
  SELECT
    b.*,
    CAST(NULL AS DOUBLE) AS routes_run,
    CAST(NULL AS DOUBLE) AS yprr
  FROM base b
),
scored AS (
  SELECT
    e.*,
    (
      0.04 * COALESCE(e.passing_yards, 0) +
      4.0  * COALESCE(e.passing_tds, 0) +
     -1.0  * COALESCE(e.interceptions, 0) +
      0.10 * COALESCE(e.rushing_yards, 0) +
      6.0  * COALESCE(e.rushing_tds, 0) +
      0.10 * COALESCE(e.receiving_yards, 0) +
      6.0  * COALESCE(e.receiving_tds, 0) +
      1.0  * COALESCE(e.receptions, 0) +
      6.0  * COALESCE(e.special_teams_tds, 0) +
     -1.0  * (
               COALESCE(e.rushing_fumbles_lost, 0)
             + COALESCE(e.receiving_fumbles_lost, 0)
             + COALESCE(e.sack_fumbles_lost, 0)
            ) +
      2.0  * (
               COALESCE(e.passing_2pt_conversions, 0)
             + COALESCE(e.rushing_2pt_conversions, 0)
             + COALESCE(e.receiving_2pt_conversions, 0)
            ) +
      CASE WHEN COALESCE(e.passing_yards, 0)  >= 300 THEN 3.0 ELSE 0.0 END +
      CASE WHEN COALESCE(e.rushing_yards, 0)  >= 100 THEN 3.0 ELSE 0.0 END +
      CASE WHEN COALESCE(e.receiving_yards, 0) >= 100 THEN 3.0 ELSE 0.0 END
    ) AS dk_ppr_points
  FROM enriched e
)
SELECT
  season,
  player_id,
  MAX(player_name) AS player_name,
  MAX(position) AS position,
  MAX(team) AS primary_team,
  COUNT(*) AS games,
  SUM(COALESCE(completions,0)) AS completions,
  SUM(COALESCE(attempts,0)) AS attempts,
  SUM(COALESCE(passing_yards,0)) AS passing_yards,
  SUM(COALESCE(passing_tds,0)) AS passing_tds,
  SUM(COALESCE(interceptions,0)) AS interceptions,
  SUM(COALESCE(carries,0)) AS carries,
  SUM(COALESCE(rushing_yards,0)) AS rushing_yards,
  SUM(COALESCE(rushing_tds,0)) AS rushing_tds,
  SUM(COALESCE(receptions,0)) AS receptions,
  SUM(COALESCE(targets,0)) AS targets,
  SUM(COALESCE(receiving_yards,0)) AS receiving_yards,
  SUM(COALESCE(receiving_tds,0)) AS receiving_tds,
  SUM(COALESCE(receiving_air_yards,0)) AS receiving_air_yards,
  SUM(COALESCE(receiving_yards_after_catch,0)) AS receiving_yac,
  SUM(COALESCE(passing_epa,0)) AS passing_epa,
  SUM(COALESCE(rushing_epa,0)) AS rushing_epa,
  SUM(COALESCE(receiving_epa,0)) AS receiving_epa,
  SUM(COALESCE(dk_ppr_points,0)) AS dk_ppr_points,
  NULLIF(0,0) AS routes_run,
  NULLIF(0,0) AS yprr
FROM scored
GROUP BY 1,2
ORDER BY season, dk_ppr_points DESC;

-- To materialize as Parquet:
-- COPY (
--   SELECT * FROM (
--     SELECT * FROM scored
--   )
-- ) TO 'data/gold/player_season_fantasy/season_partitioned.parquet' (FORMAT PARQUET);

