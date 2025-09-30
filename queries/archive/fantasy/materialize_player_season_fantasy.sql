-- Materialize player-season fantasy aggregates to Parquet under data/gold/

COPY (
  WITH params AS (
    SELECT 1999 AS season_start, 2100 AS season_end, 'REG' AS season_type
  ),
  weekly AS (
    SELECT * FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
  ),
  base AS (
    SELECT w.*
    FROM weekly w, params p
    WHERE w.season BETWEEN p.season_start AND p.season_end
      AND w.season_type = p.season_type
  ),
  enriched AS (
    SELECT b.*, CAST(NULL AS DOUBLE) AS routes_run, CAST(NULL AS DOUBLE) AS yprr
    FROM base b
  ),
  scored AS (
    SELECT e.*,
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
       -1.0  * (COALESCE(e.rushing_fumbles_lost, 0) + COALESCE(e.receiving_fumbles_lost, 0) + COALESCE(e.sack_fumbles_lost, 0)) +
        2.0  * (COALESCE(e.passing_2pt_conversions, 0) + COALESCE(e.rushing_2pt_conversions, 0) + COALESCE(e.receiving_2pt_conversions, 0)) +
        CASE WHEN COALESCE(e.passing_yards, 0)  >= 300 THEN 3.0 ELSE 0.0 END +
        CASE WHEN COALESCE(e.rushing_yards, 0)  >= 100 THEN 3.0 ELSE 0.0 END +
        CASE WHEN COALESCE(e.receiving_yards, 0) >= 100 THEN 3.0 ELSE 0.0 END
      ) AS dk_ppr_points
    FROM enriched e
  ),
  agg AS (
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
      SUM(COALESCE(routes_run,0)) AS routes_run,
      CASE WHEN SUM(COALESCE(routes_run,0)) > 0 THEN SUM(COALESCE(receiving_yards,0))::DOUBLE / NULLIF(SUM(COALESCE(routes_run,0)),0) END AS yprr
    FROM scored
    GROUP BY 1,2
  )
  SELECT * FROM agg
) TO 'data/gold/player_season_fantasy'
WITH (FORMAT PARQUET, PARTITION_BY (season));


