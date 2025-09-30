-- Materialize player-week fantasy dataset to Parquet under data/gold/
-- Requires DuckDB >= 0.9 for PARTITION_BY

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
       -2.0  * COALESCE(e.interceptions, 0) +
        0.10 * COALESCE(e.rushing_yards, 0) +
        6.0  * COALESCE(e.rushing_tds, 0) +
        0.10 * COALESCE(e.receiving_yards, 0) +
        6.0  * COALESCE(e.receiving_tds, 0) +
        1.0  * COALESCE(e.receptions, 0) +
       -2.0  * (COALESCE(e.rushing_fumbles_lost, 0) + COALESCE(e.receiving_fumbles_lost, 0) + COALESCE(e.sack_fumbles_lost, 0))
      ) AS ppr_points,
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
  )
  SELECT * FROM scored
) TO 'data/gold/player_week_fantasy'
WITH (FORMAT PARQUET, PARTITION_BY (season, week), OVERWRITE_OR_IGNORE 1);


