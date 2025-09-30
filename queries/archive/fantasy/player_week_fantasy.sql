-- Player-week fantasy dataset with advanced metrics (all available seasons)
-- Reads silver layers and computes DraftKings-style PPR points per catalog/draftkings/bestball.yml
-- Enable NGS weekly (routes_run) in catalog if you want YPRR populated; otherwise YPRR will be NULL
-- Usage examples:
--   scripts/run_query.sh -f queries/fantasy/player_week_fantasy.sql -- -csv | head -50
--   duckdb -c "\i queries/fantasy/player_week_fantasy.sql"

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
    w.player_display_name,
    w.position,
    w.position_group,
    w.team,
    w.opponent_team,
    -- Passing
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
    -- Rushing
    w.carries,
    w.rushing_yards,
    w.rushing_tds,
    w.rushing_fumbles,
    w.rushing_fumbles_lost,
    w.rushing_first_downs,
    w.rushing_epa,
    w.rushing_2pt_conversions,
    -- Receiving
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
    -- Special teams and existing fantasy from source
    w.special_teams_tds,
    w.fantasy_points,
    w.fantasy_points_ppr
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
-- DraftKings Best Ball PPR scoring per catalog/draftkings/bestball.yml
-- Notes: bonuses for 300+ pass yards, 100+ rush yards, 100+ rec yards; INT = -1; fumbles lost = -1
scored AS (
  SELECT
    e.*,
    (
      0.04 * COALESCE(e.passing_yards, 0) +
      4.0  * COALESCE(e.passing_tds, 0) +
     -2.0  * COALESCE(e.interceptions, 0) +
      0.10 * COALESCE(e.rushing_yards, 0) +
      6.0  * COALESCE(e.rushing_tds, 0) +
      0.10 * COALESCE(e.receiving_yards, 0) +
      6.0  * COALESCE(e.receiving_tds, 0) +
      1.0  * COALESCE(e.receptions, 0) +
     -2.0  * (
               COALESCE(e.rushing_fumbles_lost, 0)
             + COALESCE(e.receiving_fumbles_lost, 0)
             + COALESCE(e.sack_fumbles_lost, 0)
            )
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
SELECT *
FROM scored
ORDER BY season, week, position, dk_ppr_points DESC, player_name;

-- To materialize as Parquet (uncomment and run in DuckDB CLI):
-- COPY (
--   SELECT * FROM scored
-- ) TO 'data/gold/player_week_fantasy/season_partitioned.parquet' (FORMAT PARQUET);
-- Or partitioned by season/week via DuckDB table functions:
-- COPY (
--   SELECT * FROM scored
-- ) TO 'data/gold/player_week_fantasy/season=' || CAST(season AS VARCHAR) || '/week=' || CAST(week AS VARCHAR) || '/part.parquet' (FORMAT PARQUET);


