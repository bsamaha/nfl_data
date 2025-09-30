-- League efficiency trends (weekly lines suitable for plotting)
-- Aliases to match research/macro_view spec
WITH base AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
),
params AS (
  SELECT 2005 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
f AS (
  SELECT b.*
  FROM base b, params p
  WHERE b.season BETWEEN p.season_start AND p.season_end
    AND b.season_type = p.season_type
),
plays AS (
  SELECT *,
         COALESCE(pass, 0)::INT AS is_pass,
         COALESCE(rush_attempt, 0)::INT AS is_rush,
         COALESCE(qb_spike, 0)::INT AS is_spike,
         COALESCE(qb_kneel, 0)::INT AS is_kneel,
         COALESCE(play_deleted, 0)::INT AS is_deleted
  FROM f
),
qual AS (
  SELECT * FROM plays WHERE (is_pass=1 OR is_rush=1) AND is_spike=0 AND is_kneel=0 AND is_deleted=0
),
weekly AS (
  SELECT
    season,
    week,
    AVG(epa) AS epa_all,
    AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush
  FROM qual
  GROUP BY season, week
)
SELECT * FROM weekly ORDER BY season, week;


