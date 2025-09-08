-- FLEX tier shares by season (pool RB, WR, TE; Tier size = 12)
-- Output: season, tier, share, mean_pts, median_pts
WITH params AS (
  SELECT 2005 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
weekly AS (
  SELECT * FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
),
weekly_pts AS (
  SELECT
    season,
    week,
    player_id,
    UPPER(COALESCE(position, position_group)) AS position,
    0.04 * COALESCE(passing_yards, 0) +
    4    * COALESCE(passing_tds, 0) -
    1    * COALESCE(interceptions, 0) +
    0.10 * (COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0)) +
    6    * (COALESCE(rushing_tds, 0) + COALESCE(receiving_tds, 0)) +
    1    * COALESCE(receptions, 0) +
    6    * COALESCE(special_teams_tds, 0) -
    1    * (COALESCE(rushing_fumbles_lost, 0) + COALESCE(receiving_fumbles_lost, 0) + COALESCE(sack_fumbles_lost, 0)) +
    2    * (COALESCE(passing_2pt_conversions, 0) + COALESCE(rushing_2pt_conversions, 0) + COALESCE(receiving_2pt_conversions, 0))
    AS dk_points
  FROM weekly
),
base AS (
  SELECT season, player_id, position, SUM(COALESCE(dk_points,0)) AS pts
  FROM weekly_pts w, params p
  WHERE w.season BETWEEN p.season_start AND p.season_end
    AND UPPER(position) IN ('RB','WR','TE')
  GROUP BY season, player_id, position
),
ranked AS (
  SELECT
    season,
    player_id,
    pts,
    ROW_NUMBER() OVER (PARTITION BY season ORDER BY pts DESC, player_id) AS rnk,
    COUNT(*)      OVER (PARTITION BY season) AS n_all
  FROM base
),
tiers AS (
  SELECT
    season,
    player_id,
    pts,
    ((rnk - 1) / 12) + 1 AS tier,
    rnk,
    n_all
  FROM ranked
),
totals AS (
  SELECT season, SUM(pts) AS total_pts
  FROM tiers
  GROUP BY season
)
SELECT
  t.season,
  t.tier,
  SUM(t.pts) / NULLIF(tt.total_pts, 0) AS share,
  AVG(t.pts) AS mean_pts,
  quantile(t.pts, 0.50) AS median_pts,
  quantile(t.pts, 0.00) AS min_pts
FROM tiers t
JOIN totals tt USING (season)
GROUP BY t.season, t.tier, tt.total_pts
ORDER BY season, tier;


