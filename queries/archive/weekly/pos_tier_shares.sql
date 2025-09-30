-- Position tier shares by season (Tier size = 12)
-- Output columns (for CSV):
--   season, position, tier, share, mean_pts, median_pts, iqr_pts, top12_flag, top24_flag, top36_flag
WITH params AS (
  SELECT 2005 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
weekly AS (
  SELECT * FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
),
-- Compute DK PPR weekly points to ensure consistent scoring
weekly_pts AS (
  SELECT
    season,
    week,
    player_id,
    UPPER(COALESCE(position, position_group)) AS position,
    -- DraftKings PPR scoring
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
    AND UPPER(position) IN ('QB','RB','WR','TE')
  GROUP BY season, player_id, position
),
ranked AS (
  SELECT
    season,
    position,
    player_id,
    pts,
    ROW_NUMBER() OVER (PARTITION BY season, position ORDER BY pts DESC, player_id) AS rnk,
    COUNT(*)      OVER (PARTITION BY season, position) AS n_pos
  FROM base
),
tiers AS (
  SELECT
    season,
    position,
    player_id,
    pts,
    ((rnk - 1) / 12) + 1 AS tier,
    rnk,
    n_pos
  FROM ranked
),
pos_totals AS (
  SELECT season, position, SUM(pts) AS pos_pts
  FROM tiers
  GROUP BY season, position
),
agg AS (
  SELECT
    t.season,
    t.position,
    t.tier,
    SUM(t.pts) / NULLIF(pt.pos_pts, 0) AS share,
    AVG(t.pts) AS mean_pts,
    quantile(t.pts, 0.50) AS median_pts,
    quantile(t.pts, 0.00) AS min_pts,
    (quantile(t.pts, 0.75) - quantile(t.pts, 0.25)) AS iqr_pts,
    MAX(CASE WHEN t.rnk <= 12 THEN 1 ELSE 0 END) AS top12_flag,
    MAX(CASE WHEN t.rnk <= 24 THEN 1 ELSE 0 END) AS top24_flag,
    MAX(CASE WHEN t.rnk <= 36 THEN 1 ELSE 0 END) AS top36_flag
  FROM tiers t
  JOIN pos_totals pt
    ON pt.season = t.season AND pt.position = t.position
  GROUP BY t.season, t.position, t.tier, pt.pos_pts
)
SELECT season, position, tier, share, mean_pts, median_pts, min_pts, iqr_pts, top12_flag, top24_flag, top36_flag
FROM agg
ORDER BY season, position, tier;


