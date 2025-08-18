-- Snap share leaders by position through a given week
-- Usage: duckdb -c "\i queries/snap_share_leaders.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
snaps AS (
  SELECT * FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
),
agg AS (
  SELECT
    pfr_player_id AS player_id,
    MAX(player) AS player_name,
    MAX(team) AS team,
    MAX(position) AS position,
    SUM(CASE WHEN week <= (SELECT thru_week FROM params) THEN offense_snaps ELSE 0 END) AS off_snaps,
    SUM(CASE WHEN week <= (SELECT thru_week FROM params) THEN offense_pct ELSE 0 END) AS off_snap_pct_sum,
    COUNT(CASE WHEN week <= (SELECT thru_week FROM params) THEN 1 END) AS weeks_count
  FROM snaps
  GROUP BY pfr_player_id
)
SELECT
  player_id,
  player_name,
  team,
  position,
  off_snaps,
  off_snap_pct_sum / NULLIF(weeks_count, 0) AS avg_off_snap_pct
FROM agg
WHERE position IN ('QB','RB','WR','TE')
ORDER BY avg_off_snap_pct DESC
LIMIT 200;


