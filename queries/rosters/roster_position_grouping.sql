-- Position grouping counts by team (rosters)
-- Usage: duckdb -c "\i queries/roster_position_grouping.sql"
WITH params AS (
  SELECT 2024 AS season, 18 AS thru_week
),
rost AS (
  SELECT * FROM read_parquet('data/silver/rosters/season=*/*.parquet')
  WHERE season = (SELECT season FROM params) AND week <= (SELECT thru_week FROM params)
)
SELECT
  team,
  CASE
    WHEN position IN ('QB') THEN 'QB'
    WHEN position IN ('RB','FB') THEN 'RB'
    WHEN position IN ('WR') THEN 'WR'
    WHEN position IN ('TE') THEN 'TE'
    WHEN position IN ('T','G','C','OL') THEN 'OL'
    WHEN position IN ('DE','DT','NT','DL') THEN 'DL'
    WHEN position IN ('LB','ILB','OLB') THEN 'LB'
    WHEN position IN ('CB','S','FS','SS','DB') THEN 'DB'
    WHEN position IN ('K','P','LS') THEN 'ST'
    ELSE 'OTHER'
  END AS pos_group,
  COUNT(DISTINCT player_id) AS unique_players
FROM rost
GROUP BY 1,2
ORDER BY team, pos_group;


