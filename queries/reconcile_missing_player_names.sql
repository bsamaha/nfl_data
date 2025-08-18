-- Reconcile missing weekly.player_name by joining seasonal rosters, players, and PBP-derived names
-- Parameters: :start_season, :end_season

WITH w AS (
  SELECT DISTINCT season, player_id, team
  FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
  WHERE season BETWEEN COALESCE(:start_season, 1999) AND COALESCE(:end_season, 2005)
    AND player_name IS NULL
    AND player_id IS NOT NULL
),
r_seasonal AS (
  SELECT season, player_id,
         COALESCE(player_name, first_name || ' ' || last_name) AS name_r
  FROM read_parquet('data/silver/rosters_seasonal/season=*/**/*.parquet')
),
pbp_pairs AS (
  SELECT season,
         player_id,
         player_name_src AS name_pbp
  FROM (
    SELECT season,
           rusher_player_id  AS player_id,
           rusher_player_name AS player_name_src
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
    UNION ALL
    SELECT season, receiver_player_id, receiver_player_name
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
    UNION ALL
    SELECT season, passer_player_id, passer_player_name
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
  ) t
  WHERE player_id IS NOT NULL AND player_name_src IS NOT NULL
),
pbp_mode AS (
  -- choose the most frequent PBP name per (season, player)
  SELECT season, player_id,
         name_pbp AS name_pbp
  FROM (
    SELECT season, player_id, name_pbp,
           ROW_NUMBER() OVER (PARTITION BY season, player_id ORDER BY COUNT(*) DESC, name_pbp) AS rn
    FROM pbp_pairs
    GROUP BY season, player_id, name_pbp
  ) s WHERE rn=1
),
joined AS (
  SELECT w.season, w.player_id,
         COALESCE(r.name_r, m.name_pbp) AS best_name,
         r.name_r, m.name_pbp,
         w.team
  FROM w
  LEFT JOIN r_seasonal r USING (season, player_id)
  LEFT JOIN pbp_mode m USING (season, player_id)
)
SELECT *
FROM joined
ORDER BY season, player_id;


