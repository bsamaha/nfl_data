-- Fetch materialized player-week fantasy data from data/gold/player_week_fantasy
-- Usage examples:
--   duckdb -c "\i queries/fantasy/get_player_week_fantasy.sql"
--   scripts/run_query.sh -f queries/fantasy/get_player_week_fantasy.sql -- -csv | head -20

WITH params AS (
  SELECT
    NULL::INTEGER AS season_filter,
    NULL::INTEGER AS week_filter,
    'REG'::VARCHAR AS season_type_filter
)
SELECT
  *
FROM read_parquet('data/gold/player_week_fantasy/season=*/week=*/**/*.parquet') t, params p
WHERE (p.season_filter IS NULL OR t.season = p.season_filter)
  AND (p.week_filter   IS NULL OR t.week   = p.week_filter)
  AND (p.season_type_filter IS NULL OR t.season_type = p.season_type_filter)
ORDER BY t.season, t.week, t.position, t.dk_ppr_points DESC, t.player_name;


