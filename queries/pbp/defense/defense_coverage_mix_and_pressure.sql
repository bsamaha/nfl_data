-- Defensive coverage mix and pressure rate by team
-- Usage: duckdb -c "\i queries/defense_coverage_mix_and_pressure.sql"
WITH params AS (
  SELECT 2024 AS season, 'REG' AS season_type
),
plays AS (
  SELECT defteam, week, was_pressure, defense_man_zone_type, defense_coverage_type, pass
  FROM read_parquet('data/silver/pbp/year=*/*.parquet', union_by_name=true)
  WHERE season = (SELECT season FROM params)
    AND season_type = (SELECT season_type FROM params)
    AND play = 1 AND COALESCE(aborted_play, 0) = 0
    AND pass = 1
)
SELECT
  defteam AS team,
  AVG(CASE WHEN COALESCE(was_pressure, FALSE) THEN 1 ELSE 0 END) AS pressure_rate,
  100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(defense_man_zone_type,'')) LIKE '%man%') / NULLIF(COUNT(*),0) AS man_pct,
  100.0 * COUNT(*) FILTER (WHERE LOWER(COALESCE(defense_man_zone_type,'')) LIKE '%zone%') / NULLIF(COUNT(*),0) AS zone_pct
FROM plays
GROUP BY 1
ORDER BY pressure_rate DESC;


