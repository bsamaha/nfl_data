-- Receiving yards leaders through a given week
-- Usage: duckdb -c "\i queries/receiving_yards_leaders.sql"
SELECT player_id, player_name, SUM(receiving_yards) AS yds
FROM read_parquet('data/silver/weekly/season=2025/*.parquet')
WHERE week <= 3
GROUP BY 1,2
ORDER BY yds DESC
LIMIT 50;

