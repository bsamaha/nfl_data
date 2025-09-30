-- queries/utilization/materialize_team_week_context.sql
COPY (
  WITH params AS (
    SELECT CAST(2025 AS INTEGER) AS season,
           CAST('REG' AS VARCHAR) AS season_type
  ), snaps AS (
    SELECT season, week, team, MAX(offense_snaps) AS team_offense_snaps
    FROM read_parquet('data/silver/snap_counts/season=*/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
    GROUP BY season, week, team
  ), pbp AS (
    SELECT year AS season, week, season_type, posteam AS team,
           SUM(CASE WHEN qb_dropback=1 THEN 1 ELSE 0 END)          AS team_dropbacks,
           SUM(CASE WHEN pass_attempt=1 AND sack=0 THEN 1 ELSE 0 END) AS team_pass_attempts,
           SUM(CASE WHEN rush_attempt=1 THEN 1 ELSE 0 END)         AS team_carries
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
    GROUP BY year, week, season_type, posteam
  )
  SELECT p.season, p.week, p.season_type, p.team,
         s.team_offense_snaps, p.team_dropbacks, p.team_pass_attempts, p.team_carries
  FROM pbp p
  LEFT JOIN snaps s
    ON s.season=p.season AND s.week=p.week AND s.team=p.team
) TO 'data/gold/utilization/team_week_context/part.parquet' (FORMAT PARQUET, PARTITION_BY (season, week, season_type, team));


