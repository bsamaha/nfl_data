-- reports/materialize_defense_position_points_allowed.sql
-- Materialize per-defense fantasy points allowed by offensive position (PPR scoring)
-- Output: data/gold/reports/defense_position_points_allowed (partitioned by season, week)

COPY (
  WITH params AS (
    SELECT 2025 AS season,
           'REG' AS season_type
  ), weekly AS (
    SELECT
      season,
      week,
      season_type,
      UPPER(opponent_team) AS defense_team,
      UPPER(position) AS position,
      COALESCE(fantasy_points_ppr, 0.0) AS fantasy_points_ppr
    FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
      AND opponent_team IS NOT NULL
      AND position IN ('QB', 'RB', 'WR', 'TE')
  ), player_points AS (
    SELECT
      season,
      week,
      season_type,
      defense_team,
      position,
      SUM(fantasy_points_ppr) AS points_allowed_ppr
    FROM weekly
    GROUP BY 1,2,3,4,5
  ), defense_games AS (
    SELECT
      season,
      week,
      game_type AS season_type,
      UPPER(home_team) AS defense_team
    FROM read_parquet('data/silver/schedules/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
      AND game_type = (SELECT season_type FROM params)
    UNION ALL
    SELECT
      season,
      week,
      game_type AS season_type,
      UPPER(away_team) AS defense_team
    FROM read_parquet('data/silver/schedules/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
      AND game_type = (SELECT season_type FROM params)
  ), positions AS (
    SELECT DISTINCT position
    FROM weekly
  ), base AS (
    SELECT
      g.season,
      g.week,
      g.season_type,
      g.defense_team,
      p.position,
      COALESCE(pp.points_allowed_ppr, 0.0) AS points_allowed_ppr
    FROM defense_games g
    CROSS JOIN positions p
    LEFT JOIN player_points pp
      ON g.season = pp.season
     AND g.week = pp.week
     AND g.season_type = pp.season_type
     AND g.defense_team = pp.defense_team
     AND p.position = pp.position
    WHERE g.defense_team IS NOT NULL
  ), season_league AS (
    SELECT
      season,
      season_type,
      position,
      AVG(points_allowed_ppr) AS league_avg_points_allowed,
      STDDEV_POP(points_allowed_ppr) AS league_std_points_allowed
    FROM base
    GROUP BY 1,2,3
  ), season_defense AS (
    SELECT
      season,
      season_type,
      defense_team,
      position,
      COUNT(*) AS games_played,
      SUM(points_allowed_ppr) AS total_points_allowed,
      AVG(points_allowed_ppr) AS avg_points_allowed
    FROM base
    GROUP BY 1,2,3,4
  ), final AS (
    SELECT
      b.season,
      b.week,
      b.season_type,
      b.defense_team,
      b.position,
      b.points_allowed_ppr AS points_allowed_ppr,
      sd.games_played,
      sd.total_points_allowed,
      sd.avg_points_allowed,
      sl.league_avg_points_allowed,
      (sd.avg_points_allowed - sl.league_avg_points_allowed) AS avg_vs_league,
      sl.league_std_points_allowed
    FROM base b
    LEFT JOIN season_defense sd
      ON b.season = sd.season
     AND b.season_type = sd.season_type
     AND b.defense_team = sd.defense_team
     AND b.position = sd.position
    LEFT JOIN season_league sl
      ON b.season = sl.season
     AND b.season_type = sl.season_type
     AND b.position = sl.position
  )
  SELECT *
  FROM final
) TO 'data/gold/reports/defense_position_points_allowed'
WITH (FORMAT PARQUET, PARTITION_BY (season, week), OVERWRITE_OR_IGNORE 1);



