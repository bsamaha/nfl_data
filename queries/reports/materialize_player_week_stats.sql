-- reports/materialize_player_week_stats.sql
-- Materialize per-player weekly stats + DraftKings/PPR scoring and select advanced metrics
-- Output: data/gold/reports/player_week_stats (partitioned by season, week)

COPY (
  WITH params AS (
    SELECT 2025 AS season,
           'REG' AS season_type
  ), weekly AS (
    SELECT *
    FROM read_parquet('data/silver/weekly/season=*/**/*.parquet', union_by_name=true)
    WHERE season = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
  ), weekly_norm AS (
    -- Pick one row per player-week-team to avoid duplicate sources in weekly
    SELECT
      w.season, w.week, w.season_type,
      w.team, w.player_id, w.player_name, w.position,
      COALESCE(w.targets, 0) AS targets,
      COALESCE(w.receptions, 0) AS receptions,
      COALESCE(w.receiving_yards, 0) AS receiving_yards,
      COALESCE(w.receiving_tds, 0) AS receiving_tds,
      COALESCE(w.receiving_air_yards, 0) AS receiving_air_yards,
      COALESCE(w.passing_yards, 0) AS passing_yards,
      COALESCE(w.passing_tds, 0) AS passing_tds,
      COALESCE(w.interceptions, 0) AS interceptions,
      COALESCE(w.rushing_yards, 0) AS rushing_yards,
      COALESCE(w.rushing_tds, 0) AS rushing_tds,
      COALESCE(w.rushing_fumbles_lost, 0) AS rushing_fumbles_lost,
      COALESCE(w.receiving_fumbles_lost, 0) AS receiving_fumbles_lost,
      COALESCE(w.sack_fumbles_lost, 0) AS sack_fumbles_lost,
      COALESCE(w.special_teams_tds, 0) AS special_teams_tds,
      COALESCE(w.passing_2pt_conversions, 0) AS passing_2pt_conversions,
      COALESCE(w.rushing_2pt_conversions, 0) AS rushing_2pt_conversions,
      COALESCE(w.receiving_2pt_conversions, 0) AS receiving_2pt_conversions,
      ROW_NUMBER() OVER (
        PARTITION BY w.season, w.week, w.season_type, w.team, w.player_id
        ORDER BY w.source NULLS LAST
      ) AS rn
    FROM weekly w
  ), base AS (
    SELECT * EXCLUDE (rn)
    FROM weekly_norm
    WHERE rn = 1
  ), adot_src AS (
    -- aDOT from PBP air_yards on targets
    SELECT year AS season, week, season_type, posteam AS team,
           receiver_player_id AS player_id,
           SUM(CASE WHEN pass=1 AND receiver_player_id IS NOT NULL AND air_yards IS NOT NULL THEN air_yards ELSE 0 END) AS sum_air_yards,
           SUM(CASE WHEN pass=1 AND receiver_player_id IS NOT NULL AND air_yards IS NOT NULL THEN 1 ELSE 0 END) AS cnt_air_targets
    FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
    WHERE year = (SELECT season FROM params)
      AND season_type = (SELECT season_type FROM params)
    GROUP BY year, week, season_type, posteam, receiver_player_id
  ), xfp AS (
    -- Simple expected fantasy points proxy
    WITH events AS (
      SELECT year AS season, week, season_type, posteam AS team,
             receiver_player_id AS rec_id,
             rusher_player_id   AS rush_id,
             pass, rush, yardline_100, air_yards
      FROM read_parquet('data/silver/pbp/year=*/**/*.parquet', union_by_name=true)
      WHERE year = (SELECT season FROM params) AND season_type = (SELECT season_type FROM params)
    ), recv AS (
      SELECT season, week, season_type, team, rec_id AS player_id,
             SUM(CASE WHEN pass=1 THEN 1 ELSE 0 END) AS targets,
             SUM(CASE WHEN pass=1 AND air_yards >= 15 THEN 0.6 WHEN pass=1 THEN 0.4 ELSE 0 END) AS xfp_rec
      FROM events
      GROUP BY 1,2,3,4,5
    ), rush AS (
      SELECT season, week, season_type, team, rush_id AS player_id,
             COUNT(*) AS carries,
             SUM(CASE WHEN rush=1 AND yardline_100 <= 5 THEN 0.8 WHEN rush=1 AND yardline_100 <= 10 THEN 0.5 WHEN rush=1 AND yardline_100 <= 20 THEN 0.3 ELSE 0.15 END) AS xfp_rush
      FROM events
      WHERE rush=1
      GROUP BY 1,2,3,4,5
    )
    SELECT COALESCE(r.season, v.season) AS season,
           COALESCE(r.week, v.week) AS week,
           COALESCE(r.season_type, v.season_type) AS season_type,
           COALESCE(r.team, v.team) AS team,
           COALESCE(r.player_id, v.player_id) AS player_id,
           COALESCE(r.xfp_rush, 0) + COALESCE(v.xfp_rec, 0) AS xfp_lite
    FROM rush r
    FULL OUTER JOIN recv v USING (season, week, season_type, team, player_id)
  ), scored AS (
    -- Compute fantasy scoring
    SELECT b.*,
      (
        0.04 * COALESCE(b.passing_yards, 0) +
        4.0  * COALESCE(b.passing_tds, 0) +
       -2.0  * COALESCE(b.interceptions, 0) +
        0.10 * COALESCE(b.rushing_yards, 0) +
        6.0  * COALESCE(b.rushing_tds, 0) +
        0.10 * COALESCE(b.receiving_yards, 0) +
        6.0  * COALESCE(b.receiving_tds, 0) +
        1.0  * COALESCE(b.receptions, 0) +
       -2.0  * (COALESCE(b.rushing_fumbles_lost, 0) + COALESCE(b.receiving_fumbles_lost, 0) + COALESCE(b.sack_fumbles_lost, 0))
      ) AS ppr_points,
      (
        0.04 * COALESCE(b.passing_yards, 0) +
        4.0  * COALESCE(b.passing_tds, 0) +
       -1.0  * COALESCE(b.interceptions, 0) +
        0.10 * COALESCE(b.rushing_yards, 0) +
        6.0  * COALESCE(b.rushing_tds, 0) +
        0.10 * COALESCE(b.receiving_yards, 0) +
        6.0  * COALESCE(b.receiving_tds, 0) +
        1.0  * COALESCE(b.receptions, 0) +
        6.0  * COALESCE(b.special_teams_tds, 0) +
       -1.0  * (COALESCE(b.rushing_fumbles_lost, 0) + COALESCE(b.receiving_fumbles_lost, 0) + COALESCE(b.sack_fumbles_lost, 0)) +
        2.0  * (COALESCE(b.passing_2pt_conversions, 0) + COALESCE(b.rushing_2pt_conversions, 0) + COALESCE(b.receiving_2pt_conversions, 0)) +
        CASE WHEN COALESCE(b.passing_yards, 0)  >= 300 THEN 3.0 ELSE 0.0 END +
        CASE WHEN COALESCE(b.rushing_yards, 0)  >= 100 THEN 3.0 ELSE 0.0 END +
        CASE WHEN COALESCE(b.receiving_yards, 0) >= 100 THEN 3.0 ELSE 0.0 END
      ) AS dk_ppr_points
    FROM base b
  ), joined AS (
    SELECT
      s.*,
      CAST(NULL AS BIGINT) AS routes_run,
      CAST(NULL AS DOUBLE) AS yprr,
      CAST(NULL AS DOUBLE) AS tprr,
      CASE WHEN a.cnt_air_targets > 0 THEN a.sum_air_yards::DOUBLE / NULLIF(a.cnt_air_targets,0) END AS adot,
      x.xfp_lite
    FROM scored s
    LEFT JOIN adot_src a ON a.season=s.season AND a.week=s.week AND a.season_type=s.season_type AND a.team=s.team AND a.player_id=s.player_id
    LEFT JOIN xfp x ON x.season=s.season AND x.week=s.week AND x.season_type=s.season_type AND x.team=s.team AND x.player_id=s.player_id
  )
  SELECT * FROM joined
) TO 'data/gold/reports/player_week_stats'
WITH (FORMAT PARQUET, PARTITION_BY (season, week), OVERWRITE_OR_IGNORE 1);




