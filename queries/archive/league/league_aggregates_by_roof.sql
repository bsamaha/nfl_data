-- League aggregates split by roof class (indoor vs outdoor, with retractable shown separately)
-- Output: season, week, roof_class, plays_pg, ppg, epa_all, epa_pass, epa_rush, sec_per_play_neutral, sec_per_play_all,
--         no_huddle_rate, shotgun_rate, explosive_pass_rate, explosive_rush10_rate, explosive_rush15_rate
WITH params AS (
  SELECT 2005 AS season_start, 2025 AS season_end, 'REG' AS season_type
),
pbp AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
),
plays AS (
  SELECT
    p.*,
    UPPER(COALESCE(p.roof, 'UNKNOWN')) AS roof_raw,
    CASE
      WHEN UPPER(COALESCE(p.roof, '')) IN ('INDOORS','INDOOR','DOME','RETRACTABLE-ROOF-CLOSED','RETRACTABLE_CLOSED','CLOSED') THEN 'INDOOR'
      WHEN UPPER(COALESCE(p.roof, '')) IN ('RETRACTABLE','RETRACTABLE-ROOF-OPEN','RETRACTABLE_OPEN') THEN 'RETRACTABLE'
      WHEN UPPER(COALESCE(p.roof, '')) IN ('OUTDOORS','OUTDOOR','OPEN') THEN 'OUTDOOR'
      ELSE 'UNKNOWN'
    END AS roof_class
  FROM pbp p, params par
  WHERE p.season BETWEEN par.season_start AND par.season_end
    AND p.season_type = par.season_type
),
flags AS (
  SELECT *,
         COALESCE(pass, 0)::INT AS is_pass,
         COALESCE(rush_attempt, 0)::INT AS is_rush,
         COALESCE(qb_spike, 0)::INT AS is_spike,
         COALESCE(qb_kneel, 0)::INT AS is_kneel,
         COALESCE(play_deleted, 0)::INT AS is_deleted
  FROM plays
),
qual AS (
  SELECT * FROM flags WHERE (is_pass=1 OR is_rush=1) AND is_spike=0 AND is_kneel=0 AND is_deleted=0
),
-- Build pace gaps per offense and roof_class
with_gaps AS (
  SELECT
    season, week, game_id, posteam, roof_class,
    order_sequence,
    game_seconds_remaining,
    LAG(game_seconds_remaining) OVER (PARTITION BY season, week, game_id, posteam ORDER BY order_sequence) - game_seconds_remaining AS delta_sec,
    is_pass, is_rush, epa, yards_gained, no_huddle, shotgun
  FROM qual
),
gaps AS (
  SELECT *,
         CASE
           WHEN delta_sec IS NULL THEN NULL
           WHEN delta_sec < 0 THEN NULL
           WHEN delta_sec > 45 THEN 45
           ELSE delta_sec
         END AS gap_sec
  FROM with_gaps
),
team_game AS (
  SELECT
    season, week, game_id, posteam, roof_class,
    COUNT(*) AS plays_all,
    AVG(epa) AS epa_all,
    AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush,
    AVG(no_huddle) AS no_huddle_rate,
    AVG(shotgun) AS shotgun_rate,
    SUM(COALESCE(gap_sec,0))/NULLIF(COUNT(gap_sec),0) AS sec_per_play_all
  FROM gaps
  GROUP BY season, week, game_id, posteam, roof_class
),
game_points AS (
  SELECT season, week, game_id, MAX(total_home_score)+MAX(total_away_score) AS game_points
  FROM plays
  GROUP BY season, week, game_id
),
game_tds AS (
  SELECT season, week, game_id,
         SUM(CASE WHEN COALESCE(touchdown, 0) = 1 THEN 1 ELSE 0 END) AS tds_game
  FROM plays
  GROUP BY season, week, game_id
),
explosive AS (
  SELECT
    season, week, roof_class,
    SUM(CASE WHEN is_pass=1 THEN 1 ELSE 0 END) AS pass_plays,
    SUM(CASE WHEN is_rush=1 THEN 1 ELSE 0 END) AS rush_plays,
    SUM(CASE WHEN is_pass=1 AND yards_gained>=20 THEN 1 ELSE 0 END) AS pass_exp_20,
    SUM(CASE WHEN is_rush=1 AND yards_gained>=10 THEN 1 ELSE 0 END) AS rush_exp_10,
    SUM(CASE WHEN is_rush=1 AND yards_gained>=15 THEN 1 ELSE 0 END) AS rush_exp_15
  FROM qual
  GROUP BY season, week, roof_class
)
SELECT
  tg.season,
  tg.week,
  tg.roof_class,
  AVG(tg.plays_all) AS plays_pg,
  AVG(gp.game_points) AS ppg,
  AVG(gt.tds_game) AS tds_pg,
  AVG(tg.epa_all) AS epa_all,
  AVG(tg.epa_pass) AS epa_pass,
  AVG(tg.epa_rush) AS epa_rush,
  AVG(tg.sec_per_play_all) AS sec_per_play_all,
  AVG(tg.no_huddle_rate) AS no_huddle_rate,
  AVG(tg.shotgun_rate) AS shotgun_rate,
  (ex.pass_exp_20::DOUBLE/NULLIF(ex.pass_plays,0)) AS explosive_pass_rate,
  (ex.rush_exp_10::DOUBLE/NULLIF(ex.rush_plays,0)) AS explosive_rush10_rate,
  (ex.rush_exp_15::DOUBLE/NULLIF(ex.rush_plays,0)) AS explosive_rush15_rate
FROM team_game tg
JOIN game_points gp USING (season, week, game_id)
JOIN game_tds gt USING (season, week, game_id)
JOIN explosive ex USING (season, week, roof_class)
GROUP BY tg.season, tg.week, tg.roof_class, ex.pass_exp_20, ex.rush_exp_10, ex.rush_exp_15, ex.pass_plays, ex.rush_plays
ORDER BY season, week, roof_class;


