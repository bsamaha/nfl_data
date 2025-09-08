-- Calendar effects (simplified event-study style): post-bye, short week, mini-bye, prev OT, international
-- Output: event_type, outcome, effect, ci_low, ci_high, N
WITH params AS (
  SELECT 2009 AS season_start, 2024 AS season_end, 'REG' AS season_type
),
pbp AS (
  SELECT * FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
),
plays AS (
  SELECT p.* FROM pbp p, params par
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
with_gaps AS (
  SELECT
    season, week, season_type, game_id, posteam AS team,
    game_date,
    order_sequence,
    game_seconds_remaining,
    LAG(game_seconds_remaining) OVER (PARTITION BY season, week, game_id, posteam ORDER BY order_sequence) - game_seconds_remaining AS delta_sec,
    is_pass, is_rush, epa, xpass, no_huddle, shotgun
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
    season,
    week,
    season_type,
    game_id,
    team,
    -- Team-game metrics
    AVG(epa) AS epa_all,
    AVG(CASE WHEN is_pass=1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush=1 THEN epa END) AS epa_rush,
    AVG(no_huddle) AS no_huddle_rate,
    AVG(shotgun) AS shotgun_rate,
    AVG(xpass) FILTER (WHERE is_pass=1 OR is_rush=1) AS xpass_neutral,
    AVG(CASE WHEN is_pass=1 THEN 1 ELSE 0 END) FILTER (WHERE is_pass=1 OR is_rush=1) AS pass_rate_neutral,
    SUM(COALESCE(gap_sec,0))/NULLIF(COUNT(gap_sec),0) AS sec_per_play_neutral
  FROM gaps
  GROUP BY season, week, season_type, game_id, team
),
team_game_with_proe AS (
  SELECT *, (pass_rate_neutral - xpass_neutral) AS proe FROM team_game
),
game_meta AS (
  SELECT season, week, game_id,
         CAST(game_date AS DATE) AS game_date,
         home_team, away_team,
         UPPER(COALESCE(stadium, game_stadium, '')) AS stadium_name,
         MAX(CASE WHEN qtr>4 THEN 1 ELSE 0 END) AS had_ot
  FROM plays
  GROUP BY season, week, game_id, game_date, home_team, away_team, stadium_name
),
team_schedule AS (
  SELECT season, week, game_id, home_team AS team, game_date, stadium_name, had_ot FROM game_meta
  UNION ALL
  SELECT season, week, game_id, away_team AS team, game_date, stadium_name, had_ot FROM game_meta
),
rest_calc AS (
  SELECT
    ts.*,
    DATE_DIFF('day', LAG(ts.game_date) OVER (PARTITION BY ts.season, ts.team ORDER BY ts.game_date), ts.game_date) AS rest_days,
    LAG(ts.had_ot) OVER (PARTITION BY ts.season, ts.team ORDER BY ts.game_date) AS prev_had_ot
  FROM team_schedule ts
),
event_flags AS (
  SELECT
    rc.season,
    rc.week,
    rc.team,
    rc.game_id,
    CASE WHEN rest_days BETWEEN 13 AND 16 THEN 1 ELSE 0 END AS post_bye,
    CASE WHEN rest_days BETWEEN 3 AND 4 THEN 1 ELSE 0 END AS short_week,
    CASE WHEN rest_days BETWEEN 9 AND 11 THEN 1 ELSE 0 END AS mini_bye,
    COALESCE(prev_had_ot, 0) AS prev_ot,
    CASE WHEN stadium_name ILIKE '%WEMBLEY%' OR stadium_name ILIKE '%TOTTENHAM%'
           OR stadium_name ILIKE '%TWICKENHAM%' OR stadium_name ILIKE '%AZTECA%'
           OR stadium_name ILIKE '%MUNICH%' OR stadium_name ILIKE '%ALLIANZ%'
           OR stadium_name ILIKE '%FRANKFURT%' OR stadium_name ILIKE '%DEUTSCHE BANK%'
         THEN 1 ELSE 0 END AS international
  FROM rest_calc rc
),
events AS (
  SELECT season, week, team, game_id, 'post_bye' AS event_type FROM event_flags WHERE post_bye=1
  UNION ALL SELECT season, week, team, game_id, 'short_week' FROM event_flags WHERE short_week=1
  UNION ALL SELECT season, week, team, game_id, 'mini_bye' FROM event_flags WHERE mini_bye=1
  UNION ALL SELECT season, week, team, game_id, 'prev_ot' FROM event_flags WHERE prev_ot=1
  UNION ALL SELECT season, week, team, game_id, 'international' FROM event_flags WHERE international=1
),
metrics AS (
  SELECT season, week, team, game_id, season_type,
         epa_all, epa_pass, epa_rush, proe, sec_per_play_neutral
  FROM team_game_with_proe
  WHERE season_type='REG'
),
baseline AS (
  -- Team-season baseline excluding the event game itself
  SELECT m.season, m.team,
         AVG(epa_all) AS base_epa_all,
         AVG(epa_pass) AS base_epa_pass,
         AVG(epa_rush) AS base_epa_rush,
         AVG(proe) AS base_proe,
         AVG(sec_per_play_neutral) AS base_sec_per_play_neutral
  FROM metrics m
  GROUP BY m.season, m.team
),
joined AS (
  SELECT e.event_type, m.season, m.week, m.team,
         m.epa_all, m.epa_pass, m.epa_rush, m.proe, m.sec_per_play_neutral,
         b.base_epa_all, b.base_epa_pass, b.base_epa_rush, b.base_proe, b.base_sec_per_play_neutral
  FROM events e
  JOIN metrics m USING (season, week, team, game_id)
  JOIN baseline b USING (season, team)
),
deltas AS (
  SELECT event_type,
         (epa_all - base_epa_all) AS d_epa_all,
         (epa_pass - base_epa_pass) AS d_epa_pass,
         (epa_rush - base_epa_rush) AS d_epa_rush,
         (proe - base_proe) AS d_proe,
         (sec_per_play_neutral - base_sec_per_play_neutral) AS d_sec_per_play_neutral
  FROM joined
),
agg AS (
  SELECT event_type, 'epa_all' AS outcome,
         AVG(d_epa_all) AS effect,
         AVG(d_epa_all) - 1.96*stddev_samp(d_epa_all)/NULLIF(sqrt(COUNT(*)),0) AS ci_low,
         AVG(d_epa_all) + 1.96*stddev_samp(d_epa_all)/NULLIF(sqrt(COUNT(*)),0) AS ci_high,
         COUNT(*) AS N
  FROM deltas GROUP BY event_type
  UNION ALL
  SELECT event_type, 'epa_pass',
         AVG(d_epa_pass),
         AVG(d_epa_pass) - 1.96*stddev_samp(d_epa_pass)/NULLIF(sqrt(COUNT(*)),0),
         AVG(d_epa_pass) + 1.96*stddev_samp(d_epa_pass)/NULLIF(sqrt(COUNT(*)),0),
         COUNT(*) FROM deltas GROUP BY event_type
  UNION ALL
  SELECT event_type, 'epa_rush',
         AVG(d_epa_rush),
         AVG(d_epa_rush) - 1.96*stddev_samp(d_epa_rush)/NULLIF(sqrt(COUNT(*)),0),
         AVG(d_epa_rush) + 1.96*stddev_samp(d_epa_rush)/NULLIF(sqrt(COUNT(*)),0),
         COUNT(*) FROM deltas GROUP BY event_type
  UNION ALL
  SELECT event_type, 'proe',
         AVG(d_proe),
         AVG(d_proe) - 1.96*stddev_samp(d_proe)/NULLIF(sqrt(COUNT(*)),0),
         AVG(d_proe) + 1.96*stddev_samp(d_proe)/NULLIF(sqrt(COUNT(*)),0),
         COUNT(*) FROM deltas GROUP BY event_type
  UNION ALL
  SELECT event_type, 'sec_per_play_neutral',
         AVG(d_sec_per_play_neutral),
         AVG(d_sec_per_play_neutral) - 1.96*stddev_samp(d_sec_per_play_neutral)/NULLIF(sqrt(COUNT(*)),0),
         AVG(d_sec_per_play_neutral) + 1.96*stddev_samp(d_sec_per_play_neutral)/NULLIF(sqrt(COUNT(*)),0),
         COUNT(*) FROM deltas GROUP BY event_type
)
SELECT * FROM agg ORDER BY event_type, outcome;


