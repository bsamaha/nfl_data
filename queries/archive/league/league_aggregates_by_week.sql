-- Weekly league metrics across a season range
-- Params are discoverable by scripts/run_query.sh and can be overridden via flags
WITH params AS (
  SELECT
    2005 AS season_start,
    2024 AS season_end,
    'REG' AS season_type
),
pbp AS (
  SELECT *
  FROM read_parquet('data/silver/pbp/year=*/**/*.parquet')
),
-- Restrict to desired seasons and game types
plays AS (
  SELECT
    p.season,
    p.week,
    p.game_id,
    p.posteam,
    p.home_team,
    p.away_team,
    p.order_sequence,
    p.game_seconds_remaining,
    p.quarter_seconds_remaining,
    p.qtr,
    p.down,
    p.score_differential,
    CASE WHEN p.posteam = p.home_team THEN p.home_wp ELSE p.away_wp END AS posteam_wp,
    COALESCE(p.pass, 0)::INT AS is_pass,
    COALESCE(p.rush_attempt, 0)::INT AS is_rush,
    COALESCE(p.qb_spike, 0)::INT AS is_spike,
    COALESCE(p.qb_kneel, 0)::INT AS is_kneel,
    COALESCE(p.play_deleted, 0)::INT AS is_deleted,
    COALESCE(p.no_huddle, 0)::INT AS no_huddle,
    COALESCE(p.shotgun, 0)::INT AS shotgun,
    p.xpass,
    p.epa,
    p.yards_gained
  FROM pbp p, params par
  WHERE p.season BETWEEN par.season_start AND par.season_end
    AND p.season_type = par.season_type
),
qualifying AS (
  -- Qualifying plays: pass or rush; exclude spikes, kneels, and deleted
  SELECT *
  FROM plays
  WHERE (is_pass = 1 OR is_rush = 1)
    AND is_spike = 0 AND is_kneel = 0 AND is_deleted = 0
),
-- Compute per-offense, per-game play-to-play elapsed time to estimate pace
play_deltas AS (
  SELECT
    season,
    week,
    game_id,
    posteam,
    order_sequence,
    game_seconds_remaining,
    LAG(game_seconds_remaining) OVER (
      PARTITION BY season, week, game_id, posteam
      ORDER BY order_sequence
    ) - game_seconds_remaining AS delta_sec,
    -- keep flags/metrics
    is_pass, is_rush, no_huddle, shotgun, xpass, epa, yards_gained,
    down, score_differential, posteam_wp, qtr, quarter_seconds_remaining
  FROM qualifying
),
play_deltas_clean AS (
  -- Filter out negative or implausible gaps; cap to [0, 45] seconds
  SELECT
    *,
    CASE
      WHEN delta_sec IS NULL THEN NULL
      WHEN delta_sec < 0 THEN NULL
      WHEN delta_sec > 45 THEN 45
      ELSE delta_sec
    END AS gap_sec
  FROM play_deltas
),
neutral_flags AS (
  SELECT
    *,
    -- Neutral situation per spec: posteam WP 0.35–0.65, score diff -7..+7, downs 1-2, outside two-minute warning
    CASE
      WHEN posteam_wp BETWEEN 0.35 AND 0.65
       AND score_differential BETWEEN -7 AND 7
       AND down IN (1, 2)
       AND NOT (qtr IN (2, 4) AND quarter_seconds_remaining <= 120)
      THEN 1 ELSE 0 END AS is_neutral
  FROM play_deltas_clean
),
-- Team-game aggregates (neutral and all)
team_game AS (
  SELECT
    season,
    week,
    game_id,
    posteam,
    COUNT(*) AS plays_all,
    SUM(CASE WHEN is_pass = 1 THEN 1 ELSE 0 END) AS pass_plays_all,
    SUM(CASE WHEN is_rush = 1 THEN 1 ELSE 0 END) AS rush_plays_all,
    AVG(epa) AS epa_all,
    AVG(CASE WHEN is_pass = 1 THEN epa END) AS epa_pass,
    AVG(CASE WHEN is_rush = 1 THEN epa END) AS epa_rush,
    AVG(no_huddle) AS no_huddle_rate,
    AVG(shotgun) AS shotgun_rate,
    -- Pace: sum gap seconds over plays with a defined gap, divided by number of such plays
    SUM(COALESCE(gap_sec, 0)) / NULLIF(COUNT(gap_sec), 0) AS sec_per_play_all,
    -- Neutral-only counterparts
    COUNT(*) FILTER (WHERE is_neutral = 1) AS plays_neutral,
    SUM(CASE WHEN is_neutral = 1 AND is_pass = 1 THEN 1 ELSE 0 END) AS pass_plays_neutral,
    SUM(CASE WHEN is_neutral = 1 AND is_rush = 1 THEN 1 ELSE 0 END) AS rush_plays_neutral,
    SUM(CASE WHEN is_neutral = 1 THEN COALESCE(gap_sec, 0) END) / NULLIF(COUNT(gap_sec) FILTER (WHERE is_neutral = 1), 0) AS sec_per_play_neutral,
    AVG(CASE WHEN is_neutral = 1 THEN xpass END) AS xpass_neutral,
    AVG(CASE WHEN is_neutral = 1 THEN is_pass END) AS pass_rate_neutral
  FROM neutral_flags
  GROUP BY season, week, game_id, posteam
),
-- Team-game PROE (neutral)
team_game_proe AS (
  SELECT
    season,
    week,
    game_id,
    posteam,
    (pass_rate_neutral - xpass_neutral) AS proe
  FROM team_game
  WHERE plays_neutral > 0
),
-- League-week aggregates
plays_week AS (
  SELECT
    season,
    week,
    SUM(CASE WHEN is_pass = 1 THEN 1 ELSE 0 END) AS pass_plays,
    SUM(CASE WHEN is_rush = 1 THEN 1 ELSE 0 END) AS rush_plays,
    SUM(CASE WHEN is_pass = 1 AND yards_gained >= 20 THEN 1 ELSE 0 END) AS pass_exp_20,
    SUM(CASE WHEN is_rush = 1 AND yards_gained >= 10 THEN 1 ELSE 0 END) AS rush_exp_10,
    SUM(CASE WHEN is_rush = 1 AND yards_gained >= 15 THEN 1 ELSE 0 END) AS rush_exp_15,
    SUM(CASE WHEN is_pass = 1 AND epa > 0 THEN epa ELSE 0 END) AS pass_epa_pos,
    SUM(CASE WHEN is_pass = 1 AND yards_gained >= 20 AND epa > 0 THEN epa ELSE 0 END) AS pass_epa_pos_exp,
    SUM(CASE WHEN is_rush = 1 AND epa > 0 THEN epa ELSE 0 END) AS rush_epa_pos,
    SUM(CASE WHEN is_rush = 1 AND yards_gained >= 10 AND epa > 0 THEN epa ELSE 0 END) AS rush_epa_pos_exp
  FROM neutral_flags
  GROUP BY season, week
),
proe_week AS (
  SELECT season, week,
         AVG(proe) AS proe_mean,
         (quantile(proe, 0.75) - quantile(proe, 0.25)) AS proe_iqr
  FROM team_game_proe
  GROUP BY season, week
),
ppg_week AS (
  SELECT season, week, AVG(game_points) AS ppg
  FROM (
    SELECT season, week, game_id,
           MAX(total_home_score) + MAX(total_away_score) AS game_points
    FROM pbp p2, params par
    WHERE p2.season BETWEEN par.season_start AND par.season_end
      AND p2.season_type = par.season_type
    GROUP BY season, week, game_id
  )
  GROUP BY season, week
),
tds_week AS (
  SELECT season, week, AVG(tds_game) AS tds_pg
  FROM (
    SELECT season, week, game_id,
           SUM(CASE WHEN COALESCE(touchdown, 0) = 1 THEN 1 ELSE 0 END) AS tds_game
    FROM pbp p2, params par
    WHERE p2.season BETWEEN par.season_start AND par.season_end
      AND p2.season_type = par.season_type
    GROUP BY season, week, game_id
  )
  GROUP BY season, week
),
league_week AS (
  SELECT
    tg.season,
    tg.week,
    -- Plays per game: average team plays across team-games
    AVG(tg.plays_all) AS plays_pg,
    -- PPG from scoreboard tally
    pw.ppg,
    -- Touchdowns per game (excludes field goals and other scoring)
    AVG(tw.tds_pg) AS tds_pg,
    AVG(tg.epa_all) AS epa_all,
    AVG(tg.epa_pass) AS epa_pass,
    AVG(tg.epa_rush) AS epa_rush,
    AVG(tg.sec_per_play_neutral) AS sec_per_play_neutral,
    AVG(tg.sec_per_play_all) AS sec_per_play_all,
    AVG(tg.no_huddle_rate) AS no_huddle_rate,
    AVG(tg.shotgun_rate) AS shotgun_rate,
    -- Explosives
    (pl.pass_exp_20::DOUBLE / NULLIF(pl.pass_plays, 0)) AS explosive_pass_rate,
    (pl.rush_exp_10::DOUBLE / NULLIF(pl.rush_plays, 0)) AS explosive_rush10_rate,
    (pl.rush_exp_15::DOUBLE / NULLIF(pl.rush_plays, 0)) AS explosive_rush15_rate,
    (pl.pass_epa_pos_exp / NULLIF(pl.pass_epa_pos, 0)) AS explosive_epa_share_pass,
    (pl.rush_epa_pos_exp / NULLIF(pl.rush_epa_pos, 0)) AS explosive_epa_share_rush,
    -- PROE summary
    pr.proe_mean,
    pr.proe_iqr
  FROM team_game tg
  JOIN ppg_week pw USING (season, week)
  JOIN tds_week tw USING (season, week)
  JOIN plays_week pl USING (season, week)
  LEFT JOIN proe_week pr USING (season, week)
  GROUP BY tg.season, tg.week, pw.ppg, pl.pass_exp_20, pl.rush_exp_10, pl.rush_exp_15, pl.pass_epa_pos_exp, pl.pass_epa_pos, pl.rush_epa_pos_exp, pl.rush_epa_pos, pl.pass_plays, pl.rush_plays, pr.proe_mean, pr.proe_iqr
)
SELECT *
FROM league_week
ORDER BY season, week;


