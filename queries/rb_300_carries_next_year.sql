-- RBs with >=300 carries in a season and their next-season simple stats
-- Reads season-level aggregates derived from weekly data

WITH weekly_norm AS (
  SELECT
    season,
    player_id,
    player_name,
    player_display_name,
    position AS pos,
    COALESCE(carries, 0) AS rush_att_wk,
    COALESCE(rushing_yards, 0) AS rush_yds_wk,
    COALESCE(rushing_tds, 0) AS rush_td_wk,
    COALESCE(receptions, 0) AS rec_wk,
    COALESCE(receiving_yards, 0) AS rec_yds_wk,
    COALESCE(receiving_tds, 0) AS rec_td_wk
  FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
),
season_stats AS (
  SELECT
    season,
    player_id,
    COALESCE(MAX(player_name), MAX(player_display_name)) AS player_name,
    any_value(pos) AS pos,
    SUM(rush_att_wk) AS rush_att,
    SUM(rush_yds_wk) AS rush_yds,
    SUM(rush_td_wk) AS rush_td,
    SUM(rec_wk) AS receptions,
    SUM(rec_yds_wk) AS rec_yds,
    SUM(rec_td_wk) AS rec_td
  FROM weekly_norm
  WHERE pos = 'RB'
  GROUP BY season, player_id
),
workhorse AS (
  SELECT *
  FROM season_stats
  WHERE rush_att >= 300
)
SELECT
  w.season,
  w.player_id,
  w.player_name,
  w.rush_att,
  w.rush_yds,
  w.rush_td,
  w.receptions,
  w.rec_yds,
  w.rec_td,
  n.season AS next_season,
  n.rush_att AS rush_att_ny,
  n.rush_yds AS rush_yds_ny,
  n.rush_td AS rush_td_ny,
  n.receptions AS receptions_ny,
  n.rec_yds AS rec_yds_ny,
  n.rec_td AS rec_td_ny,
  (n.rush_att - w.rush_att) AS delta_rush_att,
  (n.rush_yds - w.rush_yds) AS delta_rush_yds,
  (n.rush_td - w.rush_td) AS delta_rush_td,
  (n.receptions - w.receptions) AS delta_receptions,
  (n.rec_yds - w.rec_yds) AS delta_rec_yds,
  (n.rec_td - w.rec_td) AS delta_rec_td
FROM workhorse w
LEFT JOIN season_stats n
  ON n.player_id = w.player_id
 AND n.season = w.season + 1
ORDER BY w.season, w.player_name;


