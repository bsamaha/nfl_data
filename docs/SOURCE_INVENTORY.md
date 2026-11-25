# nflverse source inventory

Checked on 21 Nov 2026 (week 11 of the 2026 season) against the public `nflverse/nflverse-data` releases: what each source provides, at what grain, with which key, and where it breaks. Counts and dates below are as of that day.

## How this was checked

The file listing comes from the GitHub releases API. Schemas come from the Parquet footers of each release file, read remotely. Keys were tested for duplicates and nulls in every season read, and each `game_id` was matched against the schedule; the exceptions are noted in the table and in each section. Stat definitions were not reconciled against play-by-play, and update timing was observed on one day only. The scripts that ran these checks are no longer in the tree, so the numbers below cannot be regenerated from it.

## What matters most

1. **The player-game and team-game tables already exist upstream.** `stats_player_week` and `stats_team_week` cover 1999–2026 with one unchanged schema and a key that includes `game_id` and is unique once a few null-key rows are set aside. The core layer should start from them, not re-derive box scores from play-by-play.
2. **`game_id` is the safe join for games. Team codes are not.** Three different team-code conventions are in use (see [Team codes](#team-codes)).
3. **`gsis_id` is the player key.** Snap counts and PFR advanced stats carry only a PFR ID and need `players.pfr_id` to join.
4. **Two sources changed layout in 2025.** Depth charts became timestamped snapshots with no season or week. Injuries lost `date_modified`.
5. **Participation is the only source that cannot be used in-season.** It has no 2026 file. FTN charting is published in-season and covers part of the same ground.
6. **The upstream data is small.** All nflverse-data Parquet files total about 0.76 GB, and play-by-play is 528 MB of that.

## Datasets at a glance

| Dataset | File pattern | Seasons | Rows per season | Key |
|---|---|---|---|---|
| Schedules | `schedules/games.parquet` | 1999–2026 | 7,548 total | `game_id` |
| Play-by-play | `pbp/play_by_play_{Y}.parquet` | 1999–2026 | 45k–50k | `game_id, play_id` |
| Player stats, weekly | `stats_player/stats_player_week_{Y}.parquet` | 1999–2026 | 17k–19k | `player_id, game_id` |
| Team stats, weekly | `stats_team/stats_team_week_{Y}.parquet` | 1999–2026 | 510–570 | `team, game_id` |
| Players | `players/players.parquet` | all | 24,871 total | `gsis_id` |
| Rosters, season | `rosters/roster_{Y}.parquet` | 1920–2026 (read from 1990) | 1.6k–2.2k through 2015, about 3,100 from 2016 | `season, team, gsis_id` (1–3 duplicate rows a season, 1998–2011) |
| Rosters, weekly | `weekly_rosters/roster_weekly_{Y}.parquet` | 2002–2026 | 31k–52k | `season, week, game_type, team, gsis_id` (unique from 2016 once rows with no `gsis_id` are set aside) |
| Snap counts | `snap_counts/snap_counts_{Y}.parquet` | 2013–2026 | 24k–27k | `game_id, pfr_player_id` |
| Injuries | `injuries/injuries_{Y}.parquet` | 2009–2026 | 4.5k–6.2k | `season, game_type, week, team, gsis_id` (2 repeated-key rows in 2024) |
| Depth charts, week-based | `depth_charts/depth_charts_{Y}.parquet` | 2001–2024 | 29k–39k | none that is unique |
| Depth charts, snapshot | same pattern | 2025–2026 | about 2.5k per snapshot | `dt, team, pos_id, pos_rank` |
| Next Gen Stats | `nextgen_stats/ngs_{passing,rushing,receiving}.parquet` | 2016–2026 | 6k, 6k, 15k total | `season, season_type, week, player_gsis_id` |
| Participation | `pbp_participation/pbp_participation_{Y}.parquet` | 2016–2025 | 45k–51k | `nflverse_game_id, play_id` |
| FTN charting | `ftn_charting/ftn_charting_{Y}.parquet` | 2022–2026 | 42k–48k | `nflverse_game_id, nflverse_play_id` |
| PFR advanced, weekly | `pfr_advstats/advstats_week_{pass,rush,rec,def}_{Y}.parquet` | 2018–2026 | 0.7k–8k | `game_id, pfr_player_id` |

All URLs have the form `https://github.com/nflverse/nflverse-data/releases/download/{release}/{file}`. Every file also carries its upstream build time in the Parquet metadata key `nflverse_timestamp`, which ingest records as `upstream_built_at` in `data/manifest/clean_state.json`.

## Core sources

### Schedules

- **Grain:** one row per game, played or scheduled. 46 columns.
- **Key:** `game_id`, unique with no nulls. Format `2015_01_CIN_OAK`.
- **Useful columns:** `gameday`, `gametime`, `game_type`, `week`, scores, `spread_line`, `total_line`, moneylines, `away_rest`, `home_rest`, `roof`, `surface`, `temp`, `wind`, starting QB IDs, coaches, `stadium_id`.
- **Betting lines:** `spread_line` and `total_line` are present for every game from 1999 to 2025. Both moneylines are null for every game from 1999 to 2005, for 135 games in 2006–2009 and for one game in 2017. For the season in progress, lines exist for played games and only the nearest scheduled ones (28 of 121 unplayed games on the check date).
- **Kickoff time:** `gameday` and `gametime` are strings in US Eastern time with no zone. `gametime` is null for all of 1999. Convert to UTC when building the game table.
- **Quirks:** `game_type` is `REG`, `WC`, `DIV`, `CON` or `SB`, where most other sources use `REG` and `POST`. `roof` is an empty string for 18 games. 2026 rows are regular-season only; playoff rows appear later.
- **Other IDs:** `old_game_id`, `pfr` and `espn` are always filled. `gsis` is 99% filled, `pff` 62%, `ftn` 26%.

### Play-by-play

- **Grain:** one row per play. 372 columns, identical in every season from 1999 to 2026.
- **Key:** `game_id, play_id`, unique in all 28 seasons. `play_id` is stored as a double but always holds whole numbers.
- **Links:** every `game_id` is in the schedule. Three old games (one in 1999, two in 2000) are in the schedule but have no plays.
- **Quirks:** `posteam` is null on about 2,500 rows a season (timeouts, period ends). Team columns use current codes in every season. `time_of_day` is a string, null on 3–4% of rows in the three seasons whose values were read (2015, 2019, 2025).
- **Not in this file:** `offense_personnel` and `defense_personnel`. They live in the participation files.

### Player stats, weekly

- **Grain:** one row per player per game in which the player recorded a stat, all positions including defense and kickers. 150 columns, identical in every season.
- **Not in this file:** a player who played without recording a stat. From 2013 to 2026, 93,923 snap-count rows of players in `players` have no row here; since 2016 that is 8,066 of the 68,063 QB, RB, WR and TE player-games with an offensive snap (25% for TE, 10% for WR). A stat line with nothing in a category holds 0 in its counts and shares and null in `passing_epa`, `passing_cpoe`, `pacr`, `rushing_epa`, `receiving_epa`, `racr`, `fg_long`, `fg_pct`, `pat_pct` and `pt_long`.
- **Not a number:** from 1999 to 2008, `target_share`, `air_yards_share` and `wopr` hold NaN where the share has nothing to divide by (102,141, 76,174 and 126,778 rows), and infinity in 3 rows each of the last two. No later season has either. `player_game` holds null in their place.
- **Key:** `player_id, game_id`, unique in all 28 seasons. `player_id` is the gsis ID.
- **Links:** every `game_id` is in the schedule.
- **Rows to drop:** from 2001 on, one row per week has a null `player_id`, null name and null position (21 or 22 a season).
- **Team:** `team` is the team for that game, so traded players are handled. 89 players had more than one team in 2025.
- **Column names differ from the frozen legacy files:** `team` (was `recent_team`), `passing_interceptions` (was `interceptions`), `sacks_suffered` (was `sacks`), `sack_yards_lost` (was `sack_yards`). `dakota` is gone.
- **Related files:** `stats_player_reg_{Y}` and `stats_player_post_{Y}` hold season totals. They use `recent_team` and add `games`. `stats_player_regpost_{Y}` also exists; its schema was not read.

### Team stats, weekly

- **Grain:** one row per team per game. 138 columns, identical in every season.
- **Key:** `team, game_id`, unique in all 28 seasons. One row in 1999 has a null `team`.
- **Use:** the denominators for shares (team attempts, carries, targets) and the base of the team-game table.

### Players

- **Grain:** one row per player. 24,871 rows, 39 columns.
- **Key:** `gsis_id`, unique with no nulls. Only 76% follow the modern `00-00xxxxx` pattern; older players use other formats.
- **Other IDs:** `pfr_id` (22,706 filled, unique), `espn_id` (16,603), `pff_id` (11,651), `nfl_id` (11,941), `otc_id` (9,833). `esb_id` and `smart_id` each have one duplicate value.
- **Missing:** no Sleeper or Yahoo IDs; they are in the roster files and in `ff_playerids`.

### Rosters

- **Season file key:** `season, team, gsis_id`. `season, gsis_id` is not unique because a traded player has one row per team (125 such rows in 2015, 1 in 2019, none in 2025).
- **Weekly file key:** `season, week, game_type, team, gsis_id`. It is unique in every season from 2016 once rows with a null or empty `gsis_id` are set aside; ingest set aside 2 to 38 such rows a season from 2018 on. Every season from 2002 to 2015 has 585 to 1,896 duplicate rows.
- **Schema breaks:** `jersey_number` and `draft_number` are strings from 2002 to 2015; season files before 2002 have an integer `jersey_number` and no `draft_number`. `height` is a double before 2025.
- **Quirks:** `gsis_id` is null on some rows in most seasons (none in 2000–2009, 2016 or 2017), and 2025 also has empty-string IDs. The season file has one to three duplicate rows a season from 1998 to 2011. The 2015 files use NFL internal team codes (see [Team codes](#team-codes)). Weekly `status` uses `ACT`, `RES`, `DEV`, `INA`, `CUT` and others; the detailed code is in `status_description_abbr`.
- **Coverage gap:** 154 of 3,133 weekly-roster IDs in 2025 (5%) are not in `players`.

### Snap counts

- **Grain:** one row per player per game, from Pro Football Reference.
- **Key:** `game_id, pfr_player_id`, unique in every season. The 2012 file is empty, so coverage starts in 2013.
- **Links:** every `game_id` is in the schedule and no played game is missing from 2013 to 2025.
- **Player join:** there is no gsis ID. Join through `players.pfr_id`. In 2025 that maps 628 of 630 QB, RB, WR and TE players; 6 of 2,189 IDs overall have no match.
- **Team:** three Super Bowls (`2014_21_NE_SEA`, `2015_21_CAR_DEN`, `2018_21_NE_LA`) list every player under the other team, with `team` and `opponent` exchanged. No other game from 2013 to 2026 disagrees with the player stats about a team. `player_game` turns those games back.

### Injuries

- **Grain:** one row per player per team per week, holding the game status and the practice status.
- **Key:** `season, game_type, week, team, gsis_id`, unique apart from 2 rows in 2024.
- **In-season:** yes. The 2026 file had rows for weeks 1–11.
- **Schema breaks:** `season` and `week` are doubles before 2021. From 2025 the file gains `season_type` and loses `date_modified`.
- **Point in time:** through 2024, `date_modified` shows when the row last changed, and 78% of 2024 rows were last changed on a Friday. So the file is the final weekly report, not a daily history. The value is marked as UTC in every season, but through the 2020 season it holds US Pacific clock time, seven or eight hours behind; `player_week_status` converts those seasons. From 2025 there is no timestamp at all, so the only way to know what a report said on a given day is to snapshot it yourself.
- **Values:** `report_status` is null, `Questionable`, `Out` or `Doubtful`, plus 6 rows with `Note` in 2024. Practice status is spelled out in full.

### Depth charts

- **Week-based layout, 2001–2024:** one row per player per position per week, with `season`, `club_code`, `week`, `game_type`, `formation`, `depth_team`. No combination of columns tested is unique: on the fullest key tried, every season has between 6 and 201 duplicate rows, and 2001 has 1,627. `week` is null for the `SBBYE` game type, and those rows are not in the counts.
- **Snapshot layout, 2025 on:** timestamped snapshots with `dt`, `team`, position columns, `pos_slot`, `pos_rank`, `gsis_id` and `espn_id`. There is no `season` or `week`.
- **Snapshot key:** `dt, team, pos_id, pos_rank`, unique in both files. Every snapshot covers all 32 teams.
- **History is kept upstream:** the 2025 file holds 221 snapshots from 3 Aug 2025 to 14 Mar 2026, and the 2026 file held 310 from 22 Mar 2026 to 21 Nov 2026. Point-in-time depth charts therefore exist from August 2025 without self-snapshotting.
- **Quirks:** `gsis_id` is null on 1% of 2025 rows and 3% of 2026 rows, while `espn_id` is always filled. Map those rows through `players.espn_id`. To attach a snapshot to a game, take the latest `dt` before kickoff.

### Next Gen Stats

- **Grain:** one row per player per week, in three files for passing, rushing and receiving. All seasons are in each file.
- **Key:** `season, season_type, week, player_gsis_id`, unique.
- **Rows to drop:** `week = 0` rows are season totals (444, 558 and 1,369 rows).
- **Quirks:** team column is `team_abbr` and uses `LAR` instead of `LA`. Because Next Gen Stats counts the Pro Bowl bye week, Super Bowl rows are numbered one week higher than `schedules` and `stats_player_week` (`week = 22` in 2016–2020 and `week = 23` in 2021+); subtract 1 for Super Bowl rows before joining. The files hold far fewer players than player stats do (704 distinct receivers across all seasons), so treat a missing row as "not published", not as zero.

### Participation and FTN charting

- **Participation:** one row per play with formation, personnel, players on the field, coverage and pressure. Key `nflverse_game_id, play_id`, unique, and every game from 2016 to 2025 is covered. There is no 2026 file; the last update was February 2026, after the postseason.
- **Personnel format changed in 2023.** Through 2022 the common value is `1 RB, 1 TE, 3 WR` and 24% of plays are null. From 2023 it is `1 C, 2 G, 1 QB, 1 RB, 2 T, 1 TE, 3 WR` and no plays are null. `play_id` also changed from integer to double in 2023.
- **FTN charting:** one row per play with play action, motion, screen, RPO, blitzers and similar flags. Key `nflverse_game_id, nflverse_play_id`, unique. Covers 2022–2026 and is published in-season; the 2026 file already had week 11.

### PFR advanced stats

- **Weekly files:** one row per player per game for pass, rush, rec and def. Key `game_id, pfr_player_id`. The key test read only the rec files and found the key unique in every season from 2018; ingest enforced the same key on all four file types for 2018–2026.
- **Lag:** one played 2026 game was missing.
- **Season files:** four files with all seasons. All use `pfr_id` where the weekly files use `pfr_player_id`; the rush, rec and def files also use `tm` and `pos`, and the pass file uses `team` and has no position column.

## Other files, schema checked only

| Dataset | File | Rows | Notes |
|---|---|---|---|
| Draft picks | `draft_picks/draft_picks.parquet` | 12,927 | Has `season`, `round`, `pick`, `gsis_id` and `pfr_player_id` |
| Combine | `combine/combine.parquet` | 8,968 | PFR ID only; `ht` is a string |
| Contracts | `contracts/historical_contracts.parquet` | 53,890 | Nested history columns; `otc_id` and `gsis_id` |
| Officials | `officials/officials.parquet` | 22,012 | Has `game_id` and `official_id` |
| Trades | `trades/trades.parquet` | 4,975 | PFR IDs |
| Teams | `teams/teams_colors_logos.parquet` | 36 | Team reference table |
| ESPN QBR | `espn_data/qbr_{week,season}_level.parquet` | 11,012 and 1,583 | ESPN player ID |
| Legacy player stats | `player_stats/…` | 5,597 in 2024 | Seasons 1999–2024. Frozen since May 2025; do not use |

Two useful sources sit outside `nflverse-data`:

- **`ff_playerids`** (`dynastyprocess/data`, `files/db_playerids.csv`): maps gsis to Sleeper, Yahoo, ESPN, FantasyPros, MFL and others. It has no DraftKings ID. Only the header was checked.
- **`ffopportunity`** (`ffverse/ffopportunity`, release `latest-data`): expected fantasy points per player-week in `ep_weekly_{Y}.parquet`, through 2026, updated 20 Nov 2026. Only the file listing was checked.

## Rules that apply across sources

### Team codes

| Convention | Used by | 2015 Oakland, San Diego, St. Louis |
|---|---|---|
| Current code in every season | play-by-play, player stats (team stats not compared) | `LV`, `LAC`, `LA` |
| Code of the era | schedules, injuries, snap counts, week-based depth charts, and the text of `game_id` | `OAK`, `SD`, `STL` |
| NFL internal codes | rosters and weekly rosters in 2015 | `SL` for St. Louis, plus `ARZ`, `BLT`, `CLV`, `HST` for Arizona, Baltimore, Cleveland, Houston |

The same game is `2015_01_CIN_OAK` in both the schedule and play-by-play, but play-by-play gives its home team as `LV`. So join games on `game_id`, never on team and week, and never read a team out of the `game_id` text. Normalize every team column to the current code when building the core tables.

The schedule's codes change with each move: `STL` becomes `LA` in 2016, `SD` becomes `LAC` in 2017, and `OAK` becomes `LV` in 2020.

Seasons compared across the other sources: 2015, 2019 and 2025. In 2019 the same split holds for Oakland (`OAK` in the era-code sources, `LV` in play-by-play and stats). In 2025 all sources agree. The roster files use the NFL internal codes in 2015 and not in 2019; the year they switched was not pinned down.

### Player IDs

- `gsis_id` is the key. It is called `player_id` in player stats and `player_gsis_id` in Next Gen Stats.
- All IDs in 2025 player stats and injuries, in Next Gen Stats receiving (all seasons) and in the 2026 snapshot depth charts are found in `players`.
- Treat an empty string as null.
- PFR-sourced data needs `players.pfr_id`. ESPN-only depth-chart rows need `players.espn_id`.
- No source carries a DraftKings ID.

### Seasons, weeks and time

- Postseason weeks continue the count: 18–21 through 2020 and 19–22 from 2021. `season, week` therefore identifies a game week without a season-type column.
- Season type is `REG` or `POST` in most sources, and the five-value `game_type` in schedules, rosters, injuries and snap counts.
- Kickoff comes from the schedule in Eastern time. Convert it once and use it as the cutoff for every as-of join.

## Core tables

Built into `data/core/` by `python -m nfl_lake build-core`, gated by publish-time checks (`docs/DATA_VERIFICATION.md`):

| Table | Key | Built from |
|---|---|---|
| `game.parquet` | `game_id` | Schedules: kickoff in UTC (`kickoff_utc`), teams in current 32 codes, lines, rest, roof, surface |
| `team_game.parquet` | `game_id, team` | Team stats joined to `game` for opponent, `is_home`, `points_scored`/`points_allowed`, `favored_by` (+ if team is favored), and `total_line` |
| `player.parquet` | `gsis_id` | Players with normalized `latest_team`/`draft_team`, enriched with latest `sleeper_id` and `yahoo_id` from `weekly_rosters` |
| `player_game.parquet` | `game_id, player_id` | Every player with a stat line or a snap in the game: player stats, plus snaps through `pfr_id`, plus Next Gen Stats (with Super Bowl week `22→21` / `23→22` alignment), plus PFR advanced stats where present. A player with snaps and no stat line (2013 on) has `has_stat_line` false, the values of an empty stat line, and the right team even in a game that `snap_counts` lists under the other team. A share upstream publishes as NaN or infinity is null |
| `play/play_<season>.parquet` | `game_id, play_id` | Play-by-play partitioned per season with normalized teams, plus participation (`2016+`), plus FTN charting (`2022+`) |
| `player_week_status.parquet` | `season, week, team, gsis_id` | `FULL OUTER JOIN` of `weekly_rosters` (`2016+`) and `injuries` (`2016+`) so injury-only rows are preserved. `injury_report_modified_utc` is the injuries `date_modified` in UTC (seasons through 2020 converted from Pacific clock time), which upstream publishes through 2024 only |
| `depth_chart_snapshot.parquet` | `dt, team, pos_id, pos_rank` | Snapshot depth charts (`2025+`), parsed `snapshot_utc`, and missing `gsis_id` values rescued via `players.espn_id` |

Week-based depth charts and weekly rosters before 2016 need a deduplication rule before they can be loaded. Decide whether they are worth it once the first features exist.

## Open questions

- Exact DraftKings rules for 2026: `docs/DATA_VERIFICATION.md` proved that `stats_player_week.fantasy_points_ppr` does **not** match DraftKings scoring (exact differences: missing `+3` yardage bonuses, `-2` vs `-1` on INTs and fumbles lost, return fumbles excluded from component fumbles, and offensive fumble-recovery TDs excluded). Before building projections, re-verify `catalog/draftkings.yml` against the 2026 DraftKings rules page — specifically whether PATs after opponent pick-sixes count against DST points allowed and whether return fumbles lost score `-1` for offensive players.
- What update schedule to rely on. On 21 Nov 2026 files were updated between 14:00 and 17:00 UTC and again after 22:00 UTC. The schedule nflverse states was not checked.
- How far back the training window should go. Five sources start between 2013 and 2022 (see the table), and `docs/DATA_VERIFICATION.md` shows `2016–2026` is 100% clean across every cross-source total while `1999–2015` has upstream gaps (missing `1999` gametimes, three `1999–2000` play-less games, missing `2001–2002` JAX team rows, and `2002–2008` missing targets).
