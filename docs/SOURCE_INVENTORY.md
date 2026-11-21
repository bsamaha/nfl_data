# nflverse source inventory

Checked on 21 Nov 2026 (week 11 of the 2026 season) against the public `nflverse/nflverse-data` releases. This is step 1 of the rebuild plan: what each source provides, at what grain, with which key, and where it breaks. It replaces the availability notes in `NFLVERSE_DATA_SCHEDULE.md`, which are out of date.

## How this was checked

- **File listing.** Every release and file, with size and last-update time, from the GitHub releases API.
- **Schemas.** The Parquet footer of 285 season files, read remotely. That gives column names, types and row counts for every season without downloading the data.
- **Keys and values.** Key columns were read for every season of the main datasets. A full run of the scripts transfers about 205 MB and stores only a file listing and a schema summary. Each key below was tested for duplicates and nulls, and each `game_id` was matched against the schedule.
- **Not checked.** Stat definitions were not reconciled against play-by-play. Update timing was observed on one day only.
- **Re-running.** The scripts are in `scripts/source_inventory/`; its README gives the run order.

## What matters most

1. **The player-game and team-game tables already exist upstream.** `stats_player_week` and `stats_team_week` cover 1999–2026 with one unchanged schema and a clean key on `game_id`. The core layer should start from them, not re-derive box scores from play-by-play.
2. **`game_id` is the safe join for games. Team codes are not.** Three different team-code conventions are in use (see [Team codes](#team-codes)).
3. **`gsis_id` is the player key.** Snap counts and PFR advanced stats carry only a PFR ID and need `players.pfr_id` to join.
4. **Two sources changed layout in 2025.** Depth charts became timestamped snapshots with no season or week. Injuries lost `date_modified`.
5. **Participation is the only source that cannot be used in-season.** It has no 2026 file. FTN charting is published in-season and covers part of the same ground.
6. **The whole lake is small.** All Parquet files total about 0.76 GB, and play-by-play is 528 MB of that.

## Datasets at a glance

| Dataset | File pattern | Seasons | Rows per season | Verified key | In-season in 2026 |
|---|---|---|---|---|---|
| Schedules | `schedules/games.parquet` | 1999–2026 | 7,548 total | `game_id` | Yes |
| Play-by-play | `pbp/play_by_play_{Y}.parquet` | 1999–2026 | 45k–50k | `game_id, play_id` | Yes |
| Player stats, weekly | `stats_player/stats_player_week_{Y}.parquet` | 1999–2026 | 17k–19k | `player_id, game_id` | Yes |
| Team stats, weekly | `stats_team/stats_team_week_{Y}.parquet` | 1999–2026 | about 570 | `team, game_id` | Yes |
| Players | `players/players.parquet` | all | 24,871 total | `gsis_id` | Yes |
| Rosters, season | `rosters/roster_{Y}.parquet` | 1920–2026 | about 3,100 | `season, team, gsis_id` | Yes |
| Rosters, weekly | `weekly_rosters/roster_weekly_{Y}.parquet` | 2002–2026 | 31k–52k | `season, week, game_type, team, gsis_id` (clean from 2016) | Yes |
| Snap counts | `snap_counts/snap_counts_{Y}.parquet` | 2013–2026 | about 26k | `game_id, pfr_player_id` | Yes |
| Injuries | `injuries/injuries_{Y}.parquet` | 2009–2026 | 4.5k–6.2k | `season, game_type, week, team, gsis_id` | Yes |
| Depth charts, old | `depth_charts/depth_charts_{Y}.parquet` | 2001–2024 | about 37k | none that is unique | n/a |
| Depth charts, new | same pattern | 2025–2026 | 554k, 822k so far | `dt, team, pos_id, pos_rank` | Yes |
| Next Gen Stats | `nextgen_stats/ngs_{passing,rushing,receiving}.parquet` | 2016–2026 | 6k, 6k, 15k total | `season, season_type, week, player_gsis_id` | Yes |
| Participation | `pbp_participation/pbp_participation_{Y}.parquet` | 2016–2025 | 45k–51k | `nflverse_game_id, play_id` | **No** |
| FTN charting | `ftn_charting/ftn_charting_{Y}.parquet` | 2022–2026 | about 48k | `nflverse_game_id, nflverse_play_id` | Yes |
| PFR advanced, weekly | `pfr_advstats/advstats_week_{pass,rush,rec,def}_{Y}.parquet` | 2018–2026 | 0.7k–8k | `game_id, pfr_player_id` | Yes |

All URLs have the form `https://github.com/nflverse/nflverse-data/releases/download/{release}/{file}`. Every file also carries its upstream build time in the Parquet metadata key `nflverse_timestamp`, which the ingest manifest should record.

## Contracts for the core sources

### Schedules

- **Grain:** one row per game, played or scheduled. 46 columns.
- **Key:** `game_id`, unique with no nulls. Format `2015_01_CIN_OAK`.
- **Useful columns:** `gameday`, `gametime`, `game_type`, `week`, scores, `spread_line`, `total_line`, moneylines, `away_rest`, `home_rest`, `roof`, `surface`, `temp`, `wind`, starting QB IDs, coaches, `stadium_id`.
- **Betting lines:** present for every game from 1999 to 2025. In 2026 they exist for the 151 played games and only the next 28 scheduled ones. The other 93 scheduled games have no line yet.
- **Kickoff time:** `gameday` and `gametime` are strings in US Eastern time with no zone. `gametime` is null for all of 1999. Convert to UTC when building the game table.
- **Quirks:** `game_type` is `REG`, `WC`, `DIV`, `CON` or `SB`, where most other sources use `REG` and `POST`. `roof` is an empty string for 18 games. The 2026 file holds regular-season games only; playoff rows appear later.
- **Other IDs:** `old_game_id`, `pfr` and `espn` are always filled. `gsis` is 99% filled, `pff` 62%, `ftn` 26%.

### Play-by-play

- **Grain:** one row per play. 372 columns, identical in every season from 1999 to 2026.
- **Key:** `game_id, play_id`, unique in all 28 seasons. `play_id` is stored as a double but always holds whole numbers.
- **Links:** every `game_id` is in the schedule. Three old games (one in 1999, two in 2000) are in the schedule but have no plays.
- **Quirks:** `posteam` is null on about 2,500 rows a season (timeouts, period ends). There is a `season` column and no `year` column. Team columns use current codes in every season. `time_of_day` is a UTC timestamp in recent seasons and null in older ones.
- **Not in this file:** `offense_personnel` and `defense_personnel`. They live in the participation files.
- **Size:** about 19 MB per season.

### Player stats, weekly

- **Grain:** one row per player per game, all positions including defense and kickers. 150 columns, identical in every season.
- **Key:** `player_id, game_id`, unique in all 28 seasons. `player_id` is the gsis ID.
- **Links:** every `game_id` is in the schedule, and every 2025 `player_id` is in `players`.
- **Rows to drop:** from 2001 on, one row per week has a null `player_id`, null name and null position (21 or 22 a season).
- **Team:** `team` is the team for that game, so traded players are handled. 89 players had more than one team in 2025.
- **Postseason weeks:** 18–21 through 2020, and 19–22 from 2021.
- **Column names differ from the frozen legacy files:** `team` (was `recent_team`), `passing_interceptions` (was `interceptions`), `sacks_suffered` (was `sacks`), `sack_yards_lost` (was `sack_yards`). `dakota` is gone. The old report SQL uses the legacy names.
- **Related files:** `stats_player_reg_{Y}`, `stats_player_post_{Y}` and `stats_player_regpost_{Y}` hold season totals. They use `recent_team` and add `games`.

### Team stats, weekly

- **Grain:** one row per team per game. 138 columns, identical in every season.
- **Key:** `team, game_id`, unique in all 28 seasons. One row in 1999 has a null `team`.
- **Use:** the denominators for shares (team attempts, carries, targets) and the base of the team-game table.

### Players

- **Grain:** one row per player. 24,871 rows, 39 columns.
- **Key:** `gsis_id`, unique with no nulls. Only 76% follow the modern `00-00xxxxx` pattern; older players use other formats.
- **Other IDs:** `pfr_id` (22,706 filled, unique), `espn_id` (16,603), `pff_id` (11,651), `nfl_id` (11,941), `otc_id` (9,833). `esb_id` and `smart_id` each have one duplicate value.
- **Missing:** no Sleeper, Yahoo or DraftKings IDs. Sleeper and Yahoo are in the roster files and in `ff_playerids`.

### Rosters

- **Season file key:** `season, team, gsis_id`. `season, gsis_id` is not unique because a traded player has one row per team (2 such rows in 2025, 125 in 2015).
- **Weekly file key:** `season, week, game_type, team, gsis_id`. It is unique from 2016 on, apart from 1 row in 2021 and 6 in 2022. Every season from 2002 to 2015 has 585 to 1,896 duplicate rows.
- **Schema breaks:** `jersey_number` and `draft_number` are strings before 2016. `height` is a double before 2025.
- **Quirks:** a few rows each season have a null `gsis_id`, and 2025 has empty-string IDs. The season file has one to three duplicate rows a season from 1999 to 2011. The 2015 files use the team codes `ARZ`, `BLT`, `CLV`, `HST` and `SL`; the 2019 files do not. Weekly `status` uses `ACT`, `RES`, `DEV`, `INA`, `CUT` and others; the detailed code is in `status_description_abbr`.
- **Coverage gap:** 155 of 3,134 weekly-roster IDs in 2025 (5%) are not in `players`.

### Snap counts

- **Grain:** one row per player per game, from Pro Football Reference.
- **Key:** `game_id, pfr_player_id`, unique in every season. The 2012 file is empty, so coverage starts in 2013.
- **Links:** every `game_id` is in the schedule and no played game is missing from 2013 to 2025.
- **Player join:** there is no gsis ID. Join through `players.pfr_id`. In 2025 that maps 628 of 630 QB, RB, WR and TE players; 6 of 2,189 IDs overall have no match.

### Injuries

- **Grain:** one row per player per team per week, holding the game status and the practice status.
- **Key:** `season, game_type, week, team, gsis_id`, unique apart from 2 rows in 2024.
- **In-season:** yes. The 2026 file has rows for weeks 1–11 and was updated on the day of this check.
- **Schema breaks:** `season` and `week` are doubles before 2021. From 2025 the file gains `season_type` and loses `date_modified`.
- **Point in time:** through 2024, `date_modified` shows when the row last changed, and 78% of 2024 rows were last changed on a Friday. So the file is the final weekly report, not a daily history. From 2025 there is no timestamp at all, so the only way to know what a report said on a given day is to snapshot it yourself.
- **Values:** `report_status` is null, `Questionable`, `Out` or `Doubtful`. Practice status is spelled out in full.

### Depth charts

- **Old layout, 2001–2024:** one row per player per position per week, with `season`, `club_code`, `week`, `game_type`, `formation`, `depth_team`. No combination of columns tested is unique: every season has between 6 and 207 duplicate rows, and 2001 has 1,628. `week` is null for the `SBBYE` game type.
- **New layout, 2025 on:** timestamped snapshots with `dt`, `team`, position columns, `pos_slot`, `pos_rank`, `gsis_id` and `espn_id`. There is no `season` or `week`.
- **New key:** `dt, team, pos_id, pos_rank`, unique in both files. Every snapshot covers all 32 teams.
- **History is kept upstream:** the 2025 file holds 221 snapshots from 3 Aug 2025 to 14 Mar 2026. The 2026 file holds 310 snapshots from 22 Mar 2026 to the day of this check. Point-in-time depth charts therefore exist for 2025 on without self-snapshotting.
- **Quirks:** `gsis_id` is null on 1% of 2025 rows and 3% of 2026 rows, while `espn_id` is always filled. Map those rows through `players.espn_id`. To attach a snapshot to a game, take the latest `dt` before kickoff.

### Next Gen Stats

- **Grain:** one row per player per week, in three files for passing, rushing and receiving. All seasons are in each file.
- **Key:** `season, season_type, week, player_gsis_id`, unique.
- **Rows to drop:** `week = 0` rows are season totals (444, 554 and 1,362 rows).
- **Quirks:** team column is `team_abbr`. The files hold far fewer players than player stats do (704 distinct receivers across all seasons), so treat a missing row as "not published", not as zero.

### Participation and FTN charting

- **Participation:** one row per play with formation, personnel, players on the field, coverage and pressure. Key `nflverse_game_id, play_id`, unique, and every game from 2016 to 2025 is covered. There is no 2026 file; the last update was February 2026, after the postseason.
- **Personnel format changed in 2023.** Through 2022 the common value is `1 RB, 1 TE, 3 WR` and 24% of plays are null. From 2023 it is `1 C, 2 G, 1 QB, 1 RB, 2 T, 1 TE, 3 WR` and no plays are null. `play_id` also changed from integer to double in 2023.
- **FTN charting:** one row per play with play action, motion, screen, RPO, blitzers and similar flags. Key `nflverse_game_id, nflverse_play_id`, unique. Covers 2022–2026 and is published in-season; the 2026 file had week 11 on the day of this check.

### PFR advanced stats

- **Weekly files:** one row per player per game for pass, rush, rec and def. Key `game_id, pfr_player_id`, unique for the rec files in every season from 2018. Player join goes through `players.pfr_id`.
- **Lag:** one played 2026 game was missing on the day of this check.
- **Season files:** four files with all seasons, using different column names (`tm`, `pos`, `pfr_id`).

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
| Legacy player stats | `player_stats/…` | 1999–2024 | Frozen since May 2025; do not use |

Two useful sources sit outside `nflverse-data`:

- **`ff_playerids`** (`dynastyprocess/data`, `files/db_playerids.csv`): maps gsis to Sleeper, Yahoo, ESPN, FantasyPros, MFL and others. It has no DraftKings ID. Only the header was checked.
- **`ffopportunity`** (`ffverse/ffopportunity`, release `latest-data`): expected fantasy points per player-week in `ep_weekly_{Y}.parquet`, through 2026, updated the day before this check. Only the file listing was checked.

## Rules that apply across sources

### Team codes

| Convention | Used by | 2015 Oakland, San Diego, St. Louis |
|---|---|---|
| Current code in every season | play-by-play, player stats, team stats | `LV`, `LAC`, `LA` |
| Code of the era | schedules, injuries, snap counts, old depth charts, and the text of `game_id` | `OAK`, `SD`, `STL` |
| NFL internal codes | rosters and weekly rosters in 2015 | `SL` for St. Louis, plus `ARZ`, `BLT`, `CLV`, `HST` for Arizona, Baltimore, Cleveland, Houston |

The same game is `2015_01_CIN_OAK` in both the schedule and play-by-play, but play-by-play gives its home team as `LV`. So join games on `game_id`, never on team and week, and never read a team out of the `game_id` text. Normalize every team column to the current code in staging.

The schedule's codes change with each move: `STL` becomes `LA` in 2016, `SD` becomes `LAC` in 2017, and `OAK` becomes `LV` in 2020.

Seasons compared across the other sources: 2015, 2019 and 2025. In 2019 the same split holds for Oakland (`OAK` in the era-code sources, `LV` in play-by-play and stats). In 2025 all sources agree. The roster files use the NFL internal codes in 2015 and not in 2019; the year they switched was not pinned down.

### Player IDs

- `gsis_id` is the key. It is called `player_id` in player stats and `player_gsis_id` in Next Gen Stats.
- All 2025 IDs in player stats, injuries, Next Gen Stats and the new depth charts are found in `players`.
- Treat an empty string as null. Empty-string IDs occur in the 2025 roster files.
- PFR-sourced data needs `players.pfr_id`. ESPN-only depth-chart rows need `players.espn_id`.
- No source carries a DraftKings ID.

### Seasons, weeks and time

- Postseason weeks continue the count: 18–21 through 2020 and 19–22 from 2021. `season, week` therefore identifies a game week without a season-type column.
- Season type is `REG` or `POST` in most sources, and the five-value `game_type` in schedules, rosters, injuries and snap counts.
- Kickoff comes from the schedule in Eastern time. Convert it once and use it as the cutoff for every as-of join.

## Proposed core tables

| Table | Key | Built from |
|---|---|---|
| `game` | `game_id` | Schedules: kickoff in UTC, teams in current codes, lines, rest, roof, surface |
| `team_game` | `game_id, team` | Team stats joined to `game` for opponent, home or away, spread and total |
| `player_game` | `game_id, player_id` | Player stats, plus snaps through `pfr_id`, plus Next Gen Stats and PFR advanced stats where present |
| `play` | `game_id, play_id` | Play-by-play, plus FTN charting from 2022, plus participation for completed seasons |
| `player` | `gsis_id` | Players, plus `ff_playerids` for fantasy-site IDs |
| `player_week_status` | `season, week, team, gsis_id` | Weekly rosters from 2016 and injuries from 2009 |
| `depth_chart_snapshot` | `dt, team, pos_id, pos_rank` | New depth charts, 2025 on, attached to games by latest snapshot before kickoff |

Old-layout depth charts and weekly rosters before 2016 need a deduplication rule before they can be loaded. Decide whether they are worth it once the first features exist.

## Where the existing catalog disagrees

`catalog/datasets.yml` was written against the retired library. These keys need to change before it can drive ingest or tests.

| Catalog entry | Catalog key | Verified key |
|---|---|---|
| `weekly` | `season, week, player_id, team` | `player_id, game_id` |
| `rosters_seasonal` | `season, player_id` | `season, team, gsis_id` |
| `rosters` | `season, week, player_id, team` | adds `game_type`; unique only from 2016 |
| `snap_counts` | `season, week, team, player_id` | `game_id, pfr_player_id`; there is no `player_id` column |
| `depth_charts` | `season, week, team, position, player_id` | `dt, team, pos_id, pos_rank` from 2025; none before |
| `ngs_weekly` | `season, week, player_id, stat_type` | `season, season_type, week, player_gsis_id`, one file per stat type |
| `pbp` | partitioned on `year` | the column is `season` |

## Open questions

- Which three games from 1999 and 2000 have no play-by-play, and whether that matters for the training window.
- Whether `fantasy_points_ppr` in player stats matches the scoring in the DraftKings rule files. That belongs to the scoring step.
- What update schedule to rely on. Files were updated between 14:00 and 19:00 UTC on the day of this check, which is later than the 07:00 UTC the nflverse schedule page states for rosters, injuries and depth charts.
- How far back the training window should go. Snap counts start in 2013, Next Gen Stats and participation in 2016, PFR advanced stats in 2018 and FTN charting in 2022.
