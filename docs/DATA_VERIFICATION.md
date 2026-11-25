# Data Verification Specification (Strategy 1)

Measured on 24 Nov 2026 (through week 11 of the 2026 season) against the full local lake (`185` files, `426.4 MB`, `18` datasets in `data/clean/`, verified clean with `python -m nfl_lake verify --deep`).

This document holds the measured baseline numbers, team-code mappings, join quirks, checkbook rules, DraftKings scoring deltas, and pinned real NFL anchor games behind the publish-time checks. The rules for **Stage 3 (Ingest)** and **Stage 4 (Core tables)** are enforced by `nfl_lake/checks.py` on every `build-core` and `verify`. The rules for **Stage 5 (Features)** and **Stage 6 (Projections & DraftKings)** are measured here and no code runs them yet.

---

## 1. Lake Baseline & Cross-Year Schema Transitions

| Dataset | Files | Seasons | Rows | Set Aside | Schema Across Years |
|---|---|---|---|---|---|
| `schedules` | 1 | 1999–2026 | 7,548 | 0 | Single file (46 cols) |
| `pbp` | 28 | 1999–2026 | 1,308,093 | 0 | Uniform (372 cols) |
| `stats_player_week` | 28 | 1999–2026 | 486,957 | 541 | Uniform (150 cols) |
| `stats_team_week` | 28 | 1999–2026 | 14,856 | 1 | Uniform (138 cols) |
| `players` | 1 | all | 24,871 | 0 | Single file (39 cols) |
| `weekly_rosters` | 11 | 2016–2026 | 495,380 | 153 | `height`: `Float64 -> Int32` at 2025 |
| `snap_counts` | 14 | 2013–2026 | 339,829 | 0 | Uniform (16 cols) |
| `injuries` | 18 | 2009–2026 | 94,231 | 0 | `season`/`week`: `Float64 -> Int32` at 2021; `+season_type`, `-date_modified` at 2025 |
| `depth_charts` | 2 | 2025–2026 | 1,388,332 | 0 | Uniform (12 cols) |
| `ngs_passing` | 1 | 2016–2026 | 6,299 | 0 | Single file (29 cols; 444 `week=0` season-total rows) |
| `ngs_rushing` | 1 | 2016–2026 | 6,432 | 0 | Single file (22 cols; 558 `week=0` season-total rows) |
| `ngs_receiving` | 1 | 2016–2026 | 15,598 | 0 | Single file (23 cols; 1,369 `week=0` season-total rows) |
| `pbp_participation` | 10 | 2016–2025 | 478,989 | 0 | `play_id`: `Int32 -> Float64`, +6 name/number/pos cols at 2023 |
| `ftn_charting` | 5 | 2022–2026 | 212,685 | 0 | Uniform (29 cols) |
| `pfr_week_pass` | 9 | 2018–2026 | 5,776 | 0 | Uniform (24 cols) |
| `pfr_week_rush` | 9 | 2018–2026 | 19,671 | 0 | Uniform (16 cols) |
| `pfr_week_rec` | 9 | 2018–2026 | 38,139 | 0 | Uniform (17 cols) |
| `pfr_week_def` | 9 | 2018–2026 | 66,695 | 0 | Uniform (29 cols) |

> **DuckDB reading note:** Because `weekly_rosters`, `injuries`, and `pbp_participation` changed column types or column sets across years, DuckDB queries over multi-year globs must pass `union_by_name=true` (and cast `play_id` / `season` / `week` explicitly where needed).

---

## 2. Verified Normalization, Join & Kickoff Maps

### 2.1 Complete Team-Code Normalization Map
Across every team column of all 18 datasets in `data/clean/`, only **four** non-standard team codes (plus empty string `''` on non-possession plays) exist:

| Raw Code | Normalized Code | Where It Appears in `data/clean/` |
|---|---|---|
| `OAK` | `LV` | `schedules`, `weekly_rosters`, `snap_counts`, `injuries`, `pbp_participation`, `pfr_week_*` |
| `SD` | `LAC` | `schedules`, `weekly_rosters`, `snap_counts`, `injuries`, `pbp_participation` |
| `STL` | `LA` | `schedules`, `snap_counts`, `injuries` |
| `LAR` | `LA` | `ngs_passing`, `ngs_rushing`, `ngs_receiving` |
| `''` | `NULL` | `pbp.posteam`, `pbp_participation.possession_team` (timeouts, quarter ends, no-possession rows) |

*Note:* The 2015 NFL internal roster codes (`SL`, `ARZ`, `BLT`, `CLV`, `HST`) do not appear because `weekly_rosters` starts at `min_season: 2016`. After applying the 4 mappings above, every team column in every Stage 4 core table must belong strictly to the **32 current NFL team codes**.

### 2.2 Next Gen Stats Super Bowl Week Mapping
In `schedules` and `stats_player_week`, the Super Bowl is `week = 21` (2016–2020) and `week = 22` (2021+). In `ngs_passing`, `ngs_rushing`, and `ngs_receiving`, NFL Next Gen Stats counts the Pro Bowl bye week, so **the Super Bowl is stored one week higher**:
- `season <= 2020 AND season_type = 'POST' AND week = 22` → map to `week = 21`
- `season >= 2021 AND season_type = 'POST' AND week = 23` → map to `week = 22`
- Also exclude `week = 0` (season-total summary rows: `444` passing, `558` rushing, `1,369` receiving).

**Measured result after applying that mapping:** `0` unmatched rows across all `25,958` weekly NGS rows (`ngs_passing`: `0/5,855`, `ngs_rushing`: `0/5,874`, `ngs_receiving`: `0/14,229`). Without it, every Super Bowl silently drops all NGS metrics.

### 2.3 Secondary ID Uniqueness & Cross-Dataset Coverage

**`players.parquet` secondary IDs (join fan-out safety):**
- `pfr_id` (`22,706`), `espn_id` (`16,603`), `nfl_id` (`11,941`), `pff_id` (`11,651`), `otc_id` (`9,833`): **100% unique (`0` duplicates)** — safe for 1:1 bridge joins in Stage 4 as long as uniqueness is guarded at publish time.
- `esb_id` and `smart_id`: 1 duplicate (`Paschal Ekeji`, listed under both `00-0040793` and `EKE080143`) — do not join on these.

**Measured ID match rates (`1999–2015` vs `2016–2026`):**

| Source → Target | 1999–2015 (or early era) | 2016–2026 | Notes |
|---|---|---|---|
| `stats_player_week.player_id` → `players.gsis_id` | `99.98%` (61 non-skill misses; **100.00%** skill) | **100.00%** (`0` misses / 193,368 rows) | Spotless from 2016+ |
| `injuries.gsis_id` → `players.gsis_id` | **100.00%** (`0` misses) | **100.00%** (`0` misses) | Spotless |
| `ngs_*.player_gsis_id` → `players.gsis_id` | — | **100.00%** (`0` misses) | Spotless |
| `weekly_rosters.gsis_id` → `players.gsis_id` | — | `97.56%` (`12,105` misses; `97.58%` skill) | Practice-squad / camp cuts not in `players` |
| `depth_charts` → `players` (`gsis_id`, fallback `espn_id`) | — | `98.86%` (2025), `97.57%` (2026); `393` rescued via `espn_id` | Remaining `~2%` are fringe camp/practice players |
| `snap_counts.pfr_player_id` → `players.pfr_id` | `99.98%` (2013–2015; `99.96%` skill) | `99.91%` (`99.85%` skill) | Very clean |
| `pfr_week_{pass,rush,rec,def}` → `players.pfr_id` | — | `99.95%–100.00%` (`pass` 100%, `rush` 99.98%, `rec` 99.95%, `def` 99.98%) | Very clean |
| `ftn_charting` `(game_id, play_id)` → `pbp` | — | **100.00%** (`0` unmatched / 212,685 plays) | Spotless |
| `pbp_participation` `(game_id, play_id)` → `pbp` | — | `2016–2022`: `5,396` unmatched (~770/yr); **`2023–2025`: `0` unmatched** | Left-join onto `play` so `play` never loses or duplicates rows |

### 2.4 Players Who Played Without a Stat Line (`snap_counts` → `player_game`)
`stats_player_week` has a row only for a player who recorded a stat. `snap_counts` (2013 on) has a row for everyone who was on the field.

- **All positions, `2013–2026`:** `93,923` snap-count rows of players in `players` have no stat line in that game. Offensive linemen are the largest group (`33,237` of the `74,842` from 2016 on).
- **QB, RB, WR and TE with an offensive snap, `2016–2026`:** `8,066` of `68,063` player-games have no stat line: TE `25.4%`, WR `10.3%`, RB `4.3%`, QB `2.0%`. `194` of them played 40 snaps or more.
- **What an average over stat lines alone does:** tight ends in 2025 averaged `3.12` targets over their stat lines and `2.38` over every game with an offensive snap.

`player_game` therefore holds every stat line plus one row for each of those snap-count rows, with `has_stat_line` false and the values upstream writes in an empty stat line (`0` in counts and shares, null in the per-attempt rates). `86` of the `8,066` carry a PFR id that `players` does not have and cannot be keyed, so they stay out. In the built table `0` of the `67,977` skill-position snap rows with a known player are missing, and `486,957` rows have `has_stat_line`, one per `stats_player_week` row.

### 2.5 Kickoff Timestamps (`schedules` → `game.kickoff_utc`)
- **Null `gametime` rows:** Only season `1999` (`259` games). Every game from `2000–2026` (`7,289` games) has a valid `HH:MM` US Eastern kickoff time.
- **Measured UTC kickoff hour distribution (all `7,289` games from `2000–2026`):**
  - Every single game starts between **`13:00` UTC and `03:59` UTC** (London/international morning games at `13:30`/`14:30` UTC through late Monday Night West Coast doubleheaders up to `23:30` ET = `03:30` UTC in `2013_05_SD_OAK`).
  - **Zero (`0`) games start between `04:00` and `12:59` UTC.** Any kickoff landing in `04:00–12:59` UTC immediately flags a timezone or AM/PM conversion bug.

### 2.6 Games `snap_counts` Lists Under the Other Team (`snap_counts` → `player_game.team`)
A player with a stat line takes the team from `stats_player_week`. A player who is only in `snap_counts` can only take it from there.

- **Measured against the stat lines, `2013–2026`:** after team-code normalization, `snap_counts` and `stats_player_week` name the same team for every player they share, except in three Super Bowls where they disagree for every one: `2014_21_NE_SEA` (`58` of `58`), `2015_21_CAR_DEN` (`66` of `66`) and `2018_21_NE_LA` (`55` of `55`). There `team` and `opponent` are exchanged. No game is partly exchanged.
- **What it would do:** `30`, `23` and `35` players of those games have no stat line, so `88` `player_game` rows would sit on the wrong team, with `opponent` and `is_home` wrong too.
- **Build rule:** a game in which more shared players are listed under their opponent than under their own team is turned back before the join.
- **How a wrong team shows in the built table:** a team's players with a `100%` offense share all have that team's snap count. With the three games turned back, `0` team-games from `2013–2026` hold two such counts. As listed upstream, exactly the `6` team-games of the three Super Bowls do (`53` and `74`, `60` and `80`, `65` and `72` snaps). `5,829` of the `5,848` team-games from 2016 have at least one such player.
- **Why the 11-player rule cannot do this:** it reads `snap_counts`, where an exchanged game still has 11 players a side. Read from `player_game` it would catch `2018_21_NE_LA` (`9.125`), but a full-time lineman whose PFR id `players` lacks has no row there, which already puts `9` Buffalo team-games from 2024 to 2026 exactly on its lower limit of `10.0`. One more missing player would stop every build.

### 2.7 Injury Report Times (`injuries.date_modified` → `player_week_status.injury_report_modified_utc`)
`date_modified` is a UTC timestamp in every season that has it. Through the 2020 season the clock time in it is not UTC.

| Seasons | Median hour as published, September / November | Median hour in US Eastern, as published | Median hours from report to kickoff, as published |
|---|---|---|---|
| `2016–2020` | `11.72–12.35` / `11.77–12.45` (no daylight-saving shift) | `7.25–7.73` | `54.7–55.1` |
| `2021–2024` | `19.18–19.58` / `19.62–20.25` (shifts with daylight saving, as UTC does) | `15.05–15.50` | `47.1–47.6` |

- **Reading:** the earlier seasons hold a local clock time, and clubs did not file their reports at 7 in the morning. Read as `America/Los_Angeles`, those seasons have a median Eastern hour of `14.82–15.37` and a median of `47.3–47.6` hours to kickoff, the same as the later seasons. That the clock is Pacific is inferred from these numbers; upstream does not document it.
- **Where it changes:** between seasons. The January 2021 rows of the 2020 season still hold Pacific time (median hour `12.48`), and September 2021 holds UTC (`19.52`).
- **Build rule:** seasons through 2020 are read as `America/Los_Angeles` and stored as UTC. `core_injury_report_times_in_the_afternoon` stops the build if a season's median leaves the afternoon, as it would if upstream republished the early seasons in UTC.
- **Not changed:** `24` rows from `2016–2024` were last modified after their team's kickoff, with or without the conversion.

---

## 3. Calibrated Strategy 1 Checkbook Rules (Stages 3–6)

Every rule below returns failing rows (`0` rows = pass). Hard rules (`error`) block publishing; warnings (`warn`) print loudly without stopping the run. A rule whose query cannot be run, because a file it reads is unreadable or has lost a column, blocks publishing whatever its level. Sections 3.2 and 3.3 use the names the checks have in `nfl_lake/checks.py`.

### 3.1 Measured Historical Comparison: `1999–2015` vs `2016–2026`

This side-by-side comparison explains why **`2016+` is dramatically cleaner** for strict enforcement:

| Checkbook Identity | 1999–2015 (9,008–9,030 team-games) | 2016–2026 (5,848 team-games) | Root Cause of 1999–2015 Diffs |
|---|---|---|---|
| `sum(passing_tds) == sum(receiving_tds)` | **0 diffs** | **0 diffs** | Exact across all 28 seasons |
| `sum(stats_player_week) == stats_team_week` (all 12 core stat cols) | 1–5 diffs per col | **0 diffs across all 12 cols** | Early-era null-key / legacy stat quirks |
| `derived_pts(stats_team_week) == schedules` score | 22 missing team rows; 18 point diffs | **0 missing, 0 diffs** | `1999–2000` 3 play-less games; `2001–2002` JAX home rows missing & rolled into visitor |
| `sum(pbp.passing_yards) == stats_team_week.passing_yards` | 16 diffs (max 311 yds) | **0 diffs** | Early-era PBP gaps |
| `sum(pbp.rushing_yards + lateral_rushing_yards) == stats_team_week.rushing_yards` | 41 diffs (max 223 yds) | **0 diffs** *(27 diffs without laterals!)* | Laterals explain 100% of 2016+ diffs |
| `sum(pbp.pass_td / rush_td) == stats_team_week` | 13 diffs each | **0 diffs** | Early-era PBP gaps |
| `receptions <= targets` | 21,583 violations (`2002–2008`) | **0 violations** | `targets` wasn't tracked reliably until 2009 |
| `completions <= attempts` | **0 violations** | **0 violations** | Exact across all 28 seasons |
| `stats_team_week` `(attempts - targets)` gap | p95 = `41.0`, max = `66` | p50 = `1.0`, p95 = `4.0`, p99 = `5.0`, max = `9` (`0` cases `< 0`) | Same pre-2009 target gap |
| `snap_counts` `sum(offense_pct)` per team-game | `10.80`–`11.03` (2013–2015) | p0.5 = `10.97`, p50 = `11.00`, p99.5 = `11.05`; 2 outliers (`10.34`, `10.49`) | PFR used wrong denominator on 2019 KC Weeks 16–17; raw `sum(offense_snaps)/max(offense_snaps)` = exact `11.000` on both |

### 3.2 Stage 3 (Ingest `data/clean/`) Cross-Dataset Health Checks

| Rule | Level | Exact Condition | Known Historical Exceptions |
|---|---|---|---|
| `clean_reg_game_counts` | `error` | Completed seasons in `schedules` have `248` REG (`1999–2001`), `256` REG (`2002–2020`), `272` REG (`2021+`). | `2022` has `271` REG (cancelled Week 17 BUF–CIN). |
| `clean_no_orphan_game_ids`, `clean_participation_no_orphan_game_ids`, `clean_ftn_no_orphan_game_ids` | `error` | `0` `game_id`s in `pbp`, `stats_player_week`, `stats_team_week`, `snap_counts`, `pfr_week_*`, `pbp_participation`, `ftn_charting` missing from `schedules`. | None (`0` orphans across all 10 sources). |
| `clean_played_games_in_core_feeds` | `error` | Every played game in `schedules` appears in `pbp`, `stats_player_week`, `stats_team_week`, and from 2013 `snap_counts`. | `1999_01_BAL_STL`, `2000_03_SD_KC`, `2000_06_BUF_MIA` missing from `pbp`/`stats_*`. Expected to fail between a final score reaching `schedules` and the next update of the other feeds; that has not been observed or timed. |
| `clean_ftn_in_season_lag`, `clean_pfr_in_season_lag` | `warn` | Played games since 2022 not yet in `ftn_charting`, and since 2018 not yet in `pfr_week_pass`. | On the measurement date: 1 game and 12 games. |

### 3.3 Stage 4 (Core Tables `data/core/`) Publish-Time Checks

All rules below run on `data/core/` in staging **before** publishing. Strict rules apply to **`season >= 2016`** (or all seasons where noted as `all`).

| Family | Rule | Level | Scope | Exact Check |
|---|---|---|---|---|
| **Shape & Joins** | `core_keys_unique` | `error` | all | Primary key unique + non-null on all 7 core tables. |
| **Shape & Joins** | `core_zero_join_fanout` | `error` | all | `count(game) == count(schedules)`, `count(player) == count(players)`, `count(play) == count(pbp)`, `count(depth_chart_snapshot) == count(depth_charts)`, `count(team_game) == 2 * count(played games in game)` (`2016+`). For `player_game`: rows with `has_stat_line` `== count(stats_player_week)`, and all rows `==` the stat lines plus the `snap_counts` rows of known players with no stat line in that game (Section 2.4). |
| **Shape & Joins** | `core_team_game_symmetry` | `error` | `2016+` | Every played game has exactly 2 `team_game` rows (1 home, 1 away) with matching reciprocal `opponent`. |
| **Shape & Joins** | `core_players_bridge_ids_unique` | `error` | all | `players.pfr_id` and `players.espn_id` have `0` duplicates among non-null values. |
| **Shape & Joins** | `core_foreign_keys_hold` | `error` | all / `2016+` | Every `game_id` in `team_game`, `player_game` and `play` exists in `game`; from 2016, `100%` of `player_game.player_id` exist in `player.gsis_id`. |
| **Shape & Joins** | `core_snap_join_coverage` | `error` | `2016+` | `>= 99.5%` of `snap_counts` QB/RB/WR/TE rows resolve to `player.gsis_id` via `pfr_id` (measured: `99.85%`). |
| **Shape & Joins** | `core_ngs_weekly_join_complete` | `error` | `2016+` | After dropping `week=0` and mapping SB week (`22->21`, `23->22`), `0` weekly NGS rows fail to join `player_game`. |
| **Standardization** | `core_only_32_current_teams` | `error` | all | Every non-null team column across all 7 core tables is in the 32 current NFL team codes (zero `OAK`, `SD`, `STL`, `LAR`, `''`). |
| **Standardization** | `core_kickoff_utc_valid` | `error` | `2000+` | `game.kickoff_utc` is non-null UTC and never falls in `04:00–12:59` UTC. |
| **Standardization** | `core_injury_report_times_in_the_afternoon` | `error` | `2016+` | Per season, the median `player_week_status.injury_report_modified_utc` falls between `13:00` and `18:00` US Eastern (measured: `14.82–15.50`; Section 2.7). |
| **Checkbook** | `core_pass_td_equals_rec_td` | `error` | all | `sum(player_game.passing_tds) == sum(player_game.receiving_tds)` per `(game_id, team)`. |
| **Checkbook** | `core_player_sums_equal_team` | `error` | `2016+` | `sum(player_game)` == `team_game` for all 12 core cols (`completions`, `attempts`, `passing_yards`, `passing_tds`, `passing_interceptions`, `carries`, `rushing_yards`, `rushing_tds`, `receptions`, `targets`, `receiving_yards`, `receiving_tds`). |
| **Checkbook** | `core_team_points_equal_schedule` | `error` | `2016+` | Derived `team_game` points (`6*(pass+rush+def+st+fum_rec TDs) + 3*fg_made + pat_made + 2*(rush_2pt+rec_2pt+def_2pt+def_safeties)`) == `game.home_score` / `away_score`. |
| **Checkbook** | `core_pbp_equals_team_boxscore` | `error` | `2016+` | `sum(play)` == `team_game` for `passing_yards`, `rushing_yards + lateral_rushing_yards`, `pass_touchdown`, and `rush_touchdown` per `(game_id, team)`. |
| **Checkbook** | `core_targets_vs_attempts_gap` | `error` | `2016+` | `0 <= team_game.attempts - team_game.targets <= 15` (measured max: `9`). |
| **Checkbook** | `core_snap_teams_consistent_within_game` | `error` | all | Within a game, `snap_counts` lists every player who has a stat line for one team under one team: the game is listed as played or wholly under the other team, never partly (Section 2.6). |
| **Checkbook** | `core_full_time_offense_one_snap_count` | `error` | all | Within a `player_game` team-game, every row with `offense_pct = 1` has the same `offense_snaps` (measured: `0` exceptions; Section 2.6). |
| **Checkbook** | `core_snap_11_player_physics`, `core_snap_pct_11_player_warn` | `error`, `warn` | `2016+` | `error` if `sum(offense_snaps) / max(offense_snaps)` outside `[10.0, 12.0]` (measured: `10.275–11.830`, p0.5–p99.5 = `10.986–11.015`); `warn` if `sum(offense_pct)` outside `[10.5, 11.5]` (flags only the 2 known 2019 KC PFR-denominator games). |
| **Domain Bounds** | `core_stat_ranges_valid` | `error` | `2016+` | `completions <= attempts`, `receptions <= targets`, counts `>= 0`, `offense_pct` in `[0, 1]`, `passing_yards` in `[-50, 650]`, `rushing_yards` in `[-60, 350]`, `receiving_yards` in `[-50, 380]` (measured historical extremes: pass `-16..527`, rush `-34..296`, rec `-22..329`). |
| **Anchors** | `core_anchor_games_exact` | `error` | pinned | Each pinned game in Section 5 that is in `game` matches the values the check pins for it. |

### 3.4 Stage 5 (Features `data/features/`) Publish-Time Checks (not built)
- **`time_travel_zero_leakage` (`error`):** For 2–3 real historical cutoffs (e.g. Week 6 2024 kickoff, Week 12 2025 kickoff), rebuild features on a truncated copy of `data/core/` with all rows having `kickoff_utc >= cutoff` and `depth_chart_snapshot.dt >= cutoff` removed. Every feature value for the target week must be **100% bit-for-bit identical** to the full-lake build.
- **`row_order_invariant` (`error`):** Shuffle 1 full season of `data/core/` input rows, rebuild features, sort by key, and assert **100% equality** with the unshuffled build.
- **`feature_ranges_and_finiteness` (`error`):** Zero `Inf`/`-Inf`; share/rate features in `[0.0, 1.0]`; single-game team share sums equal `1.0` (`±1e-6`); Week 5+ null rates stay under configured ceilings per column.
- **`feature_row_preservation` (`error`):** `1:1` key match with `player_game` (plus scheduled upcoming players when running inference).

### 3.5 Stage 6 (Projections & DraftKings `data/projections/`) Publish-Time Checks (not built)
- **`dk_offense_reconciles_ppr` (`error`):** Verify our computed DraftKings offensive points match `fantasy_points_ppr + documented DK deltas` across 100% of `2016+` player-games (see Section 4 below).
- **`dk_kicker_buckets_exact` (`error`):** `fg_made == fg_made_0_19 + fg_made_20_29 + fg_made_30_39 + fg_made_40_49 + fg_made_50_59 + fg_made_60_` (`0` diffs across `5,792` kicker-games in `2016–2026`).
- **`dk_anchor_scores_exact` (`error`):** Exact match to `0.01` pts on all offensive, DST, and kicker anchor games in Section 5.
- **`inactive_and_bye_zero` (`error`):** Players listed `Out`/`IR` or on a bye week receive `0.0` projected points.
- **`team_projection_conservation` (`warn`):** Sum of projected team player touchdowns/points stays within a calibrated band of Vegas implied team totals from `spread_line` and `total_line`.
- **`walk_forward_backtest_gate` (`error`):** Chronological out-of-sample MAE/calibration beats (or stays within threshold of) the simple baseline before promoting a new model.
- **`lineup_legality` (`error`):** Salary `<= 50,000`, exact slot counts from [draftkings.yml](../catalog/draftkings.yml), unique players, `>= 2` NFL teams.

---

## 4. DraftKings Scoring Specification (Resolves `SOURCE_INVENTORY.md` Open Question #1)

### 4.1 Exact Proven `nflverse` `fantasy_points_ppr` Formula
Tested across all **`486,957` rows (`1999–2026`)** with **zero (`0`) discrepancies**:

```sql
fantasy_points_ppr =
    0.04 * passing_yards + 4.0 * passing_tds - 2.0 * passing_interceptions
  + 0.10 * rushing_yards + 6.0 * rushing_tds
  + 0.10 * receiving_yards + 6.0 * receiving_tds + 1.0 * receptions
  - 2.0  * (sack_fumbles_lost + rushing_fumbles_lost + receiving_fumbles_lost)
  + 2.0  * (passing_2pt_conversions + rushing_2pt_conversions + receiving_2pt_conversions)
  + 6.0  * special_teams_tds
```

### 4.2 Exact Differences vs DraftKings Rules ([catalog/draftkings.yml](../catalog/draftkings.yml))
**`fantasy_points_ppr` does NOT match DraftKings scoring.** Do not train directly on `fantasy_points_ppr`; compute DraftKings points explicitly from stat columns because of **five exact differences**:

1. **Yardage bonuses (missing from `fantasy_points_ppr`):** `+3` for `passing_yards >= 300`, `+3` for `rushing_yards >= 100`, `+3` for `receiving_yards >= 100`.
2. **Interceptions:** DraftKings is `-1` (nflverse is `-2`).
3. **Fumbles lost:** DraftKings is `-1` (nflverse is `-2`). Also note: `fumbles_lost_total` differs from `sack_fumbles_lost + rushing_fumbles_lost + receiving_fumbles_lost` on `1,519` rows (`1999–2026`) because `fumbles_lost_total` **also includes punt/kickoff return fumbles lost** (e.g. `KaVontae Turpin`, `Charlie Jones`). Use `fumbles_lost_total` for offensive/return players under DraftKings rules (verify return-fumble scoring when re-checking `draftkings.yml` for 2026).
4. **Offensive fumble-recovery TDs (trap in `fumble_recovery_tds`!):** `fantasy_points_ppr` excludes `fumble_recovery_tds` completely. Importantly, `stats_player_week.fumble_recovery_tds` mixes **defensive** fumble-return TDs (`235` rows in `2016–2026`, e.g., Jessie Bates, Roquan Smith) with **offensive** fumble-recovery TDs (`24` rows in `2016–2026` where `fumble_recovery_own > 0 AND fumble_recovery_tds > 0`). Only offensive own-team fumble-recovery TDs (`+6`) belong on an offensive player's DraftKings score; cross-check against `pbp` (`posteam == team`) in Stage 4/6.
5. **DST `points_allowed` cannot use raw scoreboard totals blindly:** Across `2016–2026`, **`380` team-games (`6.5%`)** had an opponent defensive TD (pick-six or fumble return TD) and **`158`** had a safety. Under DraftKings DST rules, opponent defensive TDs (6 pts each) do not count against your DST's points allowed — **confirm against the 2026 DraftKings rules page whether the PAT/2pt following an opponent pick-six counts against DST (historically DraftKings counts the PAT as special-teams points allowed, excluding only the 6-pt defensive TD itself)**.
6. **Kicker (`showdown` only):** All required columns already exist in `stats_player_week` with `0` bucket mismatches (`fg_made_0_19 + fg_made_20_29 + fg_made_30_39` = `3` pts, `fg_made_40_49` = `4` pts, `fg_made_50_59 + fg_made_60_` = `5` pts, `pat_made` = `1` pt).

---

## 5. Pinned Real Anchor Games (Measured Ground Truth)

Ten real NFL records, measured from the lake. `core_anchor_games_exact` pins part of each row on every `build-core` and `verify`; the query in `nfl_lake/checks.py` is the list of what it pins. Every `ppr` and `DK` score, and the remaining values, are ground truth for the Stage 6 checks and no code checks them yet.

| # | Edge Case Tested | Game / Player | Measured Ground Truth |
|---|---|---|---|
| **1** | 2015 old team code (`OAK`→`LV`) + PFR snap join | `2015_01_CIN_OAK` (`2015-09-13 16:25 ET`) | `kickoff_utc = 2015-09-13T20:25:00Z`; `home_team = 'LV'`, `away_team = 'CIN'`, score `13–33`; Derek Carr (`00-0031280`): `7/12`, `61` pass yds, `0` TD, `23` off snaps (`37%`); Andy Dalton (`00-0027973`): `25/34`, `269` pass yds, `2` TD, `68` off snaps (`100%`). |
| **2a** | September EDT kickoff (`UTC-4`) | `2025_01_ARI_NO` (`2025-09-07 13:00 ET`) | `kickoff_utc = 2025-09-07T17:00:00Z`; `ARI 20 @ NO 13`. |
| **2b** | November EST kickoff (`UTC-5`) | `2025_10_BAL_MIN` (`2025-11-09 13:00 ET`) | `kickoff_utc = 2025-11-09T18:00:00Z`; `BAL 27 @ MIN 19`. |
| **3** | Mid-season trade (zero fan-out, right team per game) | Davante Adams (`00-0031381`) in 2024 | Wk 3 (`2024_03_CAR_LV`, `LV`): `9` tgt, `4` rec, `40` yds, `0` TD, `61` snaps (`95%`); Wk 7 (`2024_07_NYJ_PIT`, `NYJ`): `9` tgt, `3` rec, `30` yds, `0` TD, `55` snaps (`96%`); Wk 15 (`2024_15_NYJ_JAX`, `NYJ`): `12` tgt, `9` rec, `198` yds, `2` TD, `54` snaps (`93%`). |
| **4** | Postseason OT + Super Bowl NGS week (`23→22`) + 300-yd DK bonus | `2023_22_SF_KC` (Super Bowl LVIII, `2024-02-11 18:30 ET`) | `kickoff_utc = 2024-02-11T23:30:00Z`, `overtime = 1`, `SF 22 @ KC 25`; Patrick Mahomes (`00-0033873`): `34/46`, `333` pass yds, `2` pass TD, `1` INT, `66` rush yds, `79` snaps (`100%`), `ppr = 25.92`, **`DK = 29.92`** (`+3` bonus, `+1` INT delta), and NGS passing row joins (`46` att, `333` yds). |
| **5** | 2025+ snapshot depth chart strictly before kickoff (`dt < kickoff_utc`) | `2025_10_ARI_SEA` (`2025-11-09 16:05 ET`, `21:05:00Z`) | Latest `SEA` snapshot before `2025-11-09T21:05:00Z` is `dt = 2025-11-09T07:13:57Z`; `QB` `pos_rank = 1` is Sam Darnold (`gsis_id = 00-0034869`, `espn_id = 3912547`). |
| **6** | DK 300+ QB + 3 INT + 2pt conversion | Jameis Winston (`00-0031503`) in `2024_13_CLE_DEN` | `497` pass yds, `4` pass TD, `3` INT, `11` rush yds, `1` pass 2pt, `0` FL; `ppr = 32.98`, **`DK = 38.98`** (`+3` bonus, `+3` INT delta). |
| **7** | DK 100+ rusher + receiving TD + fumble lost | Saquon Barkley (`00-0034844`) in `2024_09_JAX_PHI` | `27` car, `159` rush yds, `1` rush TD, `3` rec, `40` rec yds, `1` rec TD, `1` FL; `ppr = 32.90`, **`DK = 36.90`** (`+3` bonus, `+1` FL delta). |
| **8** | DK DST + opponent pick-six points-allowed test | `2024_13_CLE_DEN` (`DEN 41, CLE 32`; DEN had 2 pick-sixes) | **DEN DST:** `3` sacks (`+3`), `3` INT (`+6`), `2` def TDs (`+12`), allowed `32` pts w/ `0` CLE def/ST TDs (`-1`) → **`DK = 20.0`**. **CLE DST:** scoreboard shows `41` pts (`-4`), but DEN defense scored `2` pick-sixes (`12` pts + `2` PATs = `14` pts); subtracting the 2 defensive TDs yields `29` pts allowed (`-1`) instead of `41` (`-4`). |
| **9** | DK Kicker (all 3 FG brackets + PAT) | Jake Moody (`00-0038562`, `SF`) in `2024_01_NYJ_SF` | `2` FG 0–39 (`6`), `2` FG 40–49 (`8`), `2` FG 50+ (`10`), `2` PAT (`2`), `6` total FG → **`DK = 26.0`**. |
| **10** | Played without a stat line (row comes from `snap_counts` alone) | Cade Otton (`00-0038129`, `TB`) in `2025_03_NYJ_TB` | `66` off snaps (`92%`), `0` targets, no row in `stats_player_week`; `player_game` has the row with `has_stat_line` false. Weeks 1, 2, 4 and 5 have stat lines (`3`, `4`, `4`, `5` targets). |

---

## 6. Why Strict Checks Start in 2016

`data/clean/` holds `1999–2026`, and the strict cross-source `error` checks apply to `season >= 2016`:

1. Every cross-source checkbook identity (`player == team`, `derived_pts == schedule`, `pbp == team`, `receptions <= targets`, `stats_player_week -> players`, `ngs_weekly -> player_game`) has `0` failures across all `5,848` team-games from `2016–2026`.
2. `weekly_rosters`, `pbp_participation`, and `ngs_*` all begin in `2016` (and `snap_counts` is complete for every game from `2013+`).
3. Before `2016`, real upstream nflverse artifacts exist (`1999` null gametimes, `1999–2000` 3 play-less games, `2001–2002` missing JAX team rows, `2002–2008` missing targets, `2002–2015` duplicate weekly rosters).
