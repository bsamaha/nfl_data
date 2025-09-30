### docs/TODO.md

### DFS NFL Roadmap and TODO

Goal: Build a slate-ready DFS system that surfaces value, leverage, stacks, matchup edges, and injury-driven role changes, leveraging our lake (pbp/weekly/snap_counts/rosters/schedules) plus a small set of external feeds (salaries, odds, weather, ownership).

Scope: DraftKings-only
- Only DraftKings data and formats are in scope.
- Scoring: DK PPR with yardage bonuses; all projections/value/leverage computed in DK points.
- Formats: DK Classic (primary), DK Showdown (secondary), DK Best Ball (supporting for draft prep). All lineup constraints and pages are tuned to DK.

DK formats coverage (status and tasks)
- Present: `catalog/draftkings/bestball.yml`, `catalog/draftkings/classic.yml`, `catalog/draftkings/showdown.yml` (rules/scoring present).
- Tasks:
  - Extend `data/gold/player_week_fantasy/` logic:
    - Ensure DK bonuses and offensive scoring; add DST fantasy scoring; add K scoring (for Showdown analysis).
    - Showdown handling: keep base DK points; apply 1.5x in lineup sims/optimizers (not in base scoring).

---

### Linear execution plan (phases with concrete deliverables)

1) Foundation: team/game context marts (internal, no external feeds)
- Deliverables:
  - `data/gold/utilization/team_week_context/` (team denominators: dropbacks, attempts, carries, snaps)
  - `data/gold/utilization/team_proe_and_pace_by_week/` (neutral PROE, neutral pace)
  - `data/gold/utilization/player_week/` (wide per-player-week utilization; WR/TE/RB)
  - `data/gold/player_week_fantasy/` (DK PPR fantasy points from weekly)
- How:
  - Add SQL under `queries/utilization/` (templates exist in `docs/FEATURE_REQUEST__UTILIZATION_REPORT.md` and `docs/UTILIZATION_USAGE.md`)
  - Materialize current season only (2025) after each `update --season 2025`
- Acceptance:
  - Spot check a handful of players vs public reporting (targets, end-zone targets, RZ carries, aDOT, YPRR/TPRR when routes exist)
  - Query runtime: <30s for season scope on laptop

2) Game Environments (internal + existing pbp vegas columns)
- Deliverables:
  - `data/gold/game_week_context/` with game_id, teams, `total_line`, `spread_line`, implied team totals, neutral PROE/pace joins
  - Streamlit page `app/pages/2_Slate_Game_Environments.py`
- How:
  - Aggregate existing `pbp.total_line/spread_line` by game_id/week (fallback to team-level if needed)
  - Compute implied totals, rank games by “Env Score” (blend: total, PROE, pace)
- Acceptance:
  - Page loads <2s, sortable/filterable by week and totals; top 3 games align with market expectations

3) Salaries (add dataset + importer; DK-only)
- Deliverables:
  - New dataset: `dfs_salaries` (silver), partitions: `season,week,site`
  - Parser for DK Contest CSV export (manual upload v1; API/scrape later)
  - Streamlit Upload widget (temporary) to write parquet under `data/silver/dfs_salaries/season=<YYYY>/week=<W>/site=dk/`
- How:
  - Users export DK contest CSV (“Export to CSV” in lobby), fields: `Position,Name,Name + ID,ID,Salary,Game Info,TeamAbbrev,AvgPointsPerGame`
  - Script `src/importers/dfs_salaries.py` or a small app widget parses to schema: `(season,week,site,player_id,team,position,salary,game_id)` with `site='dk'`
  - Map `player_id` via `ids` or exact match on `Name + team` fallback
- Acceptance:
  - ≥95% name→ID mapping for main slate, remaining rows logged for manual fixes

4) Value (points per dollar) using our internal projections v1 (DK points)
- Deliverables:
  - `data/gold/player_week_projection/` (simple v1 projections)
  - `data/gold/player_value_week/` (join with DK salaries; value metrics: proj_pts, value per $1k)
  - Add Value tab to `1_Player_Weekly_Stats` or new `6_Value_And_Leverage.py` (Value-only first)
- How (v1 projection):
  - Team plays (pace) × pass/rush splits (neutral PROE) → team pass attempts & carries
  - Shares: recent target_share/carry_share trends + RZ/End-zone shares; WR/TE route_participation (nullable)
  - Efficiency priors: league-average yards/target, TD rates by aDOT bucket; regression to mean
  - Optional xFP-lite as sanity anchor
- Acceptance:
  - Backtest last week: top value plays identify obvious pricing errors

5) Ownership (DK-only; CSV ingest v1) and Leverage
- Deliverables:
  - Dataset: `dfs_ownership` (silver), partitions: `season,week,site,slate` (`site='dk'`)
  - CSV upload widget (expected columns: `player_id, projected_ownership` or `player_name,team,pos,proj_own`)
  - Extend `player_value_week` to add `proj_own` and compute `leverage_score = proj_pts * (1 - proj_own)`
  - Streamlit page `6_Value_And_Leverage.py` (Value + Leverage)
- How:
  - Accept CSV exports from any source; or use our in-house DK ownership model
  - Minimal harmonizer to IDs; store to parquet
  - Display: rank by leverage, show pivots (same team/price tier)
- Acceptance:
  - Page shows meaningful positive leverage pivots; missing IDs <5% with clear warnings

6) Odds (API) and better implied totals (keep requests lean)
- Deliverables:
  - Dataset: `odds_lines` (silver), partitions: `season,week`, keys `(game_id, bookmaker, market, timestamp)`
  - Refresh job to fetch totals/spreads once per hour on slate days
  - Replace pbp `total_line/spread_line` in `game_week_context` with live odds when present
- How:
  - Use a light odds API (free/low-cost): The Odds API → `GET /v4/sports/americanfootball_nfl/odds?regions=us&markets=spreads,totals`
  - Map to our `schedules.game_id` via team abbreviations + start times
- Acceptance:
  - Totals in UI match live markets within a minute of fetch time

7) Weather (API) integration
- Deliverables:
  - Dataset: `weather_forecast` (silver), partitions: `season,week`, keys `(game_id, forecast_hour)`
  - Weather impact tags in `game_week_context` (wind, rain, temp; downgrade/upgrade signals)
- How:
  - Use OpenWeather/One Call or Open‑Meteo/NWS per stadium lat/lon
  - Join to `schedules` (kickoff datetime and stadium mapping file)
- Acceptance:
  - For outdoor stadiums, UI shows wind/rain flags; domes show neutral

8) Opportunity/Regression dashboards (buy-low + RZ/End-zone trends)
- Deliverables:
  - `data/gold/player_week_trends/` (last 3/5 deltas for target_share, carry_share, route_participation, rz/end-zone opp minus TDs)
  - UI: “Buy-Low Air Yards & xFP Delta” and “RZ/End-Zone Trend” sections
- How:
  - Compute rolling trends over last N weeks; surface underperformance vs xFP-lite
- Acceptance:
  - Consistently surfaces players with poor recent outcomes but strong underlying opp

9) Matchups: Coverage/DvP and OL/DL Mismatches
- Deliverables:
  - `data/gold/team_week_dvp/` (allowed YPRR, aDOT, RB target rate, TE RZ targets, man/zone rates)
  - `data/gold/ol_dl_mismatch/` (pressure rates for/against, TTT diffs, run success vs box)
  - Two new pages: `8_Coverage_Matchups.py`, `9_OL_DL_Mismatches.py`
- How:
  - Use `pbp.defense_*_type`, `was_pressure`, `time_to_throw`, `defenders_in_box`, personnel
- Acceptance:
  - Flags align with observed team styles; actionable mismatches for the slate

10) Stacks and correlation (Classic; Showdown later)
- Deliverables:
  - `data/gold/utilization/player_pair_correlation/` (teammates correlation by team/season)
  - `app/pages/10_Stack_Finder.py` (QB stacks + bring-back ranking using env + value + ownership)
- How:
  - From `player_week_fantasy` compute teammate correlations; score stack candidates
- Acceptance:
  - Top stacks come from top environment games; bring-backs make sense

11) Showdown (later)
- Deliverables:
  - Captain leverage helper; sim-lite based on variance and correlation; apply 1.5x CAPTAIN points and salary
- How:
  - Use DK salary/ownership plus env to estimate captain optimality vs ownership
- Acceptance:
  - Produces viable captain pivots on island games

---

### Data sources and acquisition (DK-only; automated where possible)

- Salaries (DK)
  - Primary (automated): Pull DraftGroups and players (salaries) from DK’s unauthenticated endpoints; store normalized rows.
    - TASK: Implement `src/acquire/dk_api.py` with functions:
      - `list_draftgroups(sport='nfl')` → draftgroup_id, start_time, game_type (Classic/Showdown), contest_type
      - `get_draftgroup_players(draftgroup_id)` → `player_id, name, team, position, salary, draftgroup_id`
    - TASK: Mapper `src/importers/dfs_salaries.py` to write parquet to `data/silver/dfs_salaries/season,week,site` (`site='dk'`) with provenance `source='api'`.
    - TASK: Map draftgroup → season/week via kickoff time and `schedules`; store `slate_type` (Classic/Showdown).
  - Secondary (fallback): Contest CSV export (manual upload)
  - Notes: Enforce DK-only; robust name→ID mapping via `players/ids` with ambiguity logs.

- Contests (DK Lobby)
  - Goal: Capture lobby contest metadata to support game selection (rake, payout structure, field size, entry limits).
  - Primary (automated): Call DK’s public web endpoints used by the lobby (unauthenticated) to retrieve contest listings and payouts.
    - TASK: Implement `src/acquire/dk_contests_api.py` to fetch and normalize:
      - Contests list (id, name, entry_fee, prize_pool, rake_pct, field_size, entry_cap, max_multi_entry, se/3max/20max/150max flags, start_time, guaranteed, draftgroup_id, payout_table)
      - Payout structures (10th/1st ratio, min-cash multiple) and `eff_rake` derivation
    - Ingestion: write to `data/silver/dfs_contests/season,week,site` (`site='dk'`) with `source='api'`.
  - Secondary (fallback): Headless browser capture (Playwright) to fetch the same JSON XHRs; or user-provided HAR upload with identical parser. Mark `source='headless'|'har'`.
  - Schema (core): `contest_id, name, entry_fee, prize_pool, rake_pct, field_size, max_multi_entry, entry_cap, is_single_entry, is_three_max, is_twenty_max, is_one_fifty_max, start_time, guaranteed, min_cash, first_prize, payout_table(json), slate_ids(json), draftgroup_id`.
  - Derived metrics: `min_cash_multiple`, `top10_pct_of_first`, `payout_flatness_score`, `eff_rake`.


- Ownership projections (DK)
  - Method: CSV upload (any source) or in-house DK ownership model (heuristics/ML)
  - Storage: `data/silver/dfs_ownership/season,week,site,slate` (`site='dk'`)
  - Notes: Harmonize to player_id; allow name/pos/team fallback then manual review

- Odds (totals/spreads)
  - Provider: The Odds API
  - Endpoint: `/v4/sports/americanfootball_nfl/odds?regions=us&markets=spreads,totals`
  - Storage: `data/silver/odds_lines/season,week`; pick a primary bookmaker or consensus

- Weather
  - Provider: OpenWeather or Open‑Meteo/NWS
  - Inputs: Stadium lat/lon, kickoff timestamps from `schedules`
  - Storage: `data/silver/weather_forecast/season,week`

- Optional: Player props (for projection anchoring)
  - Provider: The Odds API props endpoints (paid tiers) or alternative prop APIs
  - Storage: `data/silver/props/season,week,market`
  - Use: Calibrate receptions/yard/TD expectations

- Injury/late news
  - Current: `injuries` + `depth_charts` already in lake; ensure update cadence daily and gameday morning
  - Future: “Inactives hour” inputs

---

### New datasets to register (catalog/datasets.yml)

- `dfs_salaries`:
  - partitions: `season,week,site`
  - key: `(season,week,site,player_id)`
  - enabled: true

- `dfs_ownership`:
  - partitions: `season,week,site,slate`
  - key: `(season,week,site,slate,player_id)`
  - enabled: true

- `dfs_contests`:
  - partitions: `season,week,site`
  - key: `(season,week,site,contest_id)`
  - enabled: true
### DraftKings automated retrieval — libraries/endpoints & scraping plan

- TASK: Evaluate maintained libraries (prefer Python), cite and record decision
  - `draftkings_client` Python library: Lightweight client for contests, draft groups, and player data. GitHub: https://github.com/bmjjr/draftkings_client
  - `draft-kings` Python client (unofficial; DFS endpoints): https://pypi.org/project/draft-kings/ (underlying repo often references https://github.com/SeanDrum/Draft-Kings-API-Documentation for endpoints)
  - `draftkings_api_explorer` Python GUI tool for exploring and exporting data: GitHub: https://github.com/yzRobo/draftkings_api_explorer
  - `draft.kings` R package (if R support added): Docs: https://gacolitti.github.io/draft.kings/ (GitHub: https://github.com/gacolitti/draft.kings)
  - Unofficial endpoint docs: https://github.com/SeanDrum/Draft-Kings-API-Documentation
  - Decision gate: last release activity, DFS coverage (contests, draftgroups, players), no auth required. Prioritize `draftkings_client` for Python integration.

- TASK: Define unauth endpoints to use (verify via network inspector)
  - Contests list: https://www.draftkings.com/lobby/getcontests?sport=NFL (GET; returns contest metadata like ID, name, entry fee, prize pool, field size, max entries, start time, guaranteed, draftGroupId, payout table)
  - Contest details (payouts): https://api.draftkings.com/contests/v1/contests/{contestId}?format=json (GET; detailed payouts for deriving metrics)
  - DraftGroups list: https://api.draftkings.com/draftgroups/v1/?sport=NFL (GET; returns draftGroupId, startTime, gameType e.g., Classic/Showdown)
  - Draftable players (pricing): https://api.draftkings.com/draftgroups/v1/draftgroups/{draftGroupId}/draftables (GET; returns playerId, name, position, team, salary, gameInfo)
  - Record base URLs, params, schemas; add retries/backoff and on-disk cache to avoid rate issues.
  - Notes: These endpoints are well-documented in open-source repos like https://github.com/SeanDrum/Draft-Kings-API-Documentation and used by many DFS tools.

- TASK: Build resilient fetchers
  - API-first modules: `dk_api.py`, `dk_contests_api.py` with typed parsers and schema validation. Example in `dk_api.py`:
    ```
    import requests
    from datetime import datetime
    import pandas as pd

    def list_contests(sport='NFL'):
        response = requests.get(f"https://www.draftkings.com/lobby/getcontests?sport={sport}")
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['contests'])
        df['fetched_at'] = datetime.utcnow()
        df['source'] = 'api'
        return df

    def get_draftgroup_players(draftgroup_id):
        response = requests.get(f"https://api.draftkings.com/draftgroups/v1/draftgroups/{draftgroup_id}/draftables")
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['draftables'])
        return df
    ```
  - Headless fallback (Playwright): capture XHR JSON; parse with same normalizer; respect robots/ToS.

- TASK: Scheduling & ops
  - Contests refresh cadence: every 10–15 minutes on slate days; hourly otherwise.
  - Salaries refresh cadence: when new draftgroups appear or on schedule (e.g., every 30 minutes Thu–Sun).
  - Provenance: add `source`, `fetched_at`, `endpoint` to every record.

- Legal/ToS & scope notes
  - Retrieve read-only metadata and salaries only; no lineup entry or gameplay automation.
  - Prefer unauth public endpoints; fall back to user-session headless only if required; throttle requests.


- `odds_lines`:
  - partitions: `season,week`
  - key: `(season,week,game_id,bookmaker,market,timestamp)`
  - enabled: true

- `weather_forecast`:
  - partitions: `season,week`
  - key: `(season,week,game_id,forecast_hour)`
  - enabled: true

(We can later enable `scoring_lines` if upstream coverage is timely for 2025; for now we use a direct odds API.)

---

### Queries and materialization (tasks to add under `queries/utilization/`)

- `team_week_context.sql` — team denominators (dropbacks/attempts/carries/snaps)
- `team_proe_and_pace_by_week.sql` — neutral PROE, neutral seconds/play
- `player_week_utilization.sql` — wide player-week utilization
- `player_season_utilization.sql` — seasonal rollups
- `player_pair_correlation.sql` — teammates correlation
- `player_week_trends.sql` — last 3/5 slopes and deltas
- `materialize_*` wrappers — write partitioned parquet for current season

- TASK: Add `player_week_fantasy.sql` — compute DK fantasy points for offense, K, DST (no code here; tracked as task)
- TASK: Add `materialize_player_week_fantasy.sql` — materialize `data/gold/player_week_fantasy/` (no code here; tracked as task)

Run examples (current season):
```bash
scripts/run_query.sh -f queries/utilization/team_week_context.sql -s 2025 -t REG
scripts/run_query.sh -f queries/utilization/team_proe_and_pace_by_week.sql -s 2025 -t REG
scripts/run_query.sh -f queries/utilization/player_week_utilization.sql -s 2025 -t REG -- -csv
```
```

---

### DK Game Selection dashboard (Levitan-inspired)

- Purpose: Help identify the best DK contests each week using rake, payout structure, field size, and entry-limit constraints.
- Data: `dfs_contests` (silver) + derived metrics; optional historical results to validate choices.
- Metrics shown per contest:
  - Rake % (lower is better), field size, guaranteed flag
  - Entry limits (single, 3-max, 20-max, 150-max)
  - Min-cash multiple (prefer 2x), Top-10 payout as % of 1st (prefer ~10%)
  - Payout flatness score, effective rake on top vs min-cash
- Views/filters:
  - Cash (SE Double-Ups, H2Hs): largest-field single-entry Double-Ups; H2H volume builder
  - SE/3-max mid-field GPPs with favorable structures
  - Large-field practice GPPs (20-max $1/$3) for optimizer reps
- Page: `app/pages/11_DK_Game_Selection.py`
- References: [Levitan’s DFS Game Selection](https://establishtherun.com/levitans-dfs-game-selection-which-contests-to-play/)