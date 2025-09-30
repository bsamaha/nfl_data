## nflverse Data Availability and Refresh Cadence

This document summarizes what the free nflverse data covers and when it refreshes in-season and off-season. Times are noted in UTC unless otherwise stated.

Sources: nflverse docs and package references (see links in each section).

### Important availability limitations (2025)
- Broken/unavailable in-season:
  - Injuries: No in-season nflverse feed in 2025; expect only end-of-season availability, if at all
- Postseason-only (no in-season updates):
  - Participation: Provided by FTN after all postseason games conclude; not published during the season

### Play-by-Play (PBP)
- What: Detailed play-by-play for every NFL game, with EPA/WPA modeling available via tooling.
- Refresh:
  - Nightly after each game day during the season
  - Additional intra-day refreshes at specific points on game days
  - Raw JSON route is typically available ~15 minutes after games conclude (for power users via `nflfastR::build_nflfastR_pbp()` / `update_db`)
  - Best practice: refresh again between Wednesday–Thursday nights for official stat corrections
- Notes: Historical seasons are stable; in-season data are subject to corrections mid-week.
- Reference: `nflreadr` / `nflfastR` docs

### Player and Team Stats (Aggregated)
- What: Aggregated weekly/season-to-date player and team statistics derived from PBP and other sources.
- Refresh: Nightly after each game day during the season; additional specific points on game days.
- Notes: Mirrors PBP correction cadence; expect small adjustments after official corrections.
- Reference: `nflreadr` docs

### FTN Charting Data
- What: Charted stats supplied by FTN.
- Refresh: 00:00, 06:00, 12:00, and 18:00 UTC daily during the season (subject to FTN availability).
- Reference: `nflreadr` data schedule

### Participation Data
- What: Player participation by play/game.
- Refresh:
  - Through 2022: Sourced from NFL Next Gen Stats (legacy)
  - 2023 onward: Provided by FTN and released after all postseason games conclude (no in-season updates)
- Status: Postseason-only in current ecosystem; not available in-season
- Reference: `nflreadr` data schedule

### Game Schedules and Results
- What: Schedule grid, game info, and results.
- Refresh: Every 5 minutes during the season.
- Reference: `nflreadr` data schedule

### Rosters (Weekly) and Rosters Seasonal
- What: Active/inactive rosters by week; seasonal roster rollups.
- Refresh: Daily at 07:00 UTC.
- Reference: `nflreadr` data schedule

### Depth Charts
- What: Team depth charts.
- Refresh: Daily at 07:00 UTC year-round.
- Notes: Post-2024 updates carry ISO8601 timestamps instead of week assignments.
- Reference: `nflreadr` data schedule

### Snap Counts (PFR)
- What: Player snap counts from Pro Football Reference.
- Refresh: 00:00, 06:00, 12:00, and 18:00 UTC daily during the season (subject to PFR availability).
- Reference: `nflreadr` data schedule

### PFR Advanced Stats
- What: Advanced statistics from Pro Football Reference.
- Refresh: Daily at 07:00 UTC during the season (subject to PFR availability).
- Reference: `nflreadr` data schedule

### Player-Level Weekly Next Gen Stats (NGS)
- What: Weekly advanced player metrics from NFL Next Gen Stats.
- Refresh: Nightly between 03:00–05:00 ET during the season (subject to NGS availability).
- Reference: `nflreadr` data schedule

### Injuries
- What: Player injury reports.
- Status: No in-season nflverse feed in 2025; only end-of-season availability (if published)
- Reference: `nflreadr` data schedule

### IDs and Players Master
- What: Crosswalks (e.g., GSIS↔PFR) and master player index.
- Refresh: Updated as sources change; generally daily to weekly cadence depending on upstream events and merges.
- Reference: `nflreadr` package docs and repository notes

### Optional/Additional nflverse Datasets
- Officials, win totals, scoring lines, draft picks, combine, and others are available within nflverse but may be disabled by default in some tooling. Update cadence varies by source; most are daily to weekly in-season when active.

---

### Supplemental alternative sources for limited datasets

#### Injuries (in-season alternatives)
- ESPN NFL Injuries: [ESPN injuries hub](https://www.espn.com/nfl/injuries)
- NFL.com Injuries: [NFL.com injuries](https://www.nfl.com/injuries/)
- NFL Communications: Weekly injury report PDFs (Wed–Fri practice statuses, game statuses) — see [NFL Communications](https://nflcommunications.com/) releases
- Team PR sites/press releases: Official team injury reports published throughout the week

Notes: Formats differ from nflverse and may require custom scraping/normalization. Licensing/terms of use apply.

#### Participation (post-game alternatives and proxies)
- NFL Gamebooks (PDFs): Post-game PDFs include participation tables (offense/defense/special teams). Available on game pages and team sites shortly after games
- Pro-Football-Reference snap counts: [PFR snap counts](https://www.pro-football-reference.com/years/) — a practical proxy for participation even if not play-by-play granular

Notes: Gamebook PDFs update after games, not intra-game; parsing requires PDF extraction and careful QA.

#### Depth charts (if week-based views are required)
- OurLads depth charts: [OurLads NFL depth charts](https://www.ourlads.com/nfldepthcharts/)
- ESPN team depth charts: Available on each team page; formats vary

Notes: nflverse depth charts remain available but use timestamped updates (no assigned week). External sources can be used to supplement week-based snapshots.

Operational guidance for this repository:
- Local lake promotion cadence aligns with upstream schedules noted above. See `docs/TECHNICAL_OVERVIEW.md` for ingestion/promote commands and local scheduling suggestions.

