# Feature Request: Defense vs. Position Fantasy Points Allowed Report

## Title
Add a Streamlit Report for Fantasy Points Allowed by Defenses to Specific Positions

## Description
Create a new Streamlit page in the `app/pages/` directory that allows users to analyze how NFL defenses perform against specific offensive positions (e.g., QB, RB, WR, TE) in terms of fantasy points allowed. This report will help users identify matchup advantages, such as defenses that concede above-average fantasy points to wide receivers (WRs), making them good targets for starting WRs in fantasy lineups.

The report should:
- Filter by season, weeks, and positions.
- Display average fantasy points allowed per game by each defense to selected positions.
- Compare each defense's performance to the league average.
- Include visualizations (e.g., bar charts ranking defenses) and a searchable/sortable table.
- Use DraftKings PPR scoring (based on existing app patterns, like "dk_ppr_points" in player stats).

This is a common fantasy football tool (e.g., "points allowed" rankings on sites like ESPN or FantasyPros), calculated by aggregating opponent player stats against each defense.

## Rationale and Research
Based on the available data and codebase:
- **Data Sources**: The `weekly` dataset (from `catalog/datasets.yml`) is ideal, as it includes per-player, per-week stats like `fantasy_points_ppr`, `position`, `team`, `opponent_team`, and `week`. This allows aggregating points scored by players of a position against the `opponent_team` (i.e., the defense).
  - Columns confirmed via search: `fantasy_points_ppr`, `position`, `team`, `opponent_team`, `week`, etc.
  - No direct "defense points allowed" table exists, so a new aggregation is needed.
- **Existing Patterns**: Current reports (e.g., `app/pages/1_Player_Weekly_Stats.py`) use helper functions from `lib.data` (e.g., `load_player_week_stats`) to query pre-computed "gold" tables. Utilization reports (e.g., WR, Rushing) similarly load aggregated metrics. We can mirror this for defenses.
- **Calculations Needed**:
  - For each defense (team), position, and game (week): Sum `fantasy_points_ppr` for all players of that position on the opposing team.
  - Average per game: Divide by number of games played by the defense.
  - League average: Compute overall average points allowed to the position across all defenses.
  - Handle filters: e.g., last N weeks, specific seasons.
- **Edge Cases**: Handle bye weeks (no game = no points allowed), injuries (zero points), and positions with multiple players (e.g., aggregate all WRs vs. a defense).
- **Performance**: Aggregations should be efficient; pre-compute into a gold table if queries are slow.

This feature aligns with existing app goals (e.g., player stats and utilization) and leverages the `weekly` importer, which is enabled for all years.

## Requirements
### Data Requirements
- **Input Datasets**:
  - `weekly`: Primary source for player stats and fantasy points.
  - `schedules`: To confirm games played, byes, and opponents (though `weekly` already has `opponent_team`).
- **New Gold Table**: Create `gold_defense_position_points_allowed` (partitioned by season).
  - Columns: `season`, `week`, `defense_team`, `position`, `points_allowed` (sum per game), `games_played`, `avg_points_allowed`, `league_avg_points_allowed`, etc.
  - Scoring: Use PPR (full-point per reception) consistent with app (e.g., "dk_ppr_points").

### Functional Requirements
- **Filters** (via sidebar, like existing pages):
  - Season (multi-select, default to current).
  - Weeks (multi-select, default to all or last 4).
  - Positions (multi-select: QB, RB, WR, TE; default all).
  - Defense search (text input for team abbreviations).
- **Display**:
  - Table: Defenses ranked by avg. points allowed (descending), with columns for team, position, avg points, vs. league avg (e.g., +2.5), games.
  - Chart: Bar chart of top/bottom defenses vs. league average.
  - Export: Option to download CSV.
- **UX**: Warning if no data (e.g., early season). Responsive and container-width like existing pages.

### Technical Requirements
- Python 3.10+, Streamlit, Altair (for charts), Polars/Pandas for data handling.
- Integrate with existing `lib.data` module for loading functions.

## Implementation Steps
1. **Data Transformation (Backend)**:
   - Add to `src/transforms/` (e.g., `defense_points_allowed.py`).
   - Query `weekly` dataset: Group by `opponent_team` (defense), `position`, `season`, `week`; sum `fantasy_points_ppr`.
   - Compute averages and league benchmarks.
   - Write to new gold table in `data/gold/`.
   - Update `catalog/datasets.yml` to include this table (importer: custom transform, partitions: ["season"]).

2. **Helper Functions (lib.data)**:
   - Add `list_defenses(season)` (similar to `list_teams`).
   - Add `load_defense_position_points(seasons, weeks, positions, defense_search)` to query the gold table with filters.

3. **Streamlit Page (Frontend)**:
   - Create `app/pages/2_Defense_vs_Position.py` (numbered after existing pages).
   - Mirror structure: Import from `lib.data`, add filters, load data, display dataframe and Altair chart.
   - Example chart: Bar for avg points, colored by position, sorted descending.

4. **Testing**:
   - Unit tests for transformations (e.g., in `tests/test_transforms.py`).
   - Manual: Run on sample seasons, verify vs. known fantasy sites.
   - Edge: Empty filters, no data seasons.

5. **Deployment**:
   - Update app requirements if needed.
   - Add to sidebar navigation (if not automatic).

## Estimated Effort
- Backend (transform + gold table): 4-6 hours.
- Frontend (Streamlit page): 2-4 hours.
- Testing: 2 hours.
- Total: 8-12 hours (assuming familiarity with codebase).

## Dependencies/Risks
- Ensure `weekly` has complete `fantasy_points_ppr` for all years (from search, available since 1999).
- If data is incomplete (e.g., pre-1999), limit report to supported seasons.
- Risk: Performance on large queries; mitigate by pre-computing gold table.

This feature will enhance the app's fantasy analysis capabilities. If approved, I can proceed with implementation or provide code sketches.
