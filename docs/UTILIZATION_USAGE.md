## Utilization Report: Queries and Materialization

Run parameterized DuckDB queries via `scripts/run_query.sh`. Defaults are season=2025, season_type='REG' inside each SQL. Override with flags.

Examples:

```bash
# Team denominators (dropbacks, attempts, carries, snaps)
scripts/run_query.sh -f queries/utilization/team_week_context.sql -s 2025 -t REG -- -csv

# Team PROE and neutral pace
scripts/run_query.sh -f queries/utilization/team_proe_and_pace_by_week.sql -s 2025 -t REG -- -csv

# Player-week receiving target events
scripts/run_query.sh -f queries/utilization/receiving_events_by_player_week.sql -s 2025 -t REG -- -csv

# Player-week rushing carry events
scripts/run_query.sh -f queries/utilization/rushing_events_by_player_week.sql -s 2025 -t REG -- -csv

# Player-week utilization mart (wide)
scripts/run_query.sh -f queries/utilization/player_week_utilization.sql -s 2025 -t REG -- -csv

# Materialize gold: team context
scripts/run_query.sh -f queries/utilization/materialize_team_week_context.sql -s 2025 -t REG

# Materialize gold: player-week utilization
scripts/run_query.sh -f queries/utilization/materialize_player_week.sql -s 2025 -t REG
```

Notes:

- `queries/utilization/` contains modular SQL you can run ad hoc or embed in jobs.
- If `ngs_weekly` is unavailable, `routes_run`-derived fields in `player_week_utilization.sql` will be NULL.
- Default season detection can be wired into the runner later; currently pass `-s` explicitly.


