# Source inventory scripts

These scripts produced the facts in `docs/SOURCE_INVENTORY.md`. Re-run them at the start of a season, or whenever an ingest check fails, to see whether nflverse changed a file layout, a key or a team code.

They read the public `nflverse/nflverse-data` release files over HTTP. They fetch file footers and a few key columns, never whole datasets, and they do not touch the lake.

## Requirements

The project environment (`uv sync`), which already has `pyarrow` and `polars`. No DuckDB, no bash, no GitHub token.

## Run order

Run from the repo root. `data/_inventory` is a good output folder because `data/` is git-ignored.

```
uv run python scripts/source_inventory/list_releases.py data/_inventory
uv run python scripts/source_inventory/probe_schemas.py data/_inventory
uv run python scripts/source_inventory/analyze_schemas.py data/_inventory
uv run python scripts/source_inventory/probe_keys_all_seasons.py data/_inventory
uv run python scripts/source_inventory/probe_values.py data/_inventory
```

| Script | What it does | Network cost |
|---|---|---|
| `list_releases.py` | Lists every release and file with size and last-update time. Writes `releases.json`. | 1 GitHub API call |
| `probe_schemas.py` | Reads the Parquet footer of every season file. Writes `schemas.json` with columns, types and row counts. | 25 MB across 335 files |
| `analyze_schemas.py` | Prints, per dataset, the seasons where columns were added, removed or changed type. Pass a comma-separated list of labels to limit the output. | None |
| `probe_keys_all_seasons.py` | Tests each dataset's key in every season and matches game ids against the schedule. | 141 MB |
| `probe_values.py` | Checks value domains, ID coverage against `players`, and team codes, on the current, previous and two older seasons. | 40 MB, 1 GitHub API call |

Costs were measured on 21 Nov 2026. A full run transfers about 205 MB and takes a few minutes. Each network script prints its own total at the end.

`nfl_remote.py` is the shared helper that reads footers and columns by HTTP range request.

## Reading the output

- In `probe_keys_all_seasons.py`, a dataset is healthy when `dup rows`, `null key cells` and `game ids not in schedules` all say `none`. Known exceptions are listed in the inventory: null-player rows in player stats, duplicates in weekly rosters before 2016, and the week-based depth charts.
- `schedule games absent here` is expected for the season in progress (unplayed games) and for three old games in 1999 and 2000.
- In `analyze_schemas.py`, a line starting with a season means the layout changed in that season. No such lines means every season has the same columns.

## Limits

- GitHub allows 60 unauthenticated API calls an hour per address. The scripts use two.
- `probe_values.py` takes the current season from the newest `stats_player_week` file. The older comparison seasons (2015 and 2019) and the last week-based depth chart season (2024) are constants at the top of the file.
- The scripts report what the files contain. They do not check stat definitions against play-by-play.
