# Source inventory scripts

These scripts produced the facts in `docs/SOURCE_INVENTORY.md`. Re-run them at the start of a season, or when `update` reports a missing release or file, to see what ingest does not check: renamed releases and files, columns that differ between seasons, team codes and ID coverage.

They read the public `nflverse/nflverse-data` release files over HTTP range requests: footers for schemas, selected columns for the per-season key tests, and some files whole (the schedule, players, and the sampled seasons of rosters, snap counts, injuries, depth charts and PFR rec). They write only `releases.json` and `schemas.json` to the output folder and never read or change the lake's files.

## Run order

Run from the repo root. Any folder works as OUT_DIR; `data/_inventory` is git-ignored and the ingest never reads it.

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
| `probe_schemas.py` | Reads the Parquet footer of every file listed in `SETS`. Writes `schemas.json` with columns, types and row counts. | about 25 MB |
| `analyze_schemas.py` | Prints, per dataset, the seasons where columns were added, removed or changed type. Pass a comma-separated list of labels to limit the output. | None |
| `probe_keys_all_seasons.py` | Tests the key of each dataset listed in `CHECKS` in every season, matches game ids against the schedule where the dataset has them, and prints the seasons where relocation team codes, postseason week numbers and the participation personnel format change. | 142 MB |
| `probe_values.py` | Checks value domains, ID coverage against `players`, and team codes, on the current and previous seasons, 2015 and 2019, plus 1999 and 2024 for some datasets. | 39 MB, 1 GitHub API call |

Costs were measured on 21 Nov 2026. A full run transfers about 205 MB and takes about a minute. The three probe scripts each print a `bytes fetched` total.

`nfl_remote.py` is the shared helper that reads footers and columns by HTTP range request.

## Reading the output

- In `probe_keys_all_seasons.py`, a dataset is healthy when `null-key rows`, `dup rows` and `game ids not in schedules` all say `none` and its section has no `ERROR` line. `null-key rows` counts rows with a null or empty-string key column, and `dup rows` counts the rows that repeat a key among the other rows. Known exceptions are listed per dataset in `docs/SOURCE_INVENTORY.md`.
- `schedule games absent here` is expected for the season in progress (unplayed games); the other known cases are listed per dataset in `docs/SOURCE_INVENTORY.md`.
- In `analyze_schemas.py`, a line starting with a season means the layout changed in that season. No such lines means every season has the same columns.
- A probe exits non-zero when a file could not be read (`ERROR`), or when a release or file pattern it expects matched nothing (`NO FILES`).

## Limits

- GitHub allows 60 unauthenticated API calls an hour per address. The scripts use two.
