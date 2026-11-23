# Local NFL Data Lake

A local lake of the public nflverse files: Parquet on disk, queried with DuckDB or polars, with a YAML catalog that holds one contract per dataset.

## Status

The rebuild steps, in order:

1. Source inventory (done): `docs/SOURCE_INVENTORY.md`, produced by `scripts/source_inventory/`.
2. Clean project (done).
3. Ingest (done): the `nfl_lake` package described below.
4. Core tables (proposed in `docs/SOURCE_INVENTORY.md`).
5. Features.
6. First projection.
7. Rebuild the app.

What works today is ingest: fetching the nflverse files and publishing a checked copy of each. `catalog/draftkings.yml` holds DraftKings scoring and lineup rules that no code reads yet.

## Install and test

You need [uv](https://docs.astral.sh/uv/). Run the commands from the repo root; they are the same in PowerShell, cmd and a Unix shell.

```
uv sync
uv run pytest
```

`uv sync` creates `.venv` from `uv.lock`, and downloads the Python version named in `.python-version` if you do not have it. No test touches the network.

Before a commit, `uv run ruff format` lays the code out and `uv run ruff check` looks for mistakes.

## Ingest

```
uv run python -m nfl_lake update
```

The first run downloads every file in the catalog, about 600 MB as of November 2026; `update --dry-run` prints the current file count and size. Later runs compare the checksum GitHub publishes for each file with the copy on disk and download only what changed.

| Command | What it does |
|---|---|
| `update` | Fetch what changed upstream, rebuild the clean layer from it, publish only if every file passed |
| `update --dry-run` | Show what would be fetched and how many files would be rebuilt, and change nothing |
| `update --seasons 2015-2026` | Limit seasonal datasets to these seasons (`2026`, a range, or `2024,2026`) |
| `update --datasets pbp,schedules` | Limit the run to these datasets |
| `update --skip weekly_rosters` | Leave datasets out, so one that is broken upstream does not hold back the rest |
| `status` | What is published per dataset, and how the last run ended |
| `verify` | Re-check every published file against its key; `--deep` also re-hashes the raw files |
| `unlock` | Clear the lock left by an update that was killed |

`update` and `verify` exit non-zero when they find a problem. `status` only reports: it exits 0 even when the last run failed. `--help` on any command lists its options.

### What a run guarantees

- **Reruns change no data.** A second run with no upstream change fetches nothing and rewrites no data file; it adds one line to the run log.
- **Corrections land.** A season file is replaced whole from the newest upstream copy. Nothing is merged row by row.
- **Failures stop the run.** If any file fails to download intact or breaks its contract, nothing is published, the clean layer stays as it was, and the error is printed and recorded. Publishing goes file by file: a file's set-aside rows are moved or removed first, then its clean file is renamed. If it is interrupted, for example by a clean file that is open in another program, the error says how many files were published and the next run finishes the rest.
- **Keys hold.** Every clean file has a unique, non-null key, as declared in the catalog.
- **Files do not shrink or lose columns silently.** A file that loses more than 2% of its rows, drops a column or changes a column's type is refused. `--accept-schema-change` publishes a file whose columns were dropped or retyped, once downstream code is ready for it. It does not waive the row check: to accept a real drop in rows, raise that dataset's `max_row_drop` in the catalog.
- **Types are kept.** Clean files have the same columns and types as the files nflverse publishes.

### Where the data lives

Everything is under `data/`, which git ignores. Set `NFL_LAKE_ROOT` to put it elsewhere.

| Folder | Contents |
|---|---|
| `data/raw/<dataset>/<file stem>/` | Files exactly as published, named by upstream update time and checksum |
| `data/clean/<dataset>/<file name>` | The newest version of each file with its contract applied. Query this. |
| `data/rejects/<dataset>/<file name>` | Rows set aside because their key was null, such as stat rows with no player |
| `data/manifest/raw_files.jsonl` | One line per raw file stored or pruned: source URL, upstream time, checksum, rows |
| `data/manifest/runs.jsonl` | One line per run: status, errors, counts |
| `data/manifest/clean_state.json` | Which raw file and contract each clean file was built from |

Datasets marked `history: all` in `catalog/datasets.yml` keep every raw version; the rest keep only the newest.

### Querying

DuckDB runs in-process and reads the clean files directly:

```python
import duckdb

print(duckdb.sql("""
    SELECT s.season, g.game_type, count(*) AS player_games
    FROM read_parquet('data/clean/stats_player_week/*.parquet') s
    JOIN read_parquet('data/clean/schedules/games.parquet') g USING (game_id)
    GROUP BY 1, 2 ORDER BY 1, 2
""").fetchall())
```

Join games on `game_id`, not on team codes; `docs/SOURCE_INVENTORY.md` explains why.

### The catalog

`catalog/datasets.yml` defines each dataset: the nflverse release and file name, the first season to ingest, the key, how much raw history to keep, and any rule for rows that would break the key. The header of the file documents every per-dataset setting, and an unknown setting is an error. To add a dataset, add an entry and run `update --datasets <name>`; the run fails with the reason if the key does not hold.

### Settings

| Variable | Effect |
|---|---|
| `NFL_LAKE_ROOT` | Folder for the lake. Default: `data` |
| `GITHUB_TOKEN` | Optional. A run makes one GitHub API call, and the anonymous limit is 60 an hour |
| `NFL_LAKE_SOURCE_DIR` | Read files from a local folder of `<release>/<file>` instead of GitHub |
| `LOG_LEVEL` | Progress detail on stderr. Default: `INFO` |

A `.env` file in the repo root is read if present.

## Scheduling

Run it once a day; a run downloads only what changed. When upstream files land is not established yet (see Open questions in `docs/SOURCE_INVENTORY.md`), so the time in the cron line is an example, not a recommendation.

Windows Task Scheduler: create a task whose action has Program `uv` and Arguments `run --directory C:\path\to\nfl_data python -m nfl_lake update`.

cron:

```
30 9 * * * uv run --directory /path/to/nfl_data python -m nfl_lake update >> "$HOME/nfl_lake_update.log" 2>&1
```

`uv run --directory` sets the working directory, which the command needs because it reads `catalog/` and writes `data/` relative to it. A failed run exits 1, so point the scheduler's failure alert at the exit code.

## Data licence

The nflverse-data files are published under CC BY 4.0, so anything published from this lake must credit nflverse. Two datasets come from FTN under different terms: `ftn_charting`, and `pbp_participation` from the 2023 season on, are CC BY-SA 4.0 and must be credited to "FTN Data via nflverse". Checked on 21 Nov 2026 against the licence file in `nflverse/nflverse-data` and the nflreadr documentation for the two FTN datasets.
