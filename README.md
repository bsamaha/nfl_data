# Local NFL Data Lake

A local NFL lakehouse built from the public nflverse files: Parquet on disk, queried with DuckDB or polars, with a YAML catalog that holds one contract per dataset.

## Status

The repo is being rebuilt in place. The steps, in order:

1. Source inventory. Done: `docs/SOURCE_INVENTORY.md`.
2. Clean project. Done: pinned Python and dependencies, dead code and committed run output removed.
3. Rebuild ingest. Done: the `nfl_lake` package described below.
4. Core tables.
5. Features.
6. First projection.
7. Rebuild the app.

What works today is ingest: fetching the nflverse files and publishing a checked copy of each. Two leftovers from the previous team do not work yet. The report SQL in `queries/reports/` reads the old `data/silver/` layout and is kept as reference for step 4. The Streamlit pages import `app/lib/data.py`, which was never committed, so no page loads until step 7.

## Install and test

You need [uv](https://docs.astral.sh/uv/) and nothing else: no bash, no Make and no DuckDB command-line tool. The commands are the same in PowerShell, cmd and a Unix shell. Run them from the repo root.

```
uv sync
uv run pytest
```

`uv sync` creates `.venv` from `uv.lock`, and downloads the Python version named in `.python-version` if you do not have it. The expected test result is 39 passed and 1 skipped. The skipped module is `tests/test_defense_report.py`, which needs the missing `app/lib/data.py`. No test touches the network.

To add or upgrade a dependency, edit `pyproject.toml` and run `uv lock`, then commit both files.

## Ingest

```
uv run python -m nfl_lake update
```

The first run downloads every file in the catalog, about 620 MB across 185 files. Later runs compare the checksum GitHub publishes for each file with the copy on disk and download only what changed, so a nightly run in season fetches a handful of files.

| Command | What it does |
|---|---|
| `update` | Fetch what changed upstream, rebuild the clean layer from it, publish all or nothing |
| `update --dry-run` | List what would be fetched and rebuilt, and change nothing |
| `update --seasons 2015-2026` | Limit seasonal datasets to these seasons (`2026`, a range, or `2024,2026`) |
| `update --datasets pbp,schedules` | Limit the run to these datasets |
| `update --skip weekly_rosters` | Leave datasets out, so one that is broken upstream does not hold back the rest |
| `status` | What is published per dataset, and how the last run ended |
| `verify` | Re-check every published file against its key; `--deep` also re-hashes the raw files |
| `unlock` | Clear the lock left by an update that was killed |

Every command exits non-zero when something is wrong, and `--help` on any of them lists its options.

### What a run guarantees

- **Reruns change nothing.** A second run with no upstream change fetches nothing and rewrites nothing.
- **Corrections land.** A season file is replaced whole from the newest upstream copy. Nothing is merged row by row.
- **Failures stop the run.** If any file fails to download intact or breaks its contract, nothing is published, the clean layer stays as it was, the error is printed and recorded, and the exit code is 1.
- **Keys hold.** Every clean file has a unique, non-null key, as declared in the catalog.
- **Files do not shrink or lose columns silently.** A file that loses more than 2% of its rows, drops a column or changes a column's type is refused. `--accept-schema-change` publishes a layout change once downstream code is ready for it.
- **Types are kept.** Clean files have the same columns and types as the files nflverse publishes.

### Where the data lives

Everything is under `data/`, which git ignores. Set `LAKE_ROOT` to put it elsewhere.

| Folder | Contents |
|---|---|
| `data/raw/<dataset>/<file>/` | Files exactly as published, named by upstream update time and checksum |
| `data/clean/<dataset>/<file name>` | The newest version of each file with its contract applied. Query this. |
| `data/rejects/<dataset>/<file name>` | Rows set aside because their key was null, such as stat rows with no player |
| `data/manifest/raw_files.jsonl` | One line per raw file stored or removed: source URL, upstream time, checksum, rows |
| `data/manifest/runs.jsonl` | One line per run: status, errors, counts |
| `data/manifest/clean_state.json` | Which raw file and contract each clean file was built from |

Datasets whose history matters keep every raw version (schedules, players, weekly rosters, injuries). The rest keep only the newest.

### Querying

DuckDB runs in-process and reads the clean files directly:

```python
import duckdb

print(duckdb.sql("""
    SELECT s.season, count(*) AS player_games, count(DISTINCT s.game_id) AS games
    FROM read_parquet('data/clean/stats_player_week/*.parquet') s
    JOIN read_parquet('data/clean/schedules/games.parquet') g USING (game_id)
    GROUP BY 1 ORDER BY 1
""").fetchall())
```

Save it as a file and run it with `uv run python your_file.py`. Join games on `game_id`, not on team codes; `docs/SOURCE_INVENTORY.md` explains why.

### The catalog

`catalog/datasets.yml` defines each dataset: the nflverse release and file name, the first season to ingest, the key, how much raw history to keep, and any rule for rows that would break the key. The header of the file documents every setting, and an unknown setting is an error. To add a dataset, add an entry and run `update --datasets <name>`; the run fails with the reason if the key does not hold.

### Settings

| Variable | Effect |
|---|---|
| `LAKE_ROOT` | Folder for the lake. Default: `data` |
| `GITHUB_TOKEN` | Optional. A run makes one GitHub API call, and the anonymous limit is 60 an hour |
| `NFL_LAKE_SOURCE_DIR` | Read files from a local folder of `<release>/<file>` instead of GitHub |
| `LOG_LEVEL` | Progress detail on stderr. Default: `INFO` |

A `.env` file in the repo root is read if present.

## Scheduling

One daily job is enough, because a run only downloads what changed. nflverse rebuilds most files once a day in season, and stat corrections arrive midweek.

Windows Task Scheduler: create a task whose action has Program `uv` and Arguments `run --directory C:\path\to\nfl_data python -m nfl_lake update`.

cron:

```
30 9 * * * uv run --directory /path/to/nfl_data python -m nfl_lake update >> /path/to/nfl_data/logs/update.log 2>&1
```

`uv run --directory` sets the working directory, which the command needs because it reads `catalog/` and writes `data/` relative to it. A failed run exits 1, so point the scheduler's failure alert at the exit code.

## What's inside

- Ingest package: `nfl_lake/`
- Dataset catalog: `catalog/datasets.yml`
- Source contracts and how they were verified: `docs/SOURCE_INVENTORY.md`, with scripts in `scripts/source_inventory/`
- DraftKings rule files: `catalog/draftkings/`
- Best Ball notes carried over from the previous team: `docs/DK_BESTBALL_NOTES.md`
- The previous pipeline, for reference only: `docs/OLD_PIPELINE_OVERVIEW.md`
- Report SQL from the previous team, not yet rebuilt: `queries/reports/`
