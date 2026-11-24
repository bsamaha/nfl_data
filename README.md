# Local NFL Data Lake

A local lake of the public nflverse files: Parquet on disk, queried with DuckDB or polars, with a YAML catalog that holds one contract per dataset.

## Status

The rebuild steps, in order:

1. Source inventory (done): `docs/SOURCE_INVENTORY.md`.
2. Clean project (done).
3. Ingest (done): `python -m nfl_lake update` publishes a checked copy of each upstream file into `data/clean/`.
4. Core tables (done): `python -m nfl_lake build-core` builds the 7 normalized core tables into `data/core/` behind Strategy 1 publish-time checks (`docs/DATA_VERIFICATION.md`).
5. Features.
6. First projection.
7. Rebuild the app.

What works today is ingest (`data/clean/`) and core tables (`data/core/`). `catalog/draftkings.yml` holds DraftKings scoring and lineup rules that no code reads yet.

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

The first run downloads every file in the catalog, about 600 MB as of November 2026; `update --dry-run` prints the current file count and size. Later runs compare the checksum GitHub publishes for each file with the one recorded when it was last published, and download only what changed.

| Command | What it does |
|---|---|
| `update` | Fetch what changed upstream, rebuild the clean layer from it, and publish each passing dataset |
| `update --dry-run` | Show what would be fetched and how many files would be rebuilt, and change nothing |
| `update --seasons 2015-2026` | Limit seasonal datasets to these seasons (`2026`, a range, or `2024,2026`) |
| `update --datasets pbp,schedules` | Limit the run to these datasets |
| `update --skip weekly_rosters` | Leave datasets out, e.g. to skip one that is broken upstream for a clean exit 0 |
| `build-core` | Build the 7 normalized core tables into `data/core/`, gated by Strategy 1 publish-time checks |
| `status` | What is published per clean dataset and core table, and how the last run ended |
| `verify` | Re-check every published clean file and (when built) core table against keys and Strategy 1 checks; `--deep` also re-hashes every clean, set-aside, raw snapshot, and core file |
| `unlock` | Clear the lock left by an update or core build that was killed |

`update`, `build-core` and `verify` exit non-zero when they find a problem, and `update`/`verify` also exit non-zero when they find nothing to work on: a selection that matches no upstream file, or a lake with nothing published. `status` only reports: it exits 0 even when the last run failed. `--help` on any command lists its options.

When `verify` reports a damaged clean or set-aside file, delete that file and run `update` to rebuild it; `update` alone leaves a published file as it is while its upstream copy has not changed. A damaged raw snapshot can only be deleted. If it was the newest, `update` fetches the current upstream file again. When `verify` reports a damaged core file, run `build-core` to rebuild the core layer.

### What a run guarantees

- **Reruns change no data.** A second run with no upstream change fetches nothing and rewrites no data file; it adds one line to the run log. When upstream republishes a file with the same rows, as it often does with only a new build time, the file is fetched but its clean and raw files are left as they are, and only its record is refreshed.
- **Corrections land.** A season file is replaced whole from the newest upstream copy. Nothing is merged row by row.
- **A failure stays in its dataset.** A dataset is published only if every one of its files downloaded intact and passed its contract; otherwise its clean layer stays as it was. The other datasets in the run still publish, and the run exits 1 and records the errors. Publishing goes file by file: a file's set-aside rows are moved or removed first, then its clean file is renamed. If it is interrupted, for example by a clean file that is open in another program, the error says how many files were published and the next run finishes the rest.
- **Keys hold.** Every clean file has a unique, non-null key, as declared in the catalog.
- **Files do not shrink or lose columns silently.** A file that loses more than 2% of its rows, drops a column or changes a column's type is refused. `--accept-schema-change` publishes a file whose columns were dropped or retyped, once downstream code is ready for it. It does not waive the row check: to accept a real drop in rows, raise that dataset's `max_row_drop` in the catalog.
- **Types are kept.** Clean files have the same columns and types as the files nflverse publishes.

### Where the data lives

Everything is under `data/`, which git ignores. Set `NFL_LAKE_ROOT` to put it elsewhere.

| Folder | Contents |
|---|---|
| `data/raw/<dataset>/<file stem>/` | Raw snapshots for `history: all` datasets, named by upstream update time and checksum |
| `data/clean/<dataset>/<file name>` | The newest version of each upstream file with its key contract applied |
| `data/rejects/<dataset>/<file name>` | Rows set aside because their key was null, such as stat rows with no player |
| `data/core/` | Normalized, joined core tables (`game`, `team_game`, `player`, `player_game`, `play/play_<season>`, `player_week_status`, `depth_chart_snapshot`). Query this for modeling. |
| `data/manifest/clean_state.json` | Which upstream file, checksums and contract rules each clean file was built from |
| `data/manifest/core_state.json` | Row counts and SHA-256 checksums for every published core table |
| `data/manifest/listing_cache.json` | The last GitHub release listing, so that an unchanged one is not downloaded again |
| `data/manifest/runs.jsonl` | One line per update run: status, errors, counts |

Datasets marked `history: all` in `catalog/datasets.yml` keep a raw snapshot of every upstream version whose rows differ from the one before; the rest keep only the clean file. A download is kept only when its dataset publishes, so a dataset that fails fetches its files again next time, and a changed rule on a dataset with no raw snapshot fetches its files again to rebuild them.

### Core tables

```
uv run python -m nfl_lake build-core
```

`build-core` builds all 7 normalized tables into `data/_staging/` first, runs the Strategy 1 DuckDB checkbook suite (primary keys, zero join fan-out, 32 current team codes, UTC kickoffs, cross-source boxscore and play-by-play identities for 2016+, stat bounds, and pinned real NFL anchor games), and publishes into `data/core/` only if zero hard (`error`) checks fail. Soft (`warn`) checks (such as in-season FTN/PFR charting lag) are printed without blocking the build. If any `error` check fails, nothing is published and the previous `data/core/` layer is untouched.

### Querying

DuckDB runs in-process and reads both `data/clean/` and `data/core/` directly:

```python
import duckdb

print(
    duckdb.sql("""
    SELECT season, game_type, count(*) AS player_games
    FROM read_parquet('data/core/player_game.parquet')
    GROUP BY 1, 2 ORDER BY 1, 2
""").fetchall()
)
```

In `data/core/`, every team column uses the 32 current NFL team codes (`LV`, `LAC`, `LA`), `game.kickoff_utc` is UTC, `team_game.favored_by` is positive when that team is favored, and `player_game` already includes snaps, Next Gen Stats (with Super Bowl week offsets fixed), and PFR advanced stats. If querying `data/clean/` directly, join games on `game_id`, not on team codes; `docs/SOURCE_INVENTORY.md` explains why.

### The catalog

`catalog/datasets.yml` defines each dataset: the nflverse release and file name, the first season to ingest, the key, how much raw history to keep, and any rule for rows that would break the key. The header of the file documents every per-dataset setting, and an unknown setting is an error. To add a dataset, add an entry and run `update --datasets <name>`; the run fails with the reason if the key does not hold.

### Settings

| Variable | Effect |
|---|---|
| `NFL_LAKE_ROOT` | Folder for the lake. Default: `data` |
| `GITHUB_TOKEN` | Optional. A run makes one GitHub API call, and a few more when a download does not match the listing; without a token the limit is 60 an hour |
| `NFL_LAKE_SOURCE_DIR` | Read files from a local folder of `<release>/<file>` instead of GitHub |
| `LOG_LEVEL` | Progress detail on stderr. Default: `INFO` |

To keep these in a file, let uv read it: `uv run --env-file .env python -m nfl_lake update`.

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
