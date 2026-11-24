# nfl_data

A local lake of nflverse files. `README.md` is the manual (commands, guarantees, where data lives) and lists the rebuild steps; read it before changing anything.

## Commands

- `uv sync`, then `uv run pytest`. No test touches the network; the suite takes about two seconds.
- `uv run ruff format` and `uv run ruff check` before every commit.
- `uv run python -m nfl_lake update --dry-run` shows what a run would fetch and rebuild without writing anything.

## Rules

- Keep the repo small. Add a file, dependency, option or helper only when something needs it now, and delete what nothing uses.
- Tests assert outcomes: files on disk, which files were fetched or rebuilt, counts, exit codes. They never assert message wording or log format, and they patch nothing except `urlopen` in `tests/test_source_github.py`.
- Check a change to ingest on real files as well: point `NFL_LAKE_ROOT` at a scratch folder and run `update --datasets injuries --seasons 2026`.
- Use the newest Python and package versions; hold one back only for a reason you have tested.
- When behaviour changes, update `README.md`, the header of `catalog/datasets.yml` and `docs/SOURCE_INVENTORY.md` in the same commit.
- Never commit data. `data/` and `*.parquet` are git-ignored.

## Layout

- `nfl_lake/`: `cli.py` (commands), `pipeline.py` (update, verify, status), `core.py` (Stage 4 core tables and core verification), `checks.py` (reusable Strategy 1 DuckDB checkbook runner and Stage 3/4 rules), `contract.py` (key rules), `source.py` (GitHub and local-folder sources), `store.py` (paths and manifests), `catalog.py` (catalog model).
- `catalog/datasets.yml`: one contract per dataset. `catalog/draftkings.yml`: scoring rules that no code reads yet.
- `docs/SOURCE_INVENTORY.md`: what each nflverse file holds, and the Stage 4 core tables.
- `docs/DATA_VERIFICATION.md`: measured Strategy 1 checkbook rules, team-code/join maps, DraftKings scoring deltas, and pinned real anchor games.
