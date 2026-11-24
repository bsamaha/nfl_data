# nfl_data

A local lake of nflverse files. `README.md` is the manual (commands, guarantees, where data lives) and lists the rebuild steps; read it before changing anything.

## Commands

- `uv sync`, then `uv run pytest`. No test touches the network; the suite takes about ten seconds.
- `git config core.hooksPath .githooks`, once per clone. The commit hook then runs `ruff format --check`, `ruff check` and the tests, and stops a commit that fails one. `uv run ruff format` lays the code out.
- `uv run python -m nfl_lake update --dry-run` shows what a run would fetch and rebuild without writing anything.
- `uv run python -m nfl_lake build-core --dry-run` builds and checks the core tables from the real clean layer, prints what would change against the published ones, and publishes nothing.

## Rules

- Keep the repo small. Add a file, dependency, option or helper only when something needs it now, and delete what nothing uses.
- Tests assert outcomes: files on disk, which files were fetched or rebuilt, counts, exit codes. `tests/test_rules_for_tests.py` fails a test that pins message wording, reads log lines, patches or mocks anything, or reaches for a private name, and says what to do instead. When you correct a test for a new reason, add the reason there, not here.
- A test that needs a collaborator to misbehave passes one in: the GitHub source takes the function that performs its HTTP requests as an argument, and `tests/test_source_github.py` passes one that answers from memory.
- Test a promise through the command or function a user calls. For a part that only computes (key rules, option parsing, file matching), write a Hypothesis property in `tests/test_properties.py` instead of a list of examples.
- Check a change to ingest on real files as well: point `NFL_LAKE_ROOT` at a scratch folder and run `update --datasets injuries --seasons 2026`.
- Check a change to the core tables on the real lake as well: run `build-core --dry-run` and read its change report. A row or column that changed without a reason you can name is a bug.
- Use the newest Python and package versions; hold one back only for a reason you have tested.
- When behaviour changes, update `README.md`, the header of `catalog/datasets.yml` and `docs/SOURCE_INVENTORY.md` in the same commit.
- Never commit data. `data/` and `*.parquet` are git-ignored.

## Layout

- `nfl_lake/`: `cli.py` (commands), `pipeline.py` (update, verify, status), `core.py` (Stage 4 core tables and core verification), `checks.py` (reusable Strategy 1 DuckDB checkbook runner and Stage 3/4 rules), `changes.py` (what a core build changes against the published tables), `contract.py` (key rules), `source.py` (GitHub and local-folder sources), `store.py` (paths and manifests), `catalog.py` (catalog model).
- `catalog/datasets.yml`: one contract per dataset. `catalog/draftkings.yml`: scoring rules that no code reads yet.
- `docs/SOURCE_INVENTORY.md`: what each nflverse file holds, and the Stage 4 core tables.
- `docs/DATA_VERIFICATION.md`: measured Strategy 1 checkbook rules, team-code/join maps, DraftKings scoring deltas, and pinned real anchor games.
- `.githooks/pre-commit`: the checks a commit must pass.
