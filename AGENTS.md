# nfl_data: rules for coding agents

A local lake of nflverse files. `README.md` is the manual (commands, guarantees, where data lives) and lists the rebuild steps; read it before changing anything. Claude Code reads this file through `CLAUDE.md`, and Antigravity (Gemini) loads it directly.

## Before you change anything

- Two agents work on this repo. Run `git status -sb` first. If the tree holds changes you did not make, or the branch is not the one you expect, stop and tell the user. Do not switch branches, stash or reset in a checkout that holds another agent's work.
- Each agent works only in its own folder:
  - Gemini works in the worktree `nfl_data-gemini`, next to the main checkout.
  - Claude sessions work in the worktrees the app makes under `.claude/worktrees/`.
  - The main checkout `nfl_data` stays on `master` and is where merges happen. Do not edit files there unless the user asks you to merge.
  - A worktree needs its own `uv sync`. Its `data/` is empty, so set `NFL_LAKE_ROOT` to the main checkout's `data` folder to read the real lake.
- All folders share one git history, so a commit made in one is visible at once in the others. Your folder's files do not change when `master` does.
- `master` is checked out in the main checkout, so `git switch master` fails in a worktree. Do not work around that by moving to another folder.
- Start each task on a new branch from the latest `master`: `git switch -c gemini/<task> master` (or `claude/<task>`). To continue a task after `master` moved, run `git merge master` first.
- When asked to review recent commits, review `master` (`git log master`) or the branch the user names, not your own folder's branch.
- The commit hook refuses commits on `master`. Merging into `master`, pushing and deleting a branch each need the user's go-ahead.
- Commit your own work before you stop or hand off. The body says what changed and why, then a `Checked:` line names what you ran. End with a `Co-Authored-By:` trailer that names you and your model, so a reader can tell which agent wrote what.
- To commit another agent's uncommitted work, commit it unchanged first and say whose it is, then put your changes in a second commit.

## Commands

- `uv sync`, then `uv run pytest`. The suite takes well under a minute, and a test that opens a network connection fails.
- `git config core.hooksPath .githooks`, once per clone; worktrees share it. Before each commit the hook runs `ruff format --check`, `ruff check` and the tests. It stops a commit made on `master`, or one made while files on disk differ from what is staged. `uv run ruff format` lays the code out.
- `uv run python -m nfl_lake update --dry-run` shows what a run would fetch and rebuild, and writes nothing.
- `uv run python -m nfl_lake build-core --dry-run` builds and checks the core tables from the real clean layer and prints what would change against the published ones. It is safe for a reviewer: it publishes nothing, takes the lake lock while it runs, and builds in `data/_staging/`, which it empties before and after.
- `uv run python -m nfl_lake status` says whether the published core was built from the current clean files and the current `nfl_lake` code.

## Windows and sandboxes

- The shell is PowerShell. Windows PowerShell 5.1 has no `&&`, so run commands one at a time.
- Quoting breaks in `python -c`. Put multi-line Python or SQL in a script file, and write a commit message to a file for `git commit -F <file>`.
- A script outside the repo imports `nfl_lake` only with `PYTHONPATH` set to the repo root, because the project is not an installed package.
- In the Antigravity sandbox `uv run` may fail on the uv cache ("Access is denied" on `uv\.lock`). Use the same environment directly: `.venv\Scripts\python.exe -m pytest`, `-m nfl_lake`, `-m ruff`. Git writes and network access need the sandbox bypass.
- A DuckDB 2 relation (`connection.sql(...)`) has `fetchall()` but no `fetchone()`; `connection.execute(sql).fetchone()` works. `pyarrow` is not installed; read Parquet with polars or DuckDB.

## Rules

- Keep the repo small. Add a file, dependency, option or helper only when something needs it now, and delete what nothing uses.
- Names say what a thing is: no abbreviations a reader has to guess. Words invented in a chat ("Strategy 1", "Option A", "Tier 2", "Stage 4", "checkbook") do not go in code or docs, because the reader has not seen the chat. Name what the thing does. `tests/test_rules_for_docs.py` fails on these words and on local file paths.
- Measure before you claim. A statement about upstream or GitHub behaviour, about atomicity, or about a speed-up goes in only with a measurement on real files. Label an inference as one.
- Leave out numbers that go stale by themselves, such as test counts, run times and download sizes. A measured number about the data says what it was measured on.
- Tests assert outcomes: files on disk, which files were fetched or rebuilt, counts, exit codes. `tests/test_rules_for_tests.py` fails a test that pins message wording, reads log lines, patches or mocks anything, or reaches for a private name, and says what to do instead. When you correct a test for a new reason, add the reason there.
- Add a test for a promise to a user or a scheduler, or for a bug that happened. Do not add one per guard, per surviving mutation, or for an input nothing produces.
- A test that needs a collaborator to misbehave passes one in: `GitHubReleasesSource` takes the function that performs its HTTP requests, and `tests/test_source_github.py` passes one that answers from memory.
- Test a promise through the command or function a user calls. For a part that only computes (key rules, option parsing, file matching), write a Hypothesis property in `tests/test_properties.py`.
- A new column or a new source of rows gets a check, in `nfl_lake/checks.py`, on what its values mean on the real lake, not only that they are filled in. The worst bugs so far passed every fill, key and diff check: Super Bowl players listed under the other team, and early injury times in Pacific time labelled UTC. Checks that caught them: teams agree within a game, report times fall in the afternoon.
- Check a change to ingest on real files: point `NFL_LAKE_ROOT` at a scratch folder and run `update --datasets injuries --seasons 2026`. Check a change to the core tables with `build-core --dry-run` and read its change report. A row or column that changed without a reason you can name is a bug.
- Use the newest Python and package versions; hold one back only for a reason you have tested.
- When behaviour changes, update `README.md`, the header of `catalog/datasets.yml` and `docs/SOURCE_INVENTORY.md` in the same commit.
- Never commit data. `data/` and `*.parquet` are git-ignored.
- When you explain something to the user, use plain words and say what a technical term means the first time you use it.

## Already decided

Do not raise these again in a review:

- Python `>=3.15` and the newest package majors are on purpose. Typer, structlog, python-dotenv and pyarrow were removed on purpose.
- `data/clean/` keeps each upstream file as published, so columns that differ across seasons are expected there; `data/core/` is where they are unified.
- The lake is local and single-user: manifests and `core_state.json` are trusted, not checked for paths outside `data/`.
- `tests/test_rules_for_tests.py` is a blunt check on purpose; change it when it actually misfires.
- No tests for inputs nothing produces (Unicode digits, `+2024` as a season).
- The data licence is documented in `README.md`, and it matters only if the data is redistributed.
- For when a later step needs them: reader-safe publishing on Windows, lock liveness, HTTP 429 handling, `--if-stale`, run records for `build-core`, a DuckDB catalog and versioned snapshots for consumers.

## Reviewing

- Review the diff between the branch and the commit it started from. List problems older than the diff separately.
- Check each finding against the rules and the list above before you report it.
- Give each finding a command or query that shows it, with its output, and the fix you would make. When the fix touches data, try it on the real lake.
- Say whether wrong data is published or only what the current code would publish, and mark inferences. Critical means wrong data that is published, or that the next `update` or `build-core` would publish.
- After the fixes are in, check the change on real data before you start another round of review.

## Layout

- `nfl_lake/`:
  - `cli.py`: the commands.
  - `pipeline.py`: update, verify, status.
  - `core.py`: builds and verifies the core tables.
  - `checks.py`: the DuckDB check runner and the clean-layer and core checks.
  - `changes.py`: what a core build changes against the published tables.
  - `contract.py`: key rules.
  - `source.py`: the GitHub and local-folder sources.
  - `store.py`: paths and manifests.
  - `catalog.py`: the catalog model.
- `catalog/datasets.yml`: one contract per dataset. `catalog/draftkings.yml`: scoring rules that no code reads yet.
- `docs/SOURCE_INVENTORY.md`: what each nflverse file holds, and the core tables.
- `docs/DATA_VERIFICATION.md`: the measured numbers behind the core checks, team-code and join maps, DraftKings scoring differences, and pinned real games.
- `.githooks/pre-commit`: the checks a commit must pass.
