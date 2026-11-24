"""Where things live on disk, and the manifest that records them.

<root>/raw/<dataset>/<file stem>/<upstream time>_<sha12>.parquet   raw snapshots, kept only for `history: all` datasets
<root>/clean/<dataset>/<file name>                                 latest version, contract applied
<root>/rejects/<dataset>/<file name>                               rows set aside for a null key
<root>/manifest/clean_state.json                                   what each clean file was built from
<root>/manifest/listing_cache.json                                 cached GitHub release pages with ETags
<root>/manifest/runs.jsonl                                         one line per update run
<root>/_staging/<run_id>/                                          files being fetched and built; removed after the run
<root>/.update.lock                                                present while an update runs
"""

from __future__ import annotations

import contextlib
import json
import os
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

# What every clean_state.json entry holds, whichever version of the code wrote it.
_CLEAN_STATE_FIELDS = frozenset(
    {
        "dataset",
        "asset",
        "season",
        "rows",
        "rejected_rows",
        "deduped_rows",
        "columns",
        "contract",
        "raw_sha256",
        "upstream_updated_at",
        "built_at",
    }
)


class LakeLocked(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_utc_iso(timestamp: datetime) -> str:
    return timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class LakeStore:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)
        self.raw_dir = self.root_dir / "raw"
        self.clean_dir = self.root_dir / "clean"
        self.rejects_dir = self.root_dir / "rejects"
        self.manifest_dir = self.root_dir / "manifest"
        self.staging_dir = self.root_dir / "_staging"
        self.lock_path = self.root_dir / ".update.lock"

    # paths ---------------------------------------------------------------------------------
    def raw_file_path(self, dataset_name: str, asset_filename: str, updated_at: str, sha256: str) -> Path:
        asset_path = Path(asset_filename)
        compact_timestamp = re.sub(r"[^0-9TZ]", "", updated_at)  # 2026-11-21T16:46:06Z -> 20261121T164606Z
        return self.raw_dir / dataset_name / asset_path.stem / f"{compact_timestamp}_{sha256[:12]}{asset_path.suffix}"

    def clean_file_path(self, dataset_name: str, asset_filename: str) -> Path:
        return self.clean_dir / dataset_name / asset_filename

    def rejects_file_path(self, dataset_name: str, asset_filename: str) -> Path:
        return self.rejects_dir / dataset_name / asset_filename

    def relative_posix_path(self, path: Path) -> str:
        return path.relative_to(self.root_dir).as_posix()

    # manifest ------------------------------------------------------------------------------
    # A write that was killed can leave a last line of runs.jsonl with no end. The next record starts a new line so
    # that it is not joined to it, and a line that cannot be read is passed over when loading.
    def append_run_record(self, record: dict) -> None:
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_dir / "runs.jsonl", "a+b") as file_handle:
            if file_handle.seek(0, os.SEEK_END):
                file_handle.seek(-1, os.SEEK_END)
                if file_handle.read(1) != b"\n":
                    file_handle.write(b"\n")
            file_handle.write((json.dumps(record, sort_keys=True) + "\n").encode())
            file_handle.flush()
            os.fsync(file_handle.fileno())

    def load_run_records(self) -> list[dict]:
        runs_path = self.manifest_dir / "runs.jsonl"
        if not runs_path.exists():
            return []
        run_records = []
        with open(runs_path, encoding="utf-8") as file_handle:
            for line in file_handle:
                with contextlib.suppress(ValueError):
                    run_records.append(json.loads(line))
        return run_records

    def load_clean_state(self) -> dict[str, dict]:
        """The record of every published file. Hand edits are expected, so its shape is checked, not only its JSON."""
        state_path = self.manifest_dir / "clean_state.json"
        if not state_path.exists():
            return {}
        try:
            clean_state = json.loads(state_path.read_text(encoding="utf-8"))
            for file_id, entry in clean_state.items():
                if missing_fields := _CLEAN_STATE_FIELDS - entry.keys():
                    raise ValueError(f"the entry for {file_id} lacks {', '.join(sorted(missing_fields))}")
        except (ValueError, AttributeError) as error:  # AttributeError: not an object of objects
            raise ValueError(f"{state_path} is damaged: {error}") from error
        return clean_state

    def save_clean_state(self, clean_state: dict[str, dict]) -> None:
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.manifest_dir / "clean_state.json.tmp"
        with open(temp_path, "w", encoding="utf-8") as file_handle:
            file_handle.write(json.dumps(clean_state, indent=1, sort_keys=True))
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temp_path, self.manifest_dir / "clean_state.json")

    # lock ----------------------------------------------------------------------------------
    def _format_lock_holder(self) -> str:
        """' (pid N since T)' as written by the update holding the lock, or '' if the file does not say."""
        try:
            lock_info = json.loads(self.lock_path.read_text(encoding="utf-8"))
            return f" (pid {lock_info['pid']} since {lock_info['started']})"
        except OSError, ValueError, KeyError, TypeError:
            return ""

    @contextlib.contextmanager
    def acquire_lock(self) -> Iterator[None]:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        try:
            lock_fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise LakeLocked(
                f"{self.lock_path} exists{self._format_lock_holder()}: another update is running, or one was killed. "
                "If none is running, clear it with `python -m nfl_lake unlock`."
            ) from None
        try:
            with os.fdopen(lock_fd, "w") as lock_file:
                lock_file.write(json.dumps({"pid": os.getpid(), "started": format_utc_iso(utc_now())}))
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)
