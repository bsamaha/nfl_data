"""Where things live on disk, and the manifest that records them.

    <root>/raw/<dataset>/<file stem>/<upstream time>_<sha12>.parquet   files exactly as published
    <root>/clean/<dataset>/<file name>                                 latest version, contract applied
    <root>/rejects/<dataset>/<file name>                               rows set aside for a null key
    <root>/manifest/raw_files.jsonl                                    one line per stored or pruned raw file
    <root>/manifest/runs.jsonl                                         one line per update run
    <root>/manifest/clean_state.json                                   what each clean file was built from
"""
from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


class LakeLocked(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Lake:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.raw = self.root / "raw"
        self.clean = self.root / "clean"
        self.rejects = self.root / "rejects"
        self.manifest = self.root / "manifest"
        self.staging = self.root / "_staging"

    # paths ---------------------------------------------------------------------------------
    def raw_path(self, dataset: str, asset_name: str, updated_at: str, sha256: str) -> Path:
        name = Path(asset_name)
        stamp = re.sub(r"[^0-9TZ]", "", updated_at)  # 2026-11-21T16:46:06Z -> 20261121T164606Z
        return self.raw / dataset / name.stem / f"{stamp}_{sha256[:12]}{name.suffix}"

    def clean_path(self, dataset: str, asset_name: str) -> Path:
        return self.clean / dataset / asset_name

    def rejects_path(self, dataset: str, asset_name: str) -> Path:
        return self.rejects / dataset / asset_name

    def rel(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()

    # manifest ------------------------------------------------------------------------------
    def _append(self, name: str, rec: dict) -> None:
        self.manifest.mkdir(parents=True, exist_ok=True)
        with open(self.manifest / name, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _read(self, name: str) -> list[dict]:
        path = self.manifest / name
        if not path.exists():
            return []
        with open(path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def append_raw_event(self, rec: dict) -> None:
        self._append("raw_files.jsonl", rec)

    def raw_versions(self) -> dict[str, list[dict]]:
        """Raw versions currently on disk per file, oldest first."""
        versions: dict[str, list[dict]] = {}
        for e in self._read("raw_files.jsonl"):
            kept = [v for v in versions.get(e["key"], []) if v["path"] != e["path"]]
            if e["event"] == "stored":
                kept.append(e)
            versions[e["key"]] = kept
        return versions

    def append_run(self, rec: dict) -> None:
        self._append("runs.jsonl", rec)

    def runs(self) -> list[dict]:
        return self._read("runs.jsonl")

    def load_state(self) -> dict[str, dict]:
        path = self.manifest / "clean_state.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    def save_state(self, state: dict[str, dict]) -> None:
        self.manifest.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest / "clean_state.json.tmp"
        tmp.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.manifest / "clean_state.json")

    # lock ----------------------------------------------------------------------------------
    @property
    def lock_path(self) -> Path:
        return self.root / ".update.lock"

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise LakeLocked(
                f"{self.lock_path} exists: another update is running, or one was killed. "
                "If none is running, clear it with `python -m nfl_lake unlock`."
            ) from None
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps({"pid": os.getpid(), "started": iso(utc_now())}))
        try:
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)
