"""One update run: list upstream, fetch what changed, rebuild what depends on it, publish only if every file passed.

A run has three phases, and each must finish cleanly before the next starts.

1. Fetch. Every file whose upstream checksum differs from the newest raw copy is downloaded,
   verified and stored under raw/. Raw files are never edited.
2. Build. Each file whose raw copy or contract changed is rebuilt into a staging folder and checked.
3. Publish. Only if every build passed are the staged files moved into clean/.

A failed fetch or build raises UpdateFailed, is recorded in runs.jsonl, and leaves clean/ untouched.
Publish goes file by file: the rejects file is moved or removed, then the clean file is renamed. A failure
there can leave some files published and one more file's rejects ahead of its clean copy; the next run
finishes it. A dry run and a held lock record nothing.
"""
from __future__ import annotations

import concurrent.futures as cf
import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import structlog

from .catalog import Catalog, Dataset
from .contract import ContractError, build_clean, check_key
from .source import Asset, Listing, Source, SourceError, sha256_file
from .store import Lake, iso, utc_now

log = structlog.get_logger("nfl_lake")
# What polars raises for a Parquet file it cannot read. PanicException is not an Exception subclass.
UNREADABLE = (pl.exceptions.PolarsError, pl.exceptions.PanicException, OSError)


class UpdateFailed(Exception):
    """`published` is the number of clean files replaced before the failure: 0 unless publishing had started."""

    def __init__(self, errors: list[str], published: int = 0):
        super().__init__("; ".join(errors))
        self.errors = errors
        self.published = published


@dataclass(frozen=True)
class Target:
    dataset: Dataset
    asset: Asset
    season: int | None

    @property
    def file_id(self) -> str:
        return f"{self.dataset.name}/{self.asset.name}"


@dataclass(frozen=True)
class Staged:
    """A clean file built in the staging folder, waiting to be published."""

    target: Target
    clean: Path
    rejects: Path | None
    entry: dict  # its clean_state.json record, without built_at and run_id
    notes: list[str]


@dataclass
class Summary:
    run_id: str
    dry_run: bool = False
    listed: int = 0
    dataset_count: int = 0
    fetched: list[str] = field(default_factory=list)
    built: list[str] = field(default_factory=list)
    unchanged: int = 0
    pruned: int = 0
    bytes_fetched: int = 0
    rejected: dict[str, int] = field(default_factory=dict)
    deduped: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def _some(names: list[str]) -> str:
    return ", ".join(names[:5]) + (f" and {len(names) - 5} more" if len(names) > 5 else "")


def plan(catalog: Catalog, listing: Listing, state: dict, datasets: list[str] | None,
         seasons: set[int] | None) -> tuple[list[Target], list[str]]:
    """Match upstream files to catalog datasets. A dataset with nothing to match is an error, not a skip."""
    targets: list[Target] = []
    errors: list[str] = []
    for ds in catalog.select(datasets):
        files = listing.get(ds.spec.release)
        if files is None:
            errors.append(f"{ds.name}: upstream has no release named {ds.spec.release!r}")
            continue
        matched: list[tuple[Asset, int | None]] = []
        for asset in files.values():
            ok, season = ds.match(asset.name)
            if ok and (season is None or season >= ds.spec.min_season):
                matched.append((asset, season))
        if not matched:
            errors.append(f"{ds.name}: no upstream file matches {ds.spec.file!r} in release {ds.spec.release!r}")
            continue
        selected = {asset.name for asset, _ in matched}
        published = sorted(s["asset"] for s in state.values() if s["dataset"] == ds.name)
        gone = [name for name in published if name not in files]
        if gone:
            errors.append(f"{ds.name}: upstream no longer lists {_some(gone)}")
        dropped = [name for name in published if name in files and name not in selected]
        if dropped:
            errors.append(f"{ds.name}: {_some(dropped)} is published but the catalog no longer selects it "
                          "(min_season or file changed); restore the catalog entry, or delete the file from clean/ "
                          "and rejects/ and its entry from manifest/clean_state.json")
        for asset, season in sorted(matched, key=lambda m: m[0].name):
            if seasons and season is not None and season not in seasons:
                continue
            targets.append(Target(ds, asset, season))
    return targets, errors


def _needs_fetch(t: Target, versions: dict[str, list[dict]], lake: Lake) -> bool:
    have = versions.get(t.file_id)
    if not have:
        return True
    last = have[-1]
    if not (lake.root / last["path"]).exists():
        return True
    if t.asset.sha256:
        return last["sha256"] != t.asset.sha256
    return (last["size"], last["upstream_updated_at"]) != (t.asset.size, t.asset.updated_at)


def _needs_build(t: Target, versions: dict[str, list[dict]], state: dict, lake: Lake) -> bool:
    built = state.get(t.file_id)
    if built is None or not lake.clean_path(t.dataset.name, t.asset.name).exists():
        return True
    raw = versions[t.file_id][-1]
    return built["raw_path"] != raw["path"] or built["raw_sha256"] != raw["sha256"] or built["contract"] != t.dataset.fingerprint


def _fetch_one(source: Source, lake: Lake, t: Target, staging: Path, run_id: str) -> dict:
    """Runs in a worker thread. The folders it shares with other workers already exist; see _execute."""
    tmp = staging / "raw" / t.dataset.name / t.asset.name
    source.fetch(t.asset, tmp)
    size = tmp.stat().st_size
    if size != t.asset.size:
        raise SourceError(f"downloaded {size} bytes but upstream lists {t.asset.size}")
    sha = sha256_file(tmp)
    if t.asset.sha256 and sha != t.asset.sha256:
        raise SourceError("checksum differs from the one upstream publishes")
    try:
        meta = pq.read_metadata(tmp)
    except Exception as e:
        raise SourceError(f"not a readable Parquet file ({e})") from e
    kv = {k.decode(): v.decode(errors="replace") for k, v in (meta.metadata or {}).items()}
    dest = lake.raw_path(t.dataset.name, t.asset.name, t.asset.updated_at, sha)
    dest.parent.mkdir(exist_ok=True)  # this file's own folder; no other worker creates it
    os.replace(tmp, dest)
    return {
        "event": "stored", "file_id": t.file_id, "dataset": t.dataset.name, "release": t.asset.release,
        "asset": t.asset.name, "season": t.season, "url": t.asset.url,
        "upstream_updated_at": t.asset.updated_at, "upstream_built_at": kv.get("nflverse_timestamp"),
        "fetched_at": iso(utc_now()), "size": size, "sha256": sha, "rows": meta.num_rows,
        "path": lake.rel(dest), "run_id": run_id,
    }


def _build_one(lake: Lake, t: Target, raw: dict, previous: dict | None, staging: Path,
               accept_schema_change: bool) -> Staged:
    try:
        frame = pl.read_parquet(lake.root / raw["path"])
    except UNREADABLE as e:
        raise ContractError(f"raw file {raw['path']} cannot be read ({type(e).__name__}); "
                            "delete it and run update to fetch it again") from e
    built = build_clean(frame, t.dataset, previous, accept_schema_change)
    clean = staging / "clean" / t.dataset.name / t.asset.name
    clean.parent.mkdir(parents=True, exist_ok=True)
    built.frame.write_parquet(clean, compression="zstd")
    rejects = None
    if built.rejects is not None:
        rejects = staging / "rejects" / t.dataset.name / t.asset.name
        rejects.parent.mkdir(parents=True, exist_ok=True)
        built.rejects.write_parquet(rejects, compression="zstd")
    # Only the facts are kept; holding every frame until publish would not fit in memory on a first run.
    entry = {
        "dataset": t.dataset.name, "asset": t.asset.name, "season": t.season,
        "raw_sha256": raw["sha256"], "raw_path": raw["path"], "upstream_updated_at": raw["upstream_updated_at"],
        "rows": built.frame.height, "rows_in": built.rows_in, "rejected_rows": built.rejected,
        "deduped_rows": built.deduped, "columns": built.columns, "contract": t.dataset.fingerprint,
    }
    return Staged(t, clean, rejects, entry, built.notes)


def _execute(catalog: Catalog, source: Source, lake: Lake, summary: Summary, staging: Path,
             datasets: list[str] | None, seasons: set[int] | None, accept_schema_change: bool, workers: int) -> None:
    listing = source.list_assets()
    state = lake.load_state()
    targets, errors = plan(catalog, listing, state, datasets, seasons)
    if errors:
        raise UpdateFailed(errors)
    summary.listed = len(targets)
    summary.dataset_count = len({t.dataset.name for t in targets})
    versions = lake.raw_versions()
    to_fetch = [t for t in targets if _needs_fetch(t, versions, lake)]

    if summary.dry_run:
        summary.fetched = sorted(t.file_id for t in to_fetch)
        fetching = set(summary.fetched)
        summary.built = sorted(t.file_id for t in targets
                               if t.file_id in fetching or _needs_build(t, versions, state, lake))
        summary.bytes_fetched = sum(t.asset.size for t in to_fetch)
        summary.unchanged = len(targets) - len(summary.built)
        return

    # 1. fetch
    log.info("fetching", files=len(to_fetch), megabytes=round(sum(t.asset.size for t in to_fetch) / 1e6, 1))
    # The folders that workers share are made here: threads creating one folder at the same moment can fail on Windows.
    for name in {t.dataset.name for t in to_fetch}:
        (staging / "raw" / name).mkdir(parents=True, exist_ok=True)
        (lake.raw / name).mkdir(parents=True, exist_ok=True)
    with cf.ThreadPoolExecutor(workers) as pool:
        futures = {pool.submit(_fetch_one, source, lake, t, staging, summary.run_id): t for t in to_fetch}
        for fut in cf.as_completed(futures):
            t = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                errors.append(f"{t.file_id}: {e}")
                log.error("fetch_failed", file=t.file_id, error=str(e))
            else:
                lake.append_raw_event(rec)  # recorded even if another file fails, so it is not fetched twice
                summary.fetched.append(t.file_id)
                summary.bytes_fetched += rec["size"]
                log.info("fetched", file=t.file_id, rows=rec["rows"])
    summary.fetched.sort()
    if errors:
        raise UpdateFailed(sorted(errors))

    # 2. build
    versions = lake.raw_versions()
    staged: list[Staged] = []
    for t in targets:
        if not _needs_build(t, versions, state, lake):
            continue
        try:
            staged.append(_build_one(lake, t, versions[t.file_id][-1], state.get(t.file_id), staging,
                                     accept_schema_change))
            log.info("built", file=t.file_id, rows=staged[-1].entry["rows"])
        except ContractError as e:
            errors.append(f"{t.file_id}: {e}")
            log.error("contract_failed", file=t.file_id, error=str(e))
    if errors:
        raise UpdateFailed(errors)

    # 3. publish
    now = iso(utc_now())
    try:
        for s in staged:
            t = s.target
            # The rejects file goes first, so a file counts as published only once its clean copy is in place.
            rejects = lake.rejects_path(t.dataset.name, t.asset.name)
            if s.rejects:
                rejects.parent.mkdir(parents=True, exist_ok=True)
                os.replace(s.rejects, rejects)
            else:
                rejects.unlink(missing_ok=True)
            dest = lake.clean_path(t.dataset.name, t.asset.name)
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(s.clean, dest)
            state[t.file_id] = {**s.entry, "built_at": now, "run_id": summary.run_id}
            summary.built.append(t.file_id)
            if s.entry["rejected_rows"]:
                summary.rejected[t.file_id] = s.entry["rejected_rows"]
            if s.entry["deduped_rows"]:
                summary.deduped[t.file_id] = s.entry["deduped_rows"]
            summary.notes += [f"{t.file_id}: {note}" for note in s.notes]
    finally:
        if summary.built:  # also after a failure part-way, so clean/ and its record agree
            lake.save_state(state)

    for t in targets:
        if t.dataset.spec.history != "latest":
            continue
        for old in versions.get(t.file_id, [])[:-1]:
            (lake.root / old["path"]).unlink(missing_ok=True)
            lake.append_raw_event({"event": "pruned", "file_id": t.file_id, "path": old["path"],
                                   "run_id": summary.run_id, "pruned_at": now})
            summary.pruned += 1
    summary.unchanged = len(targets) - len(summary.built)


def run_update(catalog: Catalog, source: Source, lake: Lake, *, datasets: list[str] | None = None,
               seasons: set[int] | None = None, accept_schema_change: bool = False, dry_run: bool = False,
               workers: int = 4) -> Summary:
    started = utc_now()
    summary = Summary(run_id=f"run_{started:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}", dry_run=dry_run)
    staging = lake.staging / summary.run_id

    def execute() -> None:
        _execute(catalog, source, lake, summary, staging, datasets, seasons, accept_schema_change, workers)

    def record(status: str, errors: list[str]) -> None:
        lake.append_run({
            "run_id": summary.run_id, "status": status, "errors": errors, "started_at": iso(started),
            "finished_at": iso(utc_now()), "datasets": datasets, "seasons": sorted(seasons) if seasons else None,
            "listed": summary.listed, "fetched": len(summary.fetched), "built": len(summary.built),
            "unchanged": summary.unchanged, "pruned": summary.pruned, "bytes_fetched": summary.bytes_fetched,
            "rejected_rows": sum(summary.rejected.values()), "deduped_rows": sum(summary.deduped.values()),
            "notes": summary.notes,
        })

    if dry_run:  # reads only: no lock, no manifest entry
        execute()
        return summary

    with lake.lock():
        try:
            execute()
        except UpdateFailed as e:
            record("failed", e.errors)
            raise
        except SourceError as e:  # upstream could not be listed
            record("failed", [str(e)])
            raise
        except Exception as e:
            errors = [f"{type(e).__name__}: {e}"]
            record("failed", errors)
            raise UpdateFailed(errors, published=len(summary.built)) from e
        else:
            record("ok", [])
        finally:
            shutil.rmtree(staging, ignore_errors=True)
            try:
                lake.staging.rmdir()  # only succeeds when empty
            except OSError:
                pass
    return summary


def verify(catalog: Catalog, lake: Lake, datasets: list[str] | None = None, deep: bool = False) -> tuple[int, list[str]]:
    """Re-check every published clean file against its contract. Returns (files checked, problems)."""
    state = lake.load_state()
    wanted = {ds.name: ds for ds in catalog.select(datasets)}
    checked, errors = 0, []
    for file_id, st in sorted(state.items()):
        ds = wanted.get(st["dataset"])
        if ds is None:
            if datasets is None:
                errors.append(f"{file_id}: published, but dataset {st['dataset']} is not in the catalog; restore the "
                              "entry, or delete the dataset's folders under clean/, rejects/ and raw/ and its entries "
                              "in manifest/clean_state.json")
            continue
        checked += 1
        path = lake.clean_path(ds.name, st["asset"])
        if not path.exists():
            errors.append(f"{file_id}: clean file is missing")
        elif st["contract"] != ds.fingerprint:
            errors.append(f"{file_id}: built under an older contract; run update to rebuild it")
        else:
            try:
                df = pl.read_parquet(path, columns=ds.key)
            except UNREADABLE as e:
                errors.append(f"{file_id}: clean file cannot be read ({type(e).__name__})")
            else:
                if df.height != st["rows"]:
                    errors.append(f"{file_id}: has {df.height} rows but {st['rows']} were published")
                nulls, dups = check_key(df, ds.key)
                if nulls or dups:
                    errors.append(f"{file_id}: {nulls} row(s) with a null key, {dups} row(s) repeating a key")
        if deep:
            raw = lake.root / st["raw_path"]
            if not raw.exists():
                errors.append(f"{file_id}: raw file {st['raw_path']} is missing")
            elif sha256_file(raw) != st["raw_sha256"]:
                errors.append(f"{file_id}: raw file {st['raw_path']} no longer matches its recorded checksum")
    return checked, errors


def status(catalog: Catalog, lake: Lake) -> tuple[list[dict], dict | None]:
    """Per-dataset totals of what is published, and the last run record.

    `seasons` is first-last, with a trailing * when a season inside that range is not published.
    """
    state = lake.load_state()
    rows = []
    for ds in catalog.select():
        items = [s for s in state.values() if s["dataset"] == ds.name]
        seasons = sorted(s["season"] for s in items if s["season"] is not None)
        span = f"{seasons[0]}-{seasons[-1]}" if seasons else "-"
        if seasons and len(seasons) <= seasons[-1] - seasons[0]:
            span += "*"
        rows.append({
            "dataset": ds.name, "files": len(items),
            "seasons": span,
            "rows": sum(s["rows"] for s in items), "rejected": sum(s["rejected_rows"] for s in items),
            "upstream": max((s["upstream_updated_at"] for s in items), default="-"),
            "built": max((s["built_at"] for s in items), default="-"),
        })
    runs = lake.runs()
    return rows, (runs[-1] if runs else None)
