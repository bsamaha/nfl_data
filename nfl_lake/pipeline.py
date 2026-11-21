"""One update run: list upstream, fetch what changed, rebuild what depends on it, publish all or nothing.

A run has three phases, and each must finish cleanly before the next starts.

1. Fetch. Every file whose upstream checksum differs from the newest raw copy is downloaded,
   verified and stored under raw/. Raw files are never edited.
2. Build. Each file whose raw copy or contract changed is rebuilt into a staging folder and checked.
3. Publish. Only if every build passed are the staged files moved into clean/.

Any failure raises UpdateFailed, leaves clean/ exactly as it was, and is written to the run log.
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


class UpdateFailed(Exception):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass(frozen=True)
class Target:
    dataset: Dataset
    asset: Asset
    season: int | None

    @property
    def key(self) -> str:
        return f"{self.dataset.name}/{self.asset.name}"


@dataclass
class Summary:
    run_id: str
    dry_run: bool = False
    listed: int = 0
    datasets: int = 0
    fetched: list[str] = field(default_factory=list)
    built: list[str] = field(default_factory=list)
    unchanged: int = 0
    pruned: int = 0
    bytes_fetched: int = 0
    rejected: dict[str, int] = field(default_factory=dict)
    deduped: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def plan(catalog: Catalog, listing: Listing, state: dict, datasets: list[str] | None = None,
         seasons: set[int] | None = None) -> tuple[list[Target], list[str]]:
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
        names = {asset.name for asset, _ in matched}
        gone = sorted(s["asset"] for s in state.values() if s["dataset"] == ds.name and s["asset"] not in names)
        if gone:
            errors.append(f"{ds.name}: upstream no longer lists {', '.join(gone[:5])}"
                          + (f" and {len(gone) - 5} more" if len(gone) > 5 else ""))
        for asset, season in sorted(matched, key=lambda m: m[0].name):
            if seasons and season is not None and season not in seasons:
                continue
            targets.append(Target(ds, asset, season))
    return targets, errors


def _needs_fetch(t: Target, versions: dict[str, list[dict]], lake: Lake) -> bool:
    have = versions.get(t.key)
    if not have:
        return True
    last = have[-1]
    if not (lake.root / last["path"]).exists():
        return True
    if t.asset.sha256:
        return last["sha256"] != t.asset.sha256
    return (last["size"], last["upstream_updated_at"]) != (t.asset.size, t.asset.updated_at)


def _needs_build(t: Target, versions: dict[str, list[dict]], state: dict, lake: Lake) -> bool:
    built = state.get(t.key)
    if built is None or not lake.clean_path(t.dataset.name, t.asset.name).exists():
        return True
    return built["raw_sha256"] != versions[t.key][-1]["sha256"] or built["contract"] != t.dataset.fingerprint


def _fetch_one(source: Source, lake: Lake, t: Target, staging: Path, run_id: str) -> dict:
    tmp = staging / "raw" / t.dataset.name / t.asset.name
    tmp.parent.mkdir(parents=True, exist_ok=True)
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
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, dest)
    return {
        "event": "stored", "key": t.key, "dataset": t.dataset.name, "release": t.asset.release,
        "asset": t.asset.name, "season": t.season, "url": t.asset.url,
        "upstream_updated_at": t.asset.updated_at, "upstream_built_at": kv.get("nflverse_timestamp"),
        "fetched_at": iso(utc_now()), "size": size, "sha256": sha, "rows": meta.num_rows,
        "path": lake.rel(dest), "run_id": run_id,
    }


def _build_one(lake: Lake, t: Target, raw: dict, previous: dict | None, staging: Path, accept: bool) -> dict:
    built = build_clean(pl.read_parquet(lake.root / raw["path"]), t.dataset, t.key, previous, accept)
    staged = staging / "clean" / t.dataset.name / t.asset.name
    staged.parent.mkdir(parents=True, exist_ok=True)
    built.frame.write_parquet(staged, compression="zstd")
    staged_rejects = None
    if built.rejects is not None:
        staged_rejects = staging / "rejects" / t.dataset.name / t.asset.name
        staged_rejects.parent.mkdir(parents=True, exist_ok=True)
        built.rejects.write_parquet(staged_rejects, compression="zstd")
    # Only the facts are kept; holding every frame until publish would not fit in memory on a bootstrap.
    return {"target": t, "raw": raw, "staged": staged, "staged_rejects": staged_rejects, "rows": built.frame.height,
            "rows_in": built.rows_in, "rejected": built.rejected, "deduped": built.deduped,
            "columns": built.columns, "notes": built.notes}


def _execute(catalog: Catalog, source: Source, lake: Lake, summary: Summary, staging: Path,
             datasets: list[str] | None, seasons: set[int] | None, accept: bool, workers: int) -> None:
    listing = source.list_assets()
    state = lake.load_state()
    targets, errors = plan(catalog, listing, state, datasets, seasons)
    if errors:
        raise UpdateFailed(errors)
    summary.listed = len(targets)
    summary.datasets = len({t.dataset.name for t in targets})
    versions = lake.raw_versions()
    to_fetch = [t for t in targets if _needs_fetch(t, versions, lake)]

    if summary.dry_run:
        summary.fetched = sorted(t.key for t in to_fetch)
        fetching = set(summary.fetched)
        summary.built = sorted(t.key for t in targets if t.key in fetching or _needs_build(t, versions, state, lake))
        summary.bytes_fetched = sum(t.asset.size for t in to_fetch)
        summary.unchanged = len(targets) - len(summary.built)
        return

    # 1. fetch
    log.info("fetching", files=len(to_fetch), megabytes=round(sum(t.asset.size for t in to_fetch) / 1e6, 1))
    with cf.ThreadPoolExecutor(max(1, workers)) as pool:
        futures = {pool.submit(_fetch_one, source, lake, t, staging, summary.run_id): t for t in to_fetch}
        for fut in cf.as_completed(futures):
            t = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                errors.append(f"{t.key}: {e}")
                log.error("fetch_failed", file=t.key, error=str(e))
            else:
                lake.append_raw_event(rec)  # recorded even if another file fails, so it is not fetched twice
                summary.fetched.append(t.key)
                summary.bytes_fetched += rec["size"]
                log.info("fetched", file=t.key, rows=rec["rows"])
    summary.fetched.sort()
    if errors:
        raise UpdateFailed(sorted(errors))

    # 2. build
    versions = lake.raw_versions()
    candidates = []
    for t in targets:
        if not _needs_build(t, versions, state, lake):
            continue
        try:
            candidates.append(_build_one(lake, t, versions[t.key][-1], state.get(t.key), staging, accept))
            log.info("built", file=t.key, rows=candidates[-1]["rows"])
        except ContractError as e:
            errors.append(str(e))
            log.error("contract_failed", file=t.key, error=str(e))
    if errors:
        raise UpdateFailed(errors)

    # 3. publish
    now = iso(utc_now())
    for c in candidates:
        t, raw = c["target"], c["raw"]
        dest = lake.clean_path(t.dataset.name, t.asset.name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(c["staged"], dest)
        rejects = lake.rejects_path(t.dataset.name, t.asset.name)
        if c["staged_rejects"]:
            rejects.parent.mkdir(parents=True, exist_ok=True)
            os.replace(c["staged_rejects"], rejects)
        else:
            rejects.unlink(missing_ok=True)
        state[t.key] = {
            "dataset": t.dataset.name, "asset": t.asset.name, "season": t.season,
            "raw_sha256": raw["sha256"], "raw_path": raw["path"], "upstream_updated_at": raw["upstream_updated_at"],
            "rows": c["rows"], "rows_in": c["rows_in"], "rejected_rows": c["rejected"], "deduped_rows": c["deduped"],
            "columns": c["columns"], "contract": t.dataset.fingerprint, "built_at": now, "run_id": summary.run_id,
        }
        summary.built.append(t.key)
        if c["rejected"]:
            summary.rejected[t.key] = c["rejected"]
        if c["deduped"]:
            summary.deduped[t.key] = c["deduped"]
        summary.notes += c["notes"]
    if candidates:
        lake.save_state(state)

    for t in targets:
        if t.dataset.history != "latest":
            continue
        for old in versions.get(t.key, [])[:-1]:
            (lake.root / old["path"]).unlink(missing_ok=True)
            lake.append_raw_event({"event": "pruned", "key": t.key, "path": old["path"], "run_id": summary.run_id, "pruned_at": now})
            summary.pruned += 1
    summary.unchanged = len(targets) - len(summary.built)


def run_update(catalog: Catalog, source: Source, lake: Lake, datasets: list[str] | None = None,
               seasons: set[int] | None = None, accept_schema_change: bool = False, dry_run: bool = False,
               workers: int = 4) -> Summary:
    started = utc_now()
    summary = Summary(run_id=f"run_{started:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}", dry_run=dry_run)
    staging = lake.staging / summary.run_id

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
        _execute(catalog, source, lake, summary, staging, datasets, seasons, accept_schema_change, workers)
        return summary

    with lake.lock():
        try:
            _execute(catalog, source, lake, summary, staging, datasets, seasons, accept_schema_change, workers)
        except UpdateFailed as e:
            record("failed", e.errors)
            raise
        except Exception as e:
            errors = [f"{type(e).__name__}: {e}"]
            record("failed", errors)
            raise UpdateFailed(errors) from e
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
    for key, st in sorted(state.items()):
        ds = wanted.get(st["dataset"])
        if ds is None:
            continue
        checked += 1
        path = lake.clean_path(ds.name, st["asset"])
        if not path.exists():
            errors.append(f"{key}: clean file is missing")
        elif st["contract"] != ds.fingerprint:
            errors.append(f"{key}: built under an older contract; run update to rebuild it")
        else:
            df = pl.read_parquet(path, columns=ds.key)
            if df.height != st["rows"]:
                errors.append(f"{key}: has {df.height} rows but {st['rows']} were published")
            nulls, dups = check_key(df, ds.key)
            if nulls or dups:
                errors.append(f"{key}: {nulls} row(s) with a null key, {dups} row(s) repeating a key")
        if deep:
            raw = lake.root / st["raw_path"]
            if not raw.exists():
                errors.append(f"{key}: raw file {st['raw_path']} is missing")
            elif sha256_file(raw) != st["raw_sha256"]:
                errors.append(f"{key}: raw file {st['raw_path']} no longer matches its recorded checksum")
    return checked, errors


def status(catalog: Catalog, lake: Lake) -> tuple[list[dict], dict | None]:
    """Per-dataset totals of what is published, and the last run record."""
    state = lake.load_state()
    rows = []
    for ds in catalog.select():
        items = [s for s in state.values() if s["dataset"] == ds.name]
        seasons = sorted(s["season"] for s in items if s["season"] is not None)
        rows.append({
            "dataset": ds.name, "files": len(items),
            "seasons": f"{seasons[0]}-{seasons[-1]}" if seasons else "-",
            "rows": sum(s["rows"] for s in items), "rejected": sum(s["rejected_rows"] for s in items),
            "upstream": max((s["upstream_updated_at"] for s in items), default="-"),
            "built": max((s["built_at"] for s in items), default="-"),
        })
    runs = lake.runs()
    return rows, (runs[-1] if runs else None)
