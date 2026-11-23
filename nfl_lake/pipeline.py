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

import concurrent.futures as concurrent_futures
import dataclasses
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import polars as pl
import pyarrow.parquet as pq
import structlog

from .catalog import Catalog, Dataset
from .contract import ContractError, build_clean, count_key_violations
from .source import ListingMismatch, SourceError, UpstreamAsset, UpstreamListing, UpstreamSource, compute_file_sha256
from .store import LakeStore, format_utc_iso, utc_now

logger = structlog.get_logger("nfl_lake")
# How many times a run lists upstream again when a download does not match the listing: straight away, then twice
# more after waiting the source's `listing_lag_seconds`.
LISTING_RECHECKS = 3
# What polars raises for a Parquet file it cannot read. PanicException is not an Exception subclass.
UNREADABLE_PARQUET_ERRORS = (pl.exceptions.PolarsError, pl.exceptions.PanicException, OSError)


class UpdateFailed(Exception):
    """`published_file_count` is how many clean files were replaced before the failure (0 unless publishing started)."""

    def __init__(self, errors: list[str], published_file_count: int = 0):
        super().__init__("; ".join(errors))
        self.errors = errors
        self.published_file_count = published_file_count


@dataclass(frozen=True)
class IngestTarget:
    dataset: Dataset
    asset: UpstreamAsset
    season: int | None

    @property
    def file_id(self) -> str:
        return f"{self.dataset.name}/{self.asset.filename}"


@dataclass(frozen=True)
class StagedCleanFile:
    """A clean file built in the staging folder, waiting to be published."""

    target: IngestTarget
    staged_clean_path: Path
    staged_rejects_path: Path | None
    clean_state_entry: dict  # its clean_state.json record, without built_at and run_id
    notes: list[str]


@dataclass
class UpdateSummary:
    run_id: str
    dry_run: bool = False
    target_file_count: int = 0
    dataset_count: int = 0
    fetched_file_ids: list[str] = field(default_factory=list)
    rebuilt_file_ids: list[str] = field(default_factory=list)  # in a dry run: the files that would be rebuilt
    unchanged_file_count: int = 0
    pruned_raw_count: int = 0
    bytes_fetched: int = 0
    rejected_rows_by_file: dict[str, int] = field(default_factory=dict)
    deduped_rows_by_file: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


class VerificationResult(NamedTuple):
    checked_file_count: int
    errors: list[str]


@dataclass(frozen=True)
class DatasetStatus:
    dataset_name: str
    file_count: int
    season_span: str
    row_count: int
    rejected_row_count: int
    latest_upstream_updated_at: str
    latest_built_at: str


class LakeStatus(NamedTuple):
    datasets: list[DatasetStatus]
    last_run: dict | None


def _format_name_sample(names: list[str], limit: int = 5) -> str:
    return ", ".join(names[:limit]) + (f" and {len(names) - limit} more" if len(names) > limit else "")


def plan_ingest_targets(
    catalog: Catalog,
    listing: UpstreamListing,
    clean_state: dict[str, dict],
    dataset_names: list[str] | None,
    season_filter: set[int] | None,
) -> tuple[list[IngestTarget], list[str]]:
    """Match upstream files to catalog datasets. A dataset with nothing to match is an error, not a skip."""
    targets: list[IngestTarget] = []
    errors: list[str] = []
    for dataset in catalog.select_datasets(dataset_names):
        release_assets = listing.get(dataset.spec.release)
        if release_assets is None:
            errors.append(f"{dataset.name}: upstream has no release named {dataset.spec.release!r}")
            continue
        matched_assets: list[tuple[UpstreamAsset, int | None]] = []
        for asset in release_assets.values():
            is_match, season = dataset.match_asset_filename(asset.filename)
            if is_match and (season is None or season >= dataset.spec.min_season):
                matched_assets.append((asset, season))
        if not matched_assets:
            errors.append(
                f"{dataset.name}: no upstream file matches {dataset.spec.file!r} in release {dataset.spec.release!r}"
            )
            continue
        selected_filenames = {asset.filename for asset, _ in matched_assets}
        published_filenames = sorted(
            state_entry["asset"] for state_entry in clean_state.values() if state_entry["dataset"] == dataset.name
        )
        missing_upstream_filenames = [name for name in published_filenames if name not in release_assets]
        if missing_upstream_filenames:
            errors.append(f"{dataset.name}: upstream no longer lists {_format_name_sample(missing_upstream_filenames)}")
        deselected_published_filenames = [
            name for name in published_filenames if name in release_assets and name not in selected_filenames
        ]
        if deselected_published_filenames:
            errors.append(
                f"{dataset.name}: {_format_name_sample(deselected_published_filenames)} is published but the catalog "
                "no longer selects it (min_season or file changed); restore the catalog entry, or delete the file "
                "from clean/ and rejects/ and its entry from manifest/clean_state.json"
            )
        for asset, season in sorted(matched_assets, key=lambda pair: pair[0].filename):
            if season_filter and season is not None and season not in season_filter:
                continue
            targets.append(IngestTarget(dataset, asset, season))
    return targets, errors


def _needs_fetch(target: IngestTarget, raw_versions_by_file: dict[str, list[dict]], lake: LakeStore) -> bool:
    stored_versions = raw_versions_by_file.get(target.file_id)
    if not stored_versions:
        return True
    latest_version = stored_versions[-1]
    if not (lake.root_dir / latest_version["path"]).exists():
        return True
    if target.asset.sha256:
        return latest_version["sha256"] != target.asset.sha256
    return (latest_version["size"], latest_version["upstream_updated_at"]) != (
        target.asset.size_bytes,
        target.asset.updated_at,
    )


def _needs_rebuild(
    target: IngestTarget,
    raw_versions_by_file: dict[str, list[dict]],
    clean_state: dict[str, dict],
    lake: LakeStore,
) -> bool:
    published_entry = clean_state.get(target.file_id)
    if published_entry is None or not lake.clean_file_path(target.dataset.name, target.asset.filename).exists():
        return True
    latest_raw_version = raw_versions_by_file[target.file_id][-1]
    return (
        published_entry["raw_path"] != latest_raw_version["path"]
        or published_entry["raw_sha256"] != latest_raw_version["sha256"]
        or published_entry["contract"] != target.dataset.contract_fingerprint
    )


def _fetch_one_target(
    source: UpstreamSource,
    lake: LakeStore,
    target: IngestTarget,
    staging_dir: Path,
    run_id: str,
) -> dict:
    """Runs in a worker thread. Shared parent folders already exist; see _execute_update."""
    staged_raw_path = staging_dir / "raw" / target.dataset.name / target.asset.filename
    source.fetch(target.asset, staged_raw_path)
    downloaded_bytes = staged_raw_path.stat().st_size
    if downloaded_bytes != target.asset.size_bytes:
        raise ListingMismatch(f"downloaded {downloaded_bytes} bytes but upstream lists {target.asset.size_bytes}")
    actual_sha256 = compute_file_sha256(staged_raw_path)
    if target.asset.sha256 and actual_sha256 != target.asset.sha256:
        raise ListingMismatch("checksum differs from the one upstream publishes")
    try:
        parquet_metadata = pq.read_metadata(staged_raw_path)
    except Exception as error:
        raise SourceError(f"not a readable Parquet file ({error})") from error
    parquet_kv_metadata = {
        meta_key.decode(): meta_value.decode(errors="replace")
        for meta_key, meta_value in (parquet_metadata.metadata or {}).items()
    }
    final_raw_path = lake.raw_file_path(
        target.dataset.name, target.asset.filename, target.asset.updated_at, actual_sha256
    )
    final_raw_path.parent.mkdir(exist_ok=True)  # this file's own folder; no other worker creates it
    os.replace(staged_raw_path, final_raw_path)
    return {
        "event": "stored",
        "file_id": target.file_id,
        "dataset": target.dataset.name,
        "release": target.asset.release_tag,
        "asset": target.asset.filename,
        "season": target.season,
        "url": target.asset.download_url,
        "upstream_updated_at": target.asset.updated_at,
        "upstream_built_at": parquet_kv_metadata.get("nflverse_timestamp"),
        "fetched_at": format_utc_iso(utc_now()),
        "size": downloaded_bytes,
        "sha256": actual_sha256,
        "rows": parquet_metadata.num_rows,
        "path": lake.relative_posix_path(final_raw_path),
        "run_id": run_id,
    }


def _fetch_targets(
    source: UpstreamSource,
    lake: LakeStore,
    targets_to_fetch: list[IngestTarget],
    staging_dir: Path,
    summary: UpdateSummary,
    workers: int,
) -> tuple[list[str], list[tuple[IngestTarget, str]]]:
    """Fetch files in parallel and record each one stored.

    Returns (errors, mismatched): `mismatched` pairs each target whose download did not match the listing with what differed.
    """
    errors: list[str] = []
    mismatched: list[tuple[IngestTarget, str]] = []
    # The folders that workers share are made here: threads creating one folder at the same moment can fail on Windows.
    for dataset_name in {target.dataset.name for target in targets_to_fetch}:
        (staging_dir / "raw" / dataset_name).mkdir(parents=True, exist_ok=True)
        (lake.raw_dir / dataset_name).mkdir(parents=True, exist_ok=True)
    with concurrent_futures.ThreadPoolExecutor(workers) as executor:
        target_by_future = {
            executor.submit(_fetch_one_target, source, lake, target, staging_dir, summary.run_id): target
            for target in targets_to_fetch
        }
        for future in concurrent_futures.as_completed(target_by_future):
            target = target_by_future[future]
            try:
                raw_file_record = future.result()
            except ListingMismatch as mismatch:
                mismatched.append((target, str(mismatch)))
                logger.warning("listing_mismatch", file=target.file_id, detail=str(mismatch))
            except Exception as error:  # noqa: BLE001 - whatever stops one file is reported and the others go on
                errors.append(f"{target.file_id}: {error}")
                logger.error("fetch_failed", file=target.file_id, error=str(error))
            else:
                lake.append_raw_file_event(raw_file_record)  # recorded even if another file fails, avoiding re-fetches
                summary.fetched_file_ids.append(target.file_id)
                summary.bytes_fetched += raw_file_record["size"]
                logger.info("fetched", file=target.file_id, rows=raw_file_record["rows"])
    return errors, mismatched


def _build_one_target(
    lake: LakeStore,
    target: IngestTarget,
    raw_version: dict,
    previous_clean_state: dict | None,
    staging_dir: Path,
    accept_schema_change: bool,
) -> StagedCleanFile:
    try:
        raw_frame = pl.read_parquet(lake.root_dir / raw_version["path"])
    except UNREADABLE_PARQUET_ERRORS as error:
        raise ContractError(
            f"raw file {raw_version['path']} cannot be read ({type(error).__name__}); "
            "delete it and run update to fetch it again"
        ) from error
    build_result = build_clean(raw_frame, target.dataset, previous_clean_state, accept_schema_change)
    staged_clean_path = staging_dir / "clean" / target.dataset.name / target.asset.filename
    staged_clean_path.parent.mkdir(parents=True, exist_ok=True)
    build_result.clean_frame.write_parquet(staged_clean_path, compression="zstd")
    staged_rejects_path = None
    if build_result.rejected_frame is not None:
        staged_rejects_path = staging_dir / "rejects" / target.dataset.name / target.asset.filename
        staged_rejects_path.parent.mkdir(parents=True, exist_ok=True)
        build_result.rejected_frame.write_parquet(staged_rejects_path, compression="zstd")
    # Only the facts are kept; holding every frame until publish would not fit in memory on a first run.
    clean_state_entry = {
        "dataset": target.dataset.name,
        "asset": target.asset.filename,
        "season": target.season,
        "raw_sha256": raw_version["sha256"],
        "raw_path": raw_version["path"],
        "upstream_updated_at": raw_version["upstream_updated_at"],
        "rows": build_result.clean_frame.height,
        "rows_in": build_result.input_row_count,
        "rejected_rows": build_result.rejected_row_count,
        "deduped_rows": build_result.deduped_row_count,
        "columns": build_result.column_schema,
        "contract": target.dataset.contract_fingerprint,
    }
    return StagedCleanFile(target, staged_clean_path, staged_rejects_path, clean_state_entry, build_result.notes)


def _execute_update(
    catalog: Catalog,
    source: UpstreamSource,
    lake: LakeStore,
    summary: UpdateSummary,
    staging_dir: Path,
    dataset_names: list[str] | None,
    season_filter: set[int] | None,
    accept_schema_change: bool,
    workers: int,
) -> None:
    listing = source.list_assets()
    clean_state = lake.load_clean_state()
    targets, errors = plan_ingest_targets(catalog, listing, clean_state, dataset_names, season_filter)
    if errors:
        raise UpdateFailed(errors)
    summary.target_file_count = len(targets)
    summary.dataset_count = len({target.dataset.name for target in targets})
    raw_versions_by_file = lake.load_raw_versions()
    targets_to_fetch = [target for target in targets if _needs_fetch(target, raw_versions_by_file, lake)]

    if summary.dry_run:
        summary.fetched_file_ids = sorted(target.file_id for target in targets_to_fetch)
        fetching_file_ids = set(summary.fetched_file_ids)
        summary.rebuilt_file_ids = sorted(
            target.file_id for target in targets
            if target.file_id in fetching_file_ids or _needs_rebuild(target, raw_versions_by_file, clean_state, lake)
        )
        summary.bytes_fetched = sum(target.asset.size_bytes for target in targets_to_fetch)
        summary.unchanged_file_count = len(targets) - len(summary.rebuilt_file_ids)
        return

    # 1. fetch
    logger.info(
        "fetching",
        files=len(targets_to_fetch),
        megabytes=round(sum(target.asset.size_bytes for target in targets_to_fetch) / 1e6, 1),
    )
    errors, mismatched = _fetch_targets(source, lake, targets_to_fetch, staging_dir, summary, workers)
    # A download that does not match the listing usually means upstream replaced the file after the listing was
    # made. List again and fetch what the new listing describes. Where the listing has not changed, give it time
    # to catch up before calling the download bad.
    for recheck in range(LISTING_RECHECKS):
        if errors or not mismatched:
            break
        if recheck:
            logger.info("waiting_for_listing", seconds=source.listing_lag_seconds, files=len(mismatched))
            time.sleep(source.listing_lag_seconds)
        fresh_listing = source.list_assets()
        relisted: list[IngestTarget] = []
        unchanged: list[tuple[IngestTarget, str]] = []
        for target, mismatch in mismatched:
            fresh_asset = fresh_listing.get(target.asset.release_tag, {}).get(target.asset.filename)
            if fresh_asset is not None and fresh_asset != target.asset:
                relisted.append(dataclasses.replace(target, asset=fresh_asset))
            else:
                unchanged.append((target, mismatch))
        more_errors, mismatched_again = _fetch_targets(source, lake, relisted, staging_dir, summary, workers)
        errors += more_errors
        mismatched = unchanged + mismatched_again
        if not source.listing_lag_seconds:  # this source's listing is always current, so waiting would change nothing
            break
    errors += [f"{target.file_id}: {mismatch}" for target, mismatch in mismatched]
    summary.fetched_file_ids.sort()
    if errors:
        raise UpdateFailed(sorted(errors))

    # 2. build
    raw_versions_by_file = lake.load_raw_versions()
    staged_files: list[StagedCleanFile] = []
    for target in targets:
        if not _needs_rebuild(target, raw_versions_by_file, clean_state, lake):
            continue
        try:
            staged_file = _build_one_target(
                lake,
                target,
                raw_versions_by_file[target.file_id][-1],
                clean_state.get(target.file_id),
                staging_dir,
                accept_schema_change,
            )
            staged_files.append(staged_file)
            logger.info("built", file=target.file_id, rows=staged_file.clean_state_entry["rows"])
        except ContractError as error:
            errors.append(f"{target.file_id}: {error}")
            logger.error("contract_failed", file=target.file_id, error=str(error))
    if errors:
        raise UpdateFailed(errors)

    # 3. publish
    published_at = format_utc_iso(utc_now())
    try:
        for staged_file in staged_files:
            target = staged_file.target
            # The rejects file goes first, so a file counts as published only once its clean copy is in place.
            final_rejects_path = lake.rejects_file_path(target.dataset.name, target.asset.filename)
            if staged_file.staged_rejects_path:
                final_rejects_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_file.staged_rejects_path, final_rejects_path)
            else:
                final_rejects_path.unlink(missing_ok=True)
            final_clean_path = lake.clean_file_path(target.dataset.name, target.asset.filename)
            final_clean_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged_file.staged_clean_path, final_clean_path)
            clean_state[target.file_id] = {
                **staged_file.clean_state_entry,
                "built_at": published_at,
                "run_id": summary.run_id,
            }
            summary.rebuilt_file_ids.append(target.file_id)
            if staged_file.clean_state_entry["rejected_rows"]:
                summary.rejected_rows_by_file[target.file_id] = staged_file.clean_state_entry["rejected_rows"]
            if staged_file.clean_state_entry["deduped_rows"]:
                summary.deduped_rows_by_file[target.file_id] = staged_file.clean_state_entry["deduped_rows"]
            summary.notes += [f"{target.file_id}: {note}" for note in staged_file.notes]
    finally:
        if summary.rebuilt_file_ids:  # also after a failure part-way, so clean/ and its record agree
            lake.save_clean_state(clean_state)

    for target in targets:
        if target.dataset.spec.history != "latest":
            continue
        for obsolete_raw in raw_versions_by_file.get(target.file_id, [])[:-1]:
            (lake.root_dir / obsolete_raw["path"]).unlink(missing_ok=True)
            lake.append_raw_file_event({
                "event": "pruned",
                "file_id": target.file_id,
                "path": obsolete_raw["path"],
                "run_id": summary.run_id,
                "pruned_at": published_at,
            })
            summary.pruned_raw_count += 1
    summary.unchanged_file_count = len(targets) - len(summary.rebuilt_file_ids)


def run_update(
    catalog: Catalog,
    source: UpstreamSource,
    lake: LakeStore,
    *,
    dataset_names: list[str] | None = None,
    season_filter: set[int] | None = None,
    accept_schema_change: bool = False,
    dry_run: bool = False,
    workers: int = 4,
) -> UpdateSummary:
    started_at = utc_now()
    summary = UpdateSummary(run_id=f"run_{started_at:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}", dry_run=dry_run)
    staging_dir = lake.staging_dir / summary.run_id

    def execute() -> None:
        _execute_update(
            catalog, source, lake, summary, staging_dir, dataset_names, season_filter, accept_schema_change, workers
        )

    def record_run(run_status: str, errors: list[str]) -> None:
        lake.append_run_record({
            "run_id": summary.run_id,
            "status": run_status,
            "errors": errors,
            "started_at": format_utc_iso(started_at),
            "finished_at": format_utc_iso(utc_now()),
            "datasets": dataset_names,
            "seasons": sorted(season_filter) if season_filter else None,
            "listed": summary.target_file_count,
            "fetched": len(summary.fetched_file_ids),
            "built": len(summary.rebuilt_file_ids),
            "unchanged": summary.unchanged_file_count,
            "pruned": summary.pruned_raw_count,
            "bytes_fetched": summary.bytes_fetched,
            "rejected_rows": sum(summary.rejected_rows_by_file.values()),
            "deduped_rows": sum(summary.deduped_rows_by_file.values()),
            "notes": summary.notes,
        })

    if dry_run:  # reads only: no lock, no manifest entry
        execute()
        return summary

    with lake.acquire_lock():
        try:
            execute()
        except UpdateFailed as error:
            record_run("failed", error.errors)
            raise
        except SourceError as error:  # upstream could not be listed
            record_run("failed", [str(error)])
            raise
        except Exception as error:
            errors = [f"{type(error).__name__}: {error}"]
            record_run("failed", errors)
            raise UpdateFailed(errors, published_file_count=len(summary.rebuilt_file_ids)) from error
        else:
            record_run("ok", [])
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
            try:
                lake.staging_dir.rmdir()  # only succeeds when empty
            except OSError:
                pass
    return summary


def verify_lake(
    catalog: Catalog,
    lake: LakeStore,
    dataset_names: list[str] | None = None,
    deep: bool = False,
) -> VerificationResult:
    """Re-check every published clean file against its contract."""
    clean_state = lake.load_clean_state()
    selected_datasets_by_name = {dataset.name: dataset for dataset in catalog.select_datasets(dataset_names)}
    checked_file_count = 0
    errors: list[str] = []
    for file_id, published_entry in sorted(clean_state.items()):
        dataset = selected_datasets_by_name.get(published_entry["dataset"])
        if dataset is None:
            if dataset_names is None:
                errors.append(
                    f"{file_id}: published, but dataset {published_entry['dataset']} is not in the catalog; restore "
                    "the entry, or delete the dataset's folders under clean/, rejects/ and raw/ and its entries "
                    "in manifest/clean_state.json"
                )
            continue
        checked_file_count += 1
        clean_path = lake.clean_file_path(dataset.name, published_entry["asset"])
        if not clean_path.exists():
            errors.append(f"{file_id}: clean file is missing")
        elif published_entry["contract"] != dataset.contract_fingerprint:
            errors.append(f"{file_id}: built under an older contract; run update to rebuild it")
        else:
            try:
                key_frame = pl.read_parquet(clean_path, columns=dataset.key_columns)
            except UNREADABLE_PARQUET_ERRORS as error:
                errors.append(f"{file_id}: clean file cannot be read ({type(error).__name__})")
            else:
                if key_frame.height != published_entry["rows"]:
                    errors.append(f"{file_id}: has {key_frame.height} rows but {published_entry['rows']} were published")
                violations = count_key_violations(key_frame, dataset.key_columns)
                if violations.null_key_rows or violations.duplicate_key_rows:
                    errors.append(
                        f"{file_id}: {violations.null_key_rows} row(s) with a null key, "
                        f"{violations.duplicate_key_rows} row(s) repeating a key"
                    )
        if deep:
            raw_path = lake.root_dir / published_entry["raw_path"]
            if not raw_path.exists():
                errors.append(f"{file_id}: raw file {published_entry['raw_path']} is missing")
            elif compute_file_sha256(raw_path) != published_entry["raw_sha256"]:
                errors.append(f"{file_id}: raw file {published_entry['raw_path']} no longer matches its recorded checksum")
    return VerificationResult(checked_file_count=checked_file_count, errors=errors)


def get_lake_status(catalog: Catalog, lake: LakeStore) -> LakeStatus:
    """Per-dataset totals of what is published, and the last run record.

    `season_span` is first-last, with a trailing * when a season inside that range is not published.
    """
    clean_state = lake.load_clean_state()
    dataset_summaries: list[DatasetStatus] = []
    for dataset in catalog.select_datasets():
        dataset_entries = [entry for entry in clean_state.values() if entry["dataset"] == dataset.name]
        seasons = sorted(entry["season"] for entry in dataset_entries if entry["season"] is not None)
        season_span = f"{seasons[0]}-{seasons[-1]}" if seasons else "-"
        if seasons and len(seasons) <= seasons[-1] - seasons[0]:
            season_span += "*"
        dataset_summaries.append(DatasetStatus(
            dataset_name=dataset.name,
            file_count=len(dataset_entries),
            season_span=season_span,
            row_count=sum(entry["rows"] for entry in dataset_entries),
            rejected_row_count=sum(entry["rejected_rows"] for entry in dataset_entries),
            latest_upstream_updated_at=max((entry["upstream_updated_at"] for entry in dataset_entries), default="-"),
            latest_built_at=max((entry["built_at"] for entry in dataset_entries), default="-"),
        ))
    run_records = lake.load_run_records()
    return LakeStatus(datasets=dataset_summaries, last_run=(run_records[-1] if run_records else None))
