"""One update run: list upstream, fetch what changed, rebuild what depends on it, publish each passing dataset.

Each dataset runs through three phases atomically so one broken upstream dataset does not discard good
downloads from the rest:

1. Fetch. Files needing rebuild are downloaded into staging, verified via streaming SHA-256, and checked
   as readable Parquet (reusing the stored raw snapshot when a `history: all` contract changes).
2. Build. Each file is rebuilt into staging and checked against its contract. If clean output bytes are
   identical to what is already published (a metadata-only upstream rebuild), clean/ and raw/ are not rewritten.
3. Publish. Only if every file in the dataset passed are its staged files moved into clean/ (and raw/ for
   `history: all`).

A failed dataset leaves its raw/, clean/ and rejects/ untouched; other datasets in the same run still publish,
and the run ends with UpdateFailed (recorded as failed in runs.jsonl). A dry run and a held lock record nothing.
"""

from __future__ import annotations

import concurrent.futures as concurrent_futures
import contextlib
import dataclasses
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import polars as pl

from .catalog import Catalog, Dataset
from .contract import ContractError, build_clean, count_key_violations
from .source import ListingMismatch, SourceError, UpstreamAsset, UpstreamListing, UpstreamSource, compute_file_sha256
from .store import LakeStore, format_utc_iso, utc_now

logger = logging.getLogger("nfl_lake")
# How many times a run lists upstream again when a download does not match the listing: straight away, then twice
# more after waiting the source's `listing_lag_seconds`.
LISTING_RECHECKS = 3
# What polars raises for a Parquet file it cannot read. PanicException is not an Exception subclass.
UNREADABLE_PARQUET_ERRORS = (pl.exceptions.PolarsError, pl.exceptions.PanicException, OSError)
_RAW_METADATA_FIELDS = ("raw_sha256", "raw_size", "raw_path", "upstream_updated_at", "upstream_built_at")


class UpdateFailed(Exception):
    """`published_file_count` is how many clean files were replaced during the run (0 if none published)."""

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
class FetchedRawFile:
    """One upstream file downloaded into staging and verified intact."""

    staged_path: Path
    raw_metadata: dict  # raw_sha256, raw_size, raw_path, upstream_updated_at, upstream_built_at


@dataclass(frozen=True)
class StagedCleanFile:
    """One built target ready to publish. `staged_clean_path` is None when only upstream metadata changed."""

    target: IngestTarget
    staged_clean_path: Path | None
    staged_rejects_path: Path | None
    staged_raw_path: Path | None  # set only for freshly fetched `history: all` datasets with changed data
    clean_state_entry: dict
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
        release_assets = listing.get(dataset.release)
        if release_assets is None:
            errors.append(f"{dataset.name}: upstream has no release named {dataset.release!r}")
            continue
        matched_assets: list[tuple[UpstreamAsset, int | None]] = []
        for asset in release_assets.values():
            is_match, season = dataset.match_asset_filename(asset.filename)
            if is_match and (season is None or season >= dataset.min_season):
                matched_assets.append((asset, season))
        if not matched_assets:
            errors.append(f"{dataset.name}: no upstream file matches {dataset.file!r} in release {dataset.release!r}")
            continue
        selected_filenames = {asset.filename for asset, _ in matched_assets}
        published_filenames = sorted(
            state_entry["asset"] for state_entry in clean_state.values() if state_entry["dataset"] == dataset.name
        )
        missing_upstream_filenames = [name for name in published_filenames if name not in release_assets]
        if missing_upstream_filenames:
            errors.append(f"{dataset.name}: upstream no longer lists {_format_name_sample(missing_upstream_filenames)}")
            continue
        deselected_published_filenames = [
            name for name in published_filenames if name in release_assets and name not in selected_filenames
        ]
        if deselected_published_filenames:
            errors.append(
                f"{dataset.name}: {_format_name_sample(deselected_published_filenames)} is published but the catalog "
                "no longer selects it (min_season or file changed); restore the catalog entry, or delete the file "
                "from clean/ and rejects/ and its entry from manifest/clean_state.json"
            )
            continue
        for asset, season in sorted(matched_assets, key=lambda pair: pair[0].filename):
            if season_filter and season is not None and season not in season_filter:
                continue
            targets.append(IngestTarget(dataset, asset, season))
    return targets, errors


def _upstream_changed(target: IngestTarget, published_entry: dict) -> bool:
    if target.asset.sha256:
        return published_entry["raw_sha256"] != target.asset.sha256
    return (published_entry["raw_size"], published_entry["upstream_updated_at"]) != (
        target.asset.size_bytes,
        target.asset.updated_at,
    )


def _has_stored_raw(published_entry: dict | None, lake: LakeStore) -> bool:
    return bool(
        published_entry and published_entry.get("raw_path") and (lake.root_dir / published_entry["raw_path"]).exists()
    )


def _needs_rebuild(target: IngestTarget, clean_state: dict[str, dict], lake: LakeStore) -> bool:
    published = clean_state.get(target.file_id)
    if published is None or not lake.clean_file_path(target.dataset.name, target.asset.filename).exists():
        return True
    if _upstream_changed(target, published) or published["contract"] != target.dataset.contract:
        return True
    return target.dataset.history == "all" and not _has_stored_raw(published, lake)


def _needs_fetch(target: IngestTarget, clean_state: dict[str, dict], lake: LakeStore) -> bool:
    if not _needs_rebuild(target, clean_state, lake):
        return False
    published = clean_state.get(target.file_id)
    return published is None or _upstream_changed(target, published) or not _has_stored_raw(published, lake)


def _fetch_one_target(
    source: UpstreamSource,
    lake: LakeStore,
    target: IngestTarget,
    staging_dir: Path,
) -> FetchedRawFile:
    """Runs in a worker thread. Shared parent folders already exist; see _fetch_targets."""
    staged_raw_path = staging_dir / "raw" / target.dataset.name / target.asset.filename
    downloaded = source.fetch(target.asset, staged_raw_path)
    if downloaded.size_bytes != target.asset.size_bytes:
        raise ListingMismatch(f"downloaded {downloaded.size_bytes} bytes but upstream lists {target.asset.size_bytes}")
    if target.asset.sha256 and downloaded.sha256 != target.asset.sha256:
        raise ListingMismatch("checksum differs from the one upstream publishes")
    try:
        parquet_kv_metadata = pl.read_parquet_metadata(staged_raw_path)
    except UNREADABLE_PARQUET_ERRORS as error:
        raise SourceError(f"not a readable Parquet file ({error})") from error
    raw_path_rel = (
        lake.relative_posix_path(
            lake.raw_file_path(target.dataset.name, target.asset.filename, target.asset.updated_at, downloaded.sha256)
        )
        if target.dataset.history == "all"
        else None
    )
    return FetchedRawFile(
        staged_path=staged_raw_path,
        raw_metadata={
            "raw_sha256": downloaded.sha256,
            "raw_size": downloaded.size_bytes,
            "raw_path": raw_path_rel,
            "upstream_updated_at": target.asset.updated_at,
            "upstream_built_at": parquet_kv_metadata.get("nflverse_timestamp"),
        },
    )


def _fetch_targets(
    source: UpstreamSource,
    lake: LakeStore,
    targets_to_fetch: list[IngestTarget],
    staging_dir: Path,
    summary: UpdateSummary,
    fetched_by_file_id: dict[str, FetchedRawFile],
    workers: int,
) -> tuple[list[str], list[tuple[IngestTarget, str]]]:
    """Fetch files in parallel into staging. Returns (errors, mismatched)."""
    if not targets_to_fetch:
        return [], []
    errors: list[str] = []
    mismatched: list[tuple[IngestTarget, str]] = []
    # The folders that workers share are made here: threads creating one folder at the same moment can fail on Windows.
    for dataset_name in {target.dataset.name for target in targets_to_fetch}:
        (staging_dir / "raw" / dataset_name).mkdir(parents=True, exist_ok=True)
    with concurrent_futures.ThreadPoolExecutor(workers) as executor:
        target_by_future = {
            executor.submit(_fetch_one_target, source, lake, target, staging_dir): target for target in targets_to_fetch
        }
        for future in concurrent_futures.as_completed(target_by_future):
            target = target_by_future[future]
            try:
                fetched_file = future.result()
            except ListingMismatch as mismatch:
                mismatched.append((target, str(mismatch)))
                logger.warning("listing mismatch for %s: %s", target.file_id, mismatch)
            except Exception as error:  # noqa: BLE001 - whatever stops one file is reported and the others go on
                errors.append(f"{target.file_id}: {error}")
                logger.error("fetch failed for %s: %s", target.file_id, error)
            else:
                size_bytes = fetched_file.raw_metadata["raw_size"]
                fetched_by_file_id[target.file_id] = fetched_file
                summary.fetched_file_ids.append(target.file_id)
                summary.bytes_fetched += size_bytes
                logger.info("fetched %s (%.1f MB)", target.file_id, size_bytes / 1e6)
    return errors, mismatched


def _fetch_with_rechecks(
    source: UpstreamSource,
    lake: LakeStore,
    targets: list[IngestTarget],
    targets_to_fetch: list[IngestTarget],
    staging_dir: Path,
    summary: UpdateSummary,
    workers: int,
) -> tuple[list[IngestTarget], dict[str, FetchedRawFile]]:
    """Phase 1: download changed files, re-listing affected release tag(s) if a file was replaced mid-run."""
    megabytes = sum(target.asset.size_bytes for target in targets_to_fetch) / 1e6
    logger.info("fetching %d file(s) (%.1f MB)", len(targets_to_fetch), megabytes)
    fetched_by_file_id: dict[str, FetchedRawFile] = {}
    errors, mismatched = _fetch_targets(
        source, lake, targets_to_fetch, staging_dir, summary, fetched_by_file_id, workers
    )
    refreshed_by_file_id: dict[str, IngestTarget] = {}

    # A download that does not match the listing usually means upstream replaced the file after the listing was
    # made. Re-list only the affected release tag(s) and fetch what the new listing describes. Where the listing
    # has not changed, give it time to catch up before calling the download bad.
    for recheck in range(LISTING_RECHECKS):
        if errors or not mismatched:
            break
        if recheck:
            logger.info(
                "waiting %.0fs for listing to catch up (%d file(s))", source.listing_lag_seconds, len(mismatched)
            )
            time.sleep(source.listing_lag_seconds)
        fresh_by_tag = {
            release_tag: source.list_release_assets(release_tag)
            for release_tag in {target.asset.release_tag for target, _ in mismatched}
        }
        relisted: list[IngestTarget] = []
        unchanged: list[tuple[IngestTarget, str]] = []
        for target, mismatch in mismatched:
            fresh_asset = fresh_by_tag[target.asset.release_tag].get(target.asset.filename)
            if fresh_asset is not None and fresh_asset != target.asset:
                updated_target = dataclasses.replace(target, asset=fresh_asset)
                relisted.append(updated_target)
                refreshed_by_file_id[updated_target.file_id] = updated_target
            else:
                unchanged.append((target, mismatch))
        more_errors, mismatched_again = _fetch_targets(
            source, lake, relisted, staging_dir, summary, fetched_by_file_id, workers
        )
        errors += more_errors
        mismatched = unchanged + mismatched_again
        if not source.listing_lag_seconds:  # this source's listing is always current, so waiting would change nothing
            break

    errors += [f"{target.file_id}: {mismatch}" for target, mismatch in mismatched]
    if errors:
        raise UpdateFailed(sorted(errors))
    return [refreshed_by_file_id.get(target.file_id, target) for target in targets], fetched_by_file_id


def _build_one_target(
    lake: LakeStore,
    target: IngestTarget,
    fetched: FetchedRawFile | None,
    previous_clean_state: dict | None,
    staging_dir: Path,
    accept_schema_change: bool,
) -> StagedCleanFile:
    if fetched is not None:
        source_raw_path = fetched.staged_path
        raw_metadata = fetched.raw_metadata
        staged_raw_path = fetched.staged_path if target.dataset.history == "all" else None
    else:
        assert previous_clean_state is not None and previous_clean_state.get("raw_path")
        source_raw_path = lake.root_dir / previous_clean_state["raw_path"]
        raw_metadata = {field_name: previous_clean_state.get(field_name) for field_name in _RAW_METADATA_FIELDS}
        if target.dataset.history != "all":
            raw_metadata["raw_path"] = None
        staged_raw_path = None

    try:
        raw_frame = pl.read_parquet(source_raw_path)
    except UNREADABLE_PARQUET_ERRORS as error:
        if fetched is not None:  # it matched the listing, so fetching again would bring the same file
            advice = "the downloaded file cannot be read; skip the dataset with --skip until upstream replaces it"
        else:
            advice = f"raw file {source_raw_path} cannot be read; delete it and run update to fetch it again"
        raise ContractError(f"{advice} ({type(error).__name__})") from error

    # Drop temporary raw downloads for `history: latest` as soon as they are in memory to halve peak staging disk.
    if fetched is not None and target.dataset.history != "all":
        fetched.staged_path.unlink(missing_ok=True)

    build_result = build_clean(raw_frame, target.dataset, previous_clean_state, accept_schema_change)
    staged_clean_path = staging_dir / "clean" / target.dataset.name / target.asset.filename
    staged_clean_path.parent.mkdir(parents=True, exist_ok=True)
    build_result.clean_frame.write_parquet(staged_clean_path, compression="zstd")
    clean_sha256 = compute_file_sha256(staged_clean_path)

    staged_rejects_path = None
    if build_result.rejected_frame is not None:
        staged_rejects_path = staging_dir / "rejects" / target.dataset.name / target.asset.filename
        staged_rejects_path.parent.mkdir(parents=True, exist_ok=True)
        build_result.rejected_frame.write_parquet(staged_rejects_path, compression="zstd")

    # If upstream republished identical rows (for example, a new Parquet timestamp only), refresh upstream metadata
    # in clean_state.json without rewriting clean/ or storing a duplicate raw snapshot.
    clean_path = lake.clean_file_path(target.dataset.name, target.asset.filename)
    rejects_path = lake.rejects_file_path(target.dataset.name, target.asset.filename)
    rejects_unchanged = (
        previous_clean_state is not None
        and build_result.rejected_row_count == previous_clean_state["rejected_rows"]
        and (
            not rejects_path.exists()
            if staged_rejects_path is None
            else rejects_path.exists() and staged_rejects_path.read_bytes() == rejects_path.read_bytes()
        )
    )
    data_unchanged = (
        previous_clean_state is not None
        and clean_path.exists()
        and previous_clean_state["contract"] == target.dataset.contract
        and clean_sha256 == previous_clean_state["clean_sha256"]
        and rejects_unchanged
        and (target.dataset.history != "all" or _has_stored_raw(previous_clean_state, lake))
    )
    if data_unchanged:
        assert previous_clean_state is not None
        staged_clean_path.unlink(missing_ok=True)
        if staged_rejects_path is not None:
            staged_rejects_path.unlink(missing_ok=True)
        if staged_raw_path is not None:
            staged_raw_path.unlink(missing_ok=True)
        refreshed_entry = {
            **previous_clean_state,
            "raw_sha256": raw_metadata["raw_sha256"],
            "raw_size": raw_metadata["raw_size"],
            "upstream_updated_at": raw_metadata["upstream_updated_at"],
            "upstream_built_at": raw_metadata["upstream_built_at"],
        }
        return StagedCleanFile(target, None, None, None, refreshed_entry, [])

    # Only the facts are kept; holding every frame until publish would not fit in memory on a first run.
    clean_state_entry = {
        **raw_metadata,
        "dataset": target.dataset.name,
        "asset": target.asset.filename,
        "season": target.season,
        "clean_sha256": clean_sha256,
        "rows": build_result.clean_frame.height,
        "rows_in": build_result.input_row_count,
        "rejected_rows": build_result.rejected_row_count,
        "deduped_rows": build_result.deduped_row_count,
        "columns": build_result.column_schema,
        "contract": target.dataset.contract,
    }
    return StagedCleanFile(
        target, staged_clean_path, staged_rejects_path, staged_raw_path, clean_state_entry, build_result.notes
    )


def _build_staged_files(
    lake: LakeStore,
    targets: list[IngestTarget],
    fetched_by_file_id: dict[str, FetchedRawFile],
    clean_state: dict[str, dict],
    staging_dir: Path,
    accept_schema_change: bool,
) -> list[StagedCleanFile]:
    """Phase 2: rebuild every target that changed or has a new contract into staging."""
    staged_files: list[StagedCleanFile] = []
    errors: list[str] = []
    for target in targets:
        fetched = fetched_by_file_id.get(target.file_id)
        if fetched is None and not _needs_rebuild(target, clean_state, lake):
            continue
        try:
            staged_file = _build_one_target(
                lake, target, fetched, clean_state.get(target.file_id), staging_dir, accept_schema_change
            )
            staged_files.append(staged_file)
            if staged_file.staged_clean_path is None:
                logger.info("unchanged data %s (upstream metadata refreshed)", target.file_id)
            else:
                logger.info("built %s (%d rows)", target.file_id, staged_file.clean_state_entry["rows"])
        except ContractError as error:
            errors.append(f"{target.file_id}: {error}")
            logger.error("contract failed for %s: %s", target.file_id, error)
    if errors:
        raise UpdateFailed(errors)
    return staged_files


def _publish_staged_files(
    lake: LakeStore,
    staged_files: list[StagedCleanFile],
    clean_state: dict[str, dict],
    summary: UpdateSummary,
    published_at: str,
) -> None:
    """Phase 3: move staged files into raw/ (for `history: all`), rejects/, and clean/."""
    state_updated = False
    try:
        for staged_file in staged_files:
            target = staged_file.target
            if staged_file.staged_clean_path is None:
                clean_state[target.file_id] = staged_file.clean_state_entry
                state_updated = True
                continue
            if staged_file.staged_raw_path is not None:
                final_raw_path = lake.root_dir / staged_file.clean_state_entry["raw_path"]
                final_raw_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_file.staged_raw_path, final_raw_path)
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
            state_updated = True
            summary.rebuilt_file_ids.append(target.file_id)
            if staged_file.clean_state_entry["rejected_rows"]:
                summary.rejected_rows_by_file[target.file_id] = staged_file.clean_state_entry["rejected_rows"]
            if staged_file.clean_state_entry["deduped_rows"]:
                summary.deduped_rows_by_file[target.file_id] = staged_file.clean_state_entry["deduped_rows"]
            summary.notes += [f"{target.file_id}: {note}" for note in staged_file.notes]
    finally:
        if state_updated:  # also after a failure part-way, so clean/ and its record agree
            lake.save_clean_state(clean_state)


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
    targets, plan_errors = plan_ingest_targets(catalog, listing, clean_state, dataset_names, season_filter)
    summary.target_file_count = len(targets)
    summary.dataset_count = len({target.dataset.name for target in targets})

    if summary.dry_run:
        if plan_errors:
            raise UpdateFailed(plan_errors)
        targets_to_fetch = [target for target in targets if _needs_fetch(target, clean_state, lake)]
        summary.fetched_file_ids = sorted(target.file_id for target in targets_to_fetch)
        summary.rebuilt_file_ids = sorted(
            target.file_id for target in targets if _needs_rebuild(target, clean_state, lake)
        )
        summary.bytes_fetched = sum(target.asset.size_bytes for target in targets_to_fetch)
        summary.unchanged_file_count = len(targets) - len(summary.rebuilt_file_ids)
        return

    targets_by_dataset: dict[str, list[IngestTarget]] = {}
    for target in targets:
        targets_by_dataset.setdefault(target.dataset.name, []).append(target)

    all_errors = list(plan_errors)
    for dataset_name, dataset_targets in targets_by_dataset.items():
        dataset_to_fetch = [target for target in dataset_targets if _needs_fetch(target, clean_state, lake)]
        dataset_staging = staging_dir / dataset_name
        try:
            if dataset_to_fetch:
                dataset_targets, fetched_by_file_id = _fetch_with_rechecks(
                    source, lake, dataset_targets, dataset_to_fetch, dataset_staging, summary, workers
                )
            else:
                fetched_by_file_id = {}
            staged_files = _build_staged_files(
                lake, dataset_targets, fetched_by_file_id, clean_state, dataset_staging, accept_schema_change
            )
            if staged_files:
                _publish_staged_files(lake, staged_files, clean_state, summary, format_utc_iso(utc_now()))
        except UpdateFailed as dataset_failure:
            all_errors.extend(dataset_failure.errors)
        finally:
            shutil.rmtree(dataset_staging, ignore_errors=True)

    summary.fetched_file_ids.sort()
    summary.unchanged_file_count = len(targets) - len(summary.rebuilt_file_ids)
    if all_errors:
        raise UpdateFailed(all_errors, published_file_count=len(summary.rebuilt_file_ids))


def run_update(
    catalog: Catalog,
    source: UpstreamSource,
    lake: LakeStore,
    *,
    dataset_names: list[str] | None = None,
    season_filter: set[int] | None = None,
    accept_schema_change: bool = False,
    dry_run: bool = False,
    workers: int = 8,
) -> UpdateSummary:
    started_at = utc_now()
    summary = UpdateSummary(run_id=f"run_{started_at:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:4]}", dry_run=dry_run)
    staging_dir = lake.staging_dir / summary.run_id

    def execute() -> None:  # written once, so a dry run plans exactly what a real run does
        _execute_update(
            catalog, source, lake, summary, staging_dir, dataset_names, season_filter, accept_schema_change, workers
        )

    def record_run(run_status: str, errors: list[str]) -> None:
        lake.append_run_record(
            {
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
                "bytes_fetched": summary.bytes_fetched,
                "rejected_rows": sum(summary.rejected_rows_by_file.values()),
                "deduped_rows": sum(summary.deduped_rows_by_file.values()),
                "notes": summary.notes,
            }
        )

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
            if summary.rebuilt_file_ids:
                raise UpdateFailed([str(error)], published_file_count=len(summary.rebuilt_file_ids)) from error
            raise
        except Exception as error:
            errors = [f"{type(error).__name__}: {error}"]
            record_run("failed", errors)
            raise UpdateFailed(errors, published_file_count=len(summary.rebuilt_file_ids)) from error
        else:
            record_run("ok", [])
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
            with contextlib.suppress(OSError):
                lake.staging_dir.rmdir()  # only succeeds when empty
    return summary


def verify_lake(
    catalog: Catalog,
    lake: LakeStore,
    dataset_names: list[str] | None = None,
    deep: bool = False,
) -> VerificationResult:
    """Re-check every published clean file against its contract, and with `deep` re-hash clean files and every raw
    snapshot of the selected datasets."""
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
        elif published_entry["contract"] != dataset.contract:
            errors.append(f"{file_id}: built under an older contract; run update to rebuild it")
        else:
            try:
                key_frame = pl.read_parquet(clean_path, columns=dataset.key_columns)
            except UNREADABLE_PARQUET_ERRORS as error:
                errors.append(f"{file_id}: clean file cannot be read ({type(error).__name__})")
            else:
                if key_frame.height != published_entry["rows"]:
                    errors.append(
                        f"{file_id}: has {key_frame.height} rows but {published_entry['rows']} were published"
                    )
                violations = count_key_violations(key_frame, dataset.key_columns)
                if violations.null_key_rows or violations.duplicate_key_rows:
                    errors.append(
                        f"{file_id}: {violations.null_key_rows} row(s) with a null key, "
                        f"{violations.duplicate_key_rows} row(s) repeating a key"
                    )
            if deep and compute_file_sha256(clean_path) != published_entry["clean_sha256"]:
                errors.append(f"{file_id}: clean file no longer matches its recorded checksum")
        if deep and published_entry.get("raw_path") and not _has_stored_raw(published_entry, lake):
            errors.append(f"{file_id}: raw file {published_entry['raw_path']} is missing")
    if deep:  # a raw snapshot is named after its own checksum, so every one is checked, not only the newest
        for dataset_name in selected_datasets_by_name:
            for raw_path in sorted((lake.raw_dir / dataset_name).glob("*/*")):
                if not raw_path.stem.endswith(compute_file_sha256(raw_path)[:12]):
                    errors.append(
                        f"raw file {lake.relative_posix_path(raw_path)} no longer matches the checksum in its name"
                    )
    return VerificationResult(checked_file_count=checked_file_count, errors=errors)


def get_lake_status(catalog: Catalog, lake: LakeStore) -> LakeStatus:
    """Per-dataset totals of what is published, and the last run record.

    `season_span` is first-last, with a trailing * when a season inside that range is not published.
    """
    clean_state = lake.load_clean_state()
    entries_by_dataset: dict[str, list[dict]] = {}
    for entry in clean_state.values():
        entries_by_dataset.setdefault(entry["dataset"], []).append(entry)

    dataset_summaries: list[DatasetStatus] = []
    for dataset in catalog.select_datasets():
        dataset_entries = entries_by_dataset.get(dataset.name, [])
        seasons = sorted(entry["season"] for entry in dataset_entries if entry["season"] is not None)
        season_span = f"{seasons[0]}-{seasons[-1]}" if seasons else "-"
        if seasons and len(seasons) <= seasons[-1] - seasons[0]:
            season_span += "*"
        dataset_summaries.append(
            DatasetStatus(
                dataset_name=dataset.name,
                file_count=len(dataset_entries),
                season_span=season_span,
                row_count=sum(entry["rows"] for entry in dataset_entries),
                rejected_row_count=sum(entry["rejected_rows"] for entry in dataset_entries),
                latest_upstream_updated_at=max(
                    (entry["upstream_updated_at"] for entry in dataset_entries), default="-"
                ),
                latest_built_at=max((entry["built_at"] for entry in dataset_entries), default="-"),
            )
        )
    run_records = lake.load_run_records()
    return LakeStatus(datasets=dataset_summaries, last_run=(run_records[-1] if run_records else None))
