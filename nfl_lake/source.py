"""Where upstream files come from.

`GitHubReleasesSource` reads the public nflverse-data releases. `LocalDirSource` reads a folder laid out
as `<release>/<file>`; the tests use it, and it also works as an offline mirror.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple, Protocol, TypeVar

from .store import format_utc_iso

USER_AGENT = "nfl-lake-ingest"
DOWNLOAD_CHUNK_BYTES = 1 << 20
ResultT = TypeVar("ResultT")


class SourceError(RuntimeError):
    """The upstream could not be listed, or a file could not be fetched intact."""


class ListingMismatch(SourceError):
    """The file that was fetched is not the one the listing describes: its size or checksum differs.

    The usual cause is that upstream replaced the file after the listing was made, so the run lists again
    before it gives up.
    """


class _Transient(Exception):
    """A failure of the network or the server, which another attempt may get past."""


class _NotModified(Exception):
    """Raised when a conditional request with `If-None-Match` returns HTTP 304."""


# What reading a release listing raises when it is not one: text that is not JSON, or JSON of another shape.
_NOT_A_LISTING = (ValueError, LookupError, TypeError, AttributeError)


@contextlib.contextmanager
def _network() -> Iterator[None]:
    """Wrap code that only talks to the network: any OS or HTTP error raised inside is transient.

    A failure is classed by where it happens, not by its type. A dropped network can surface as a plain OSError
    or a PermissionError, which by type alone would look like a problem with the local file.
    """
    try:
        yield
    except (OSError, http.client.HTTPException) as error:
        raise _Transient(str(error) or type(error).__name__) from error


@dataclass(frozen=True)
class UpstreamAsset:
    release_tag: str
    filename: str
    size_bytes: int
    updated_at: str  # ISO 8601 UTC, as upstream reports it
    sha256: str | None  # hex digest, when upstream publishes one
    download_url: str


class FetchResult(NamedTuple):
    size_bytes: int
    sha256: str


UpstreamListing = dict[str, dict[str, UpstreamAsset]]  # release_tag -> filename -> asset


class UpstreamSource(Protocol):
    # How long a listing can go on describing a file after upstream has replaced it. 0 when a listing is always current.
    listing_lag_seconds: float

    def list_assets(self) -> UpstreamListing: ...

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> FetchResult: ...


def compute_file_sha256(file_path: Path) -> str:
    with open(file_path, "rb") as file_handle:
        return hashlib.file_digest(file_handle, "sha256").hexdigest()


def _format_rate_limit_reset(reset_epoch: str | None) -> str:
    if not reset_epoch:
        return "later"
    try:
        return datetime.fromtimestamp(int(reset_epoch), UTC).strftime("%H:%M UTC")
    except ValueError, OverflowError, OSError:
        return "later"


def _parse_content_length(raw_length: str | None, fallback_bytes: int) -> int:
    if raw_length is None:
        return fallback_bytes
    try:
        parsed_length = int(raw_length)
    except ValueError:
        return fallback_bytes
    return parsed_length if parsed_length > 0 else fallback_bytes


def _slim_release_payload(release_payload: dict) -> dict:
    return {
        "tag_name": release_payload["tag_name"],
        "assets": [
            {
                "name": asset_payload["name"],
                "size": int(asset_payload["size"]),
                "updated_at": asset_payload["updated_at"],
                "digest": asset_payload.get("digest"),
                "browser_download_url": asset_payload["browser_download_url"],
            }
            for asset_payload in release_payload.get("assets", [])
        ],
    }


def _parse_release_assets(release_tag: str, release_payload: dict) -> dict[str, UpstreamAsset]:
    assets_by_filename: dict[str, UpstreamAsset] = {}
    for asset_payload in release_payload.get("assets", []):
        raw_digest = asset_payload.get("digest") or ""
        assets_by_filename[asset_payload["name"]] = UpstreamAsset(
            release_tag=release_tag,
            filename=asset_payload["name"],
            size_bytes=int(asset_payload["size"]),
            updated_at=asset_payload["updated_at"],
            sha256=raw_digest.split(":", 1)[1] if raw_digest.startswith("sha256:") else None,
            download_url=asset_payload["browser_download_url"],
        )
    return assets_by_filename


class GitHubReleasesSource:
    """All releases of one GitHub repository: one API call per 100 releases, with optional ETag caching."""

    # On 23 Nov 2026 the listing still described the previous schedules file over a minute after it was replaced.
    listing_lag_seconds = 60.0

    def __init__(
        self,
        repo: str,
        token: str | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 2.0,
        cache_path: Path | None = None,
        write_cache: bool = True,
    ):
        self.repo = repo
        self.token = token
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.cache_path = Path(cache_path) if cache_path is not None else None
        self.write_cache = write_cache

    def _api_headers(self, etag: str | None = None) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if etag:
            headers["If-None-Match"] = etag
        return headers

    # The cache only saves a download, so no fault in it may fail a run: a file that cannot be read, or that is not
    # in the form written below, counts as no cache, and a cache that cannot be written is not written.
    def _load_etag_cache(self) -> dict[str, dict]:
        """Release pages by URL, each {"etag": ..., "releases": [...]}."""
        if self.cache_path is None:
            return {}
        try:
            cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
            for cached_page in cache.values():
                if not isinstance(cached_page["etag"], str) or not isinstance(cached_page["releases"], list):
                    raise TypeError
                for release_payload in cached_page["releases"]:
                    _parse_release_assets(release_payload["tag_name"], release_payload)
        except (OSError, *_NOT_A_LISTING):
            return {}
        return cache

    def _save_etag_cache(self, cache: dict[str, dict]) -> None:
        if self.cache_path is None or not self.write_cache:
            return
        with contextlib.suppress(OSError):
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.cache_path.with_suffix(".json.tmp")
            temp_path.write_text(json.dumps(cache, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.cache_path)

    def _retry_transient_errors(self, operation: str, action: Callable[[], ResultT]) -> ResultT:
        """Run `action` up to `max_attempts` times.

        Server errors and failed connections are retried. Rate limits, other statuses below 500 and local disk
        errors are not.
        """
        last_error: Exception | None = None
        for attempt_number in range(1, self.max_attempts + 1):
            try:
                return action()
            except _Transient as error:
                last_error = error
                if attempt_number < self.max_attempts:
                    time.sleep(self.backoff_seconds * attempt_number)
        raise SourceError(f"failed after {self.max_attempts} attempts while {operation}: {last_error}") from last_error

    def _open_request(self, request: urllib.request.Request, operation: str, *, allow_not_modified: bool = False):
        """Perform one HTTP request. A rate limit or any status below 500 is final, so it becomes a SourceError here."""
        with _network():
            try:
                return urllib.request.urlopen(request, timeout=120)
            except urllib.error.HTTPError as http_error:
                headers = http_error.headers or {}
                http_code = http_error.code
                if http_error.fp is not None:
                    http_error.close()
                if allow_not_modified and http_code == 304:
                    raise _NotModified from http_error
                if http_code in (403, 429) and headers.get("X-RateLimit-Remaining") == "0":
                    reset_time_utc = _format_rate_limit_reset(headers.get("X-RateLimit-Reset"))
                    raise SourceError(
                        f"GitHub API rate limit reached while {operation}. "
                        f"Retry after {reset_time_utc}, or set GITHUB_TOKEN."
                    ) from http_error
                if http_code < 500:
                    raise SourceError(f"HTTP {http_code} while {operation}") from http_error
                raise  # 5xx propagates into _network(), which converts it to _Transient

    def list_assets(self) -> UpstreamListing:
        operation = f"listing {self.repo} releases"
        cached_pages = self._load_etag_cache()
        updated_cache: dict[str, dict] = {}
        listing: UpstreamListing = {}
        page = 1
        while True:
            page_url = f"https://api.github.com/repos/{self.repo}/releases?per_page=100&page={page}"
            cached_page = cached_pages.get(page_url)
            cached_etag = cached_page["etag"] if cached_page else None
            headers = self._api_headers(etag=cached_etag)

            def read_release_page(
                page_url: str = page_url,
                headers: dict[str, str] = headers,
                cached_page: dict | None = cached_page,
                cached_etag: str | None = cached_etag,
            ) -> tuple[list[dict], str | None]:
                page_request = urllib.request.Request(page_url, headers=headers)
                try:
                    with self._open_request(
                        page_request, operation, allow_not_modified=cached_etag is not None
                    ) as response:
                        try:
                            with _network():
                                payload = json.load(response)
                            if not isinstance(payload, list):
                                raise TypeError("expected a list of releases")
                            releases = [_slim_release_payload(release_payload) for release_payload in payload]
                        except _NOT_A_LISTING as error:  # a body cut short, or a page from something in between
                            raise _Transient(f"the reply is not a release listing ({type(error).__name__})") from error
                        return releases, getattr(response, "headers", {}).get("ETag")
                except _NotModified:
                    assert cached_page is not None
                    return cached_page["releases"], cached_etag

            release_page, response_etag = self._retry_transient_errors(operation, read_release_page)
            if response_etag:
                updated_cache[page_url] = {"etag": response_etag, "releases": release_page}
            for release_payload in release_page:
                release_tag = release_payload["tag_name"]
                listing.setdefault(release_tag, {}).update(_parse_release_assets(release_tag, release_payload))
            if len(release_page) < 100:
                if updated_cache != cached_pages and updated_cache:
                    self._save_etag_cache(updated_cache)
                return listing
            page += 1

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> FetchResult:
        dest_path = Path(dest_path)
        if not dest_path.parent.is_dir():
            raise SourceError(f"cannot write {dest_path}: folder {dest_path.parent} does not exist")
        # No token here: the download is public and redirects to another host.
        request = urllib.request.Request(asset.download_url, headers={"User-Agent": USER_AGENT})
        operation = f"downloading {asset.filename}"

        def download_once() -> FetchResult:
            hasher = hashlib.sha256()
            with self._open_request(request, operation) as response, open(dest_path, "wb") as output_file:
                # What this response says it will send. The listing can describe an earlier file of another size.
                declared_length = getattr(response, "headers", {}).get("Content-Length")
                received_bytes = 0
                while True:
                    with _network():
                        chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    output_file.write(chunk)
                    hasher.update(chunk)
                    received_bytes += len(chunk)
            expected_bytes = _parse_content_length(declared_length, asset.size_bytes)
            if received_bytes < expected_bytes:  # http.client returns a body that ended early without raising
                raise _Transient(f"body ended after {received_bytes} of {expected_bytes} bytes")
            return FetchResult(size_bytes=received_bytes, sha256=hasher.hexdigest())

        try:
            return self._retry_transient_errors(operation, download_once)
        except OSError as error:  # only opening or writing the destination can raise one: the network raises _Transient
            with contextlib.suppress(OSError):
                dest_path.unlink()
            raise SourceError(f"cannot write {dest_path}: {error}") from error
        except SourceError:
            with contextlib.suppress(OSError):
                dest_path.unlink()
            raise


class LocalDirSource:
    """A folder of `<release>/<file>` used as the upstream."""

    listing_lag_seconds = 0.0

    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir).resolve()

    def list_assets(self) -> UpstreamListing:
        if not self.root_dir.is_dir():
            raise SourceError(f"source folder {self.root_dir} does not exist")
        listing: UpstreamListing = {}
        for release_dir in sorted(path for path in self.root_dir.iterdir() if path.is_dir()):
            assets_by_filename = listing.setdefault(release_dir.name, {})
            for file_path in sorted(path for path in release_dir.iterdir() if path.is_file()):
                file_stat = file_path.stat()
                updated_at = format_utc_iso(datetime.fromtimestamp(file_stat.st_mtime, UTC))
                assets_by_filename[file_path.name] = UpstreamAsset(
                    release_tag=release_dir.name,
                    filename=file_path.name,
                    size_bytes=file_stat.st_size,
                    updated_at=updated_at,
                    sha256=compute_file_sha256(file_path),
                    download_url=file_path.as_uri(),
                )
        return listing

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> FetchResult:
        src_path = self.root_dir / asset.release_tag / asset.filename
        dest_path = Path(dest_path)
        hasher = hashlib.sha256()
        copied_bytes = 0
        try:
            with open(src_path, "rb") as src_handle, open(dest_path, "wb") as dest_handle:
                while chunk := src_handle.read(DOWNLOAD_CHUNK_BYTES):
                    dest_handle.write(chunk)
                    hasher.update(chunk)
                    copied_bytes += len(chunk)
        except OSError as error:
            with contextlib.suppress(OSError):
                dest_path.unlink()
            raise SourceError(f"cannot copy {src_path} to {dest_path}: {error}") from error
        return FetchResult(size_bytes=copied_bytes, sha256=hasher.hexdigest())
