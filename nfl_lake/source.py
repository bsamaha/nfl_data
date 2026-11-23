"""Where upstream files come from.

`GitHubReleasesSource` reads the public nflverse-data releases. `LocalDirSource` reads a folder laid out
as `<release>/<file>`; the tests use it, and it also works as an offline mirror.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import shutil
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypeVar

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


@dataclass(frozen=True)
class UpstreamAsset:
    release_tag: str
    filename: str
    size_bytes: int
    updated_at: str  # ISO 8601 UTC, as upstream reports it
    sha256: str | None  # hex digest, when upstream publishes one
    download_url: str


UpstreamListing = dict[str, dict[str, UpstreamAsset]]  # release_tag -> filename -> asset


class UpstreamSource(Protocol):
    # How long a listing can go on describing a file after upstream has replaced it. 0 when a listing is always current.
    listing_lag_seconds: float

    def list_assets(self) -> UpstreamListing: ...

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> None: ...


def compute_file_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with open(file_path, "rb") as file_handle:
        while chunk := file_handle.read(DOWNLOAD_CHUNK_BYTES):
            hasher.update(chunk)
    return hasher.hexdigest()


class GitHubReleasesSource:
    """All releases of one GitHub repository: one API call per 100 releases."""

    # On 23 Nov 2026 the listing still described the previous schedules file over a minute after it was replaced.
    listing_lag_seconds = 60.0

    def __init__(self, repo: str, token: str | None = None, max_attempts: int = 3, backoff_seconds: float = 2.0):
        self.repo = repo
        self.token = token
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    def _retry_transient_errors(self, operation: str, action: Callable[[], ResultT]) -> ResultT:
        """Run `action` up to `max_attempts` times.

        Server errors and failed connections are retried; rate limits and 4xx are not.
        """
        last_error: Exception | None = None
        for attempt_number in range(1, self.max_attempts + 1):
            try:
                return action()
            except (OSError, http.client.HTTPException) as error:  # includes HTTP 5xx, timeouts and dropped connections
                last_error = error
                if attempt_number < self.max_attempts:
                    time.sleep(self.backoff_seconds * attempt_number)
        raise SourceError(f"failed after {self.max_attempts} attempts while {operation}: {last_error}") from last_error

    def _open_request(self, request: urllib.request.Request, operation: str):
        """Perform one HTTP request. A rate limit or any status below 500 is final, so it becomes a SourceError here."""
        try:
            return urllib.request.urlopen(request, timeout=120)
        except urllib.error.HTTPError as http_error:
            if http_error.code in (403, 429) and http_error.headers.get("X-RateLimit-Remaining") == "0":
                reset_epoch = http_error.headers.get("X-RateLimit-Reset")
                reset_time_utc = (
                    datetime.fromtimestamp(int(reset_epoch), UTC).strftime("%H:%M UTC") if reset_epoch else "later"
                )
                raise SourceError(
                    f"GitHub API rate limit reached while {operation}. Retry after {reset_time_utc}, or set GITHUB_TOKEN."
                ) from http_error
            if http_error.code < 500:
                raise SourceError(f"HTTP {http_error.code} while {operation}") from http_error
            raise

    def list_assets(self) -> UpstreamListing:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        operation = f"listing {self.repo} releases"
        listing: UpstreamListing = {}
        page = 1
        while True:
            page_url = f"https://api.github.com/repos/{self.repo}/releases?per_page=100&page={page}"

            def read_release_page(page_url: str = page_url) -> list[dict]:
                with self._open_request(urllib.request.Request(page_url, headers=headers), operation) as response:
                    return json.load(response)

            release_page = self._retry_transient_errors(operation, read_release_page)
            for release_payload in release_page:
                release_tag = release_payload["tag_name"]
                assets_by_filename = listing.setdefault(release_tag, {})
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
            if len(release_page) < 100:
                return listing
            page += 1

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> None:
        # No token here: the download is public and redirects to another host.
        request = urllib.request.Request(asset.download_url, headers={"User-Agent": USER_AGENT})
        operation = f"downloading {asset.filename}"
        try:
            Path(dest_path).touch()
        except OSError as error:  # a local problem, which trying the download again would not fix
            raise SourceError(f"cannot write {dest_path}: {error}") from error

        def download_once() -> None:
            with self._open_request(request, operation) as response, open(dest_path, "wb") as output_file:
                # What this response says it will send. The listing can describe an earlier file of another size.
                declared_length = getattr(response, "headers", {}).get("Content-Length")
                shutil.copyfileobj(response, output_file, DOWNLOAD_CHUNK_BYTES)
                received_bytes = output_file.tell()
            expected_bytes = int(declared_length) if declared_length else asset.size_bytes
            if received_bytes < expected_bytes:  # http.client returns a body that ended early without raising
                raise OSError(f"body ended after {received_bytes} of {expected_bytes} bytes")

        self._retry_transient_errors(operation, download_once)


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

    def fetch(self, asset: UpstreamAsset, dest_path: Path) -> None:
        shutil.copyfile(self.root_dir / asset.release_tag / asset.filename, dest_path)
