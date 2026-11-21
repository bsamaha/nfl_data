"""Where the files come from.

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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol, TypeVar

from .store import iso

USER_AGENT = "nfl-lake-ingest"
CHUNK = 1 << 20
T = TypeVar("T")


class SourceError(RuntimeError):
    """The upstream could not be listed, or a file could not be fetched intact."""


@dataclass(frozen=True)
class Asset:
    release: str
    name: str
    size: int
    updated_at: str        # ISO 8601 UTC, as upstream reports it
    sha256: str | None     # hex digest, when upstream publishes one
    url: str


Listing = dict[str, dict[str, Asset]]  # release -> file name -> asset


class Source(Protocol):
    def list_assets(self) -> Listing: ...

    def fetch(self, asset: Asset, dest: Path) -> None: ...


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


class GitHubReleasesSource:
    """All releases of one GitHub repository: one API call per 100 releases."""

    def __init__(self, repo: str, token: str | None = None, retries: int = 3, backoff: float = 2.0):
        self.repo = repo
        self.token = token
        self.retries = retries
        self.backoff = backoff

    def _retry(self, what: str, attempt: Callable[[], T]) -> T:
        """Run `attempt` up to `retries` times. A server error or a failed connection is retried; a rate limit or other status below 500 is not."""
        last: Exception | None = None
        for n in range(1, self.retries + 1):
            try:
                return attempt()
            except (OSError, http.client.HTTPException) as e:  # includes HTTP 5xx, timeouts and a dropped connection
                last = e
                if n < self.retries:
                    time.sleep(self.backoff * n)
        raise SourceError(f"failed after {self.retries} attempts while {what}: {last}") from last

    def _open(self, req: urllib.request.Request, what: str):
        """One request. A rate limit or any status below 500 is final, so it becomes a SourceError here."""
        try:
            return urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            if e.code in (403, 429) and e.headers.get("X-RateLimit-Remaining") == "0":
                reset = e.headers.get("X-RateLimit-Reset")
                when = datetime.fromtimestamp(int(reset), timezone.utc).strftime("%H:%M UTC") if reset else "later"
                raise SourceError(f"GitHub API rate limit reached while {what}. Retry after {when}, or set GITHUB_TOKEN.") from e
            if e.code < 500:
                raise SourceError(f"HTTP {e.code} while {what}") from e
            raise

    def list_assets(self) -> Listing:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        what = f"listing {self.repo} releases"
        listing: Listing = {}
        page = 1
        while True:
            url = f"https://api.github.com/repos/{self.repo}/releases?per_page=100&page={page}"

            def read_page() -> list[dict]:
                with self._open(urllib.request.Request(url, headers=headers), what) as r:
                    return json.load(r)

            batch = self._retry(what, read_page)
            for rel in batch:
                files = listing.setdefault(rel["tag_name"], {})
                for a in rel.get("assets", []):
                    digest = a.get("digest") or ""
                    files[a["name"]] = Asset(
                        release=rel["tag_name"],
                        name=a["name"],
                        size=int(a["size"]),
                        updated_at=a["updated_at"],
                        sha256=digest.split(":", 1)[1] if digest.startswith("sha256:") else None,
                        url=a["browser_download_url"],
                    )
            if len(batch) < 100:
                return listing
            page += 1

    def fetch(self, asset: Asset, dest: Path) -> None:
        # No token here: the download is public and redirects to another host.
        req = urllib.request.Request(asset.url, headers={"User-Agent": USER_AGENT})
        what = f"downloading {asset.name}"

        def download() -> None:
            with self._open(req, what) as r, open(dest, "wb") as f:
                shutil.copyfileobj(r, f, CHUNK)
            got = Path(dest).stat().st_size
            if got < asset.size:  # http.client returns a body that ended early without raising
                raise OSError(f"body ended after {got} of {asset.size} bytes")

        self._retry(what, download)


class LocalDirSource:
    """A folder of `<release>/<file>` used as the upstream."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()

    def list_assets(self) -> Listing:
        if not self.root.is_dir():
            raise SourceError(f"source folder {self.root} does not exist")
        listing: Listing = {}
        for rel in sorted(p for p in self.root.iterdir() if p.is_dir()):
            files = listing.setdefault(rel.name, {})
            for f in sorted(p for p in rel.iterdir() if p.is_file()):
                st = f.stat()
                updated = iso(datetime.fromtimestamp(st.st_mtime, timezone.utc))
                files[f.name] = Asset(rel.name, f.name, st.st_size, updated, sha256_file(f), f.as_uri())
        return listing

    def fetch(self, asset: Asset, dest: Path) -> None:
        shutil.copyfile(self.root / asset.release / asset.name, dest)
