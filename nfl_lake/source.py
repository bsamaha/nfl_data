"""Where the files come from.

`GitHubReleases` reads the public nflverse-data releases. `LocalDirSource` reads a folder laid out as
`<release>/<file>`; the tests use it, and it also works as an offline mirror.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

USER_AGENT = "nfl-lake-ingest"
CHUNK = 1 << 20


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


class GitHubReleases:
    """All releases of one GitHub repository, listed with a single paginated API call."""

    def __init__(self, repo: str, token: str | None = None, retries: int = 3, backoff: float = 2.0):
        self.repo, self.token, self.retries, self.backoff = repo, token, retries, backoff

    def _open(self, req: urllib.request.Request, what: str):
        last: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                return urllib.request.urlopen(req, timeout=120)
            except urllib.error.HTTPError as e:
                if e.code in (403, 429) and e.headers.get("X-RateLimit-Remaining") == "0":
                    reset = e.headers.get("X-RateLimit-Reset")
                    when = datetime.fromtimestamp(int(reset), timezone.utc).strftime("%H:%M UTC") if reset else "later"
                    raise SourceError(f"GitHub API rate limit reached while {what}. Retry after {when}, or set GITHUB_TOKEN.") from e
                if e.code < 500:
                    raise SourceError(f"HTTP {e.code} while {what}") from e
                last = e
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last = e
            if attempt < self.retries:
                time.sleep(self.backoff * attempt)
        raise SourceError(f"failed after {self.retries} attempts while {what}: {last}") from last

    def list_assets(self) -> Listing:
        listing: Listing = {}
        page = 1
        while True:
            url = f"https://api.github.com/repos/{self.repo}/releases?per_page=100&page={page}"
            headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            with self._open(urllib.request.Request(url, headers=headers), f"listing {self.repo} releases") as r:
                batch = json.load(r)
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
        last: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                with self._open(req, f"downloading {asset.name}") as r, open(dest, "wb") as f:
                    shutil.copyfileobj(r, f, CHUNK)
                return
            except SourceError:
                raise
            except (OSError, TimeoutError) as e:  # connection dropped mid-file
                last = e
                if attempt < self.retries:
                    time.sleep(self.backoff * attempt)
        raise SourceError(f"failed after {self.retries} attempts while downloading {asset.name}: {last}") from last


class LocalDirSource:
    """A folder of `<release>/<file>` used as the upstream."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def list_assets(self) -> Listing:
        if not self.root.is_dir():
            raise SourceError(f"source folder {self.root} does not exist")
        listing: Listing = {}
        for rel in sorted(p for p in self.root.iterdir() if p.is_dir()):
            files = listing.setdefault(rel.name, {})
            for f in sorted(p for p in rel.iterdir() if p.is_file()):
                st = f.stat()
                updated = datetime.fromtimestamp(st.st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                files[f.name] = Asset(rel.name, f.name, st.st_size, updated, sha256_file(f), f.as_uri())
        return listing

    def fetch(self, asset: Asset, dest: Path) -> None:
        shutil.copyfile(self.root / asset.release / asset.name, dest)
