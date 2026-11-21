"""GitHubReleases against canned responses. No test here touches the network."""
import io
import json
import urllib.error
from email.message import Message

import pytest

from nfl_lake import source as source_module
from nfl_lake.source import Asset, GitHubReleases, SourceError


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def release(tag, *names, digest="sha256:" + "ab" * 32):
    return {"tag_name": tag, "assets": [
        {"name": n, "size": 10, "updated_at": "2026-11-21T16:46:06Z", "digest": digest,
         "browser_download_url": f"https://github.com/o/r/releases/download/{tag}/{n}"} for n in names]}


def http_error(code, headers=None):
    msg = Message()
    for k, v in (headers or {}).items():
        msg[k] = v
    return urllib.error.HTTPError("https://api.github.com/x", code, "error", msg, None)


def test_listing_pages_through_every_release_and_reads_the_checksum(monkeypatch):
    pages = {1: [release(f"r{i}", "a.parquet") for i in range(100)], 2: [release("pbp", "play_by_play_2026.parquet", "x.csv")]}
    seen = []

    def urlopen(req, timeout):
        seen.append(req)
        page = int(req.full_url.rsplit("page=", 1)[1])
        return Response(json.dumps(pages[page]).encode())

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    listing = GitHubReleases("o/r", token="t0ken").list_assets()

    assert len(listing) == 101 and len(seen) == 2
    assert listing["pbp"]["play_by_play_2026.parquet"] == Asset(
        "pbp", "play_by_play_2026.parquet", 10, "2026-11-21T16:46:06Z", "ab" * 32,
        "https://github.com/o/r/releases/download/pbp/play_by_play_2026.parquet")
    assert seen[0].get_header("Authorization") == "Bearer t0ken"


def test_file_without_a_published_checksum_has_none(monkeypatch):
    monkeypatch.setattr(source_module.urllib.request, "urlopen",
                        lambda req, timeout: Response(json.dumps([release("pbp", "a.parquet", digest=None)]).encode()))
    assert GitHubReleases("o/r").list_assets()["pbp"]["a.parquet"].sha256 is None


def test_rate_limit_is_reported_as_such(monkeypatch):
    def urlopen(req, timeout):
        raise http_error(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1795300000"})

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    with pytest.raises(SourceError, match="rate limit reached.*GITHUB_TOKEN"):
        GitHubReleases("o/r").list_assets()


def test_download_retries_a_dropped_connection_and_sends_no_token(monkeypatch, tmp_path):
    calls = []

    def urlopen(req, timeout):
        calls.append(req)
        if len(calls) == 1:
            raise urllib.error.URLError("connection reset")
        return Response(b"PAR1 bytes")

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    asset = Asset("pbp", "a.parquet", 10, "2026-11-21T16:46:06Z", None, "https://github.com/o/r/releases/download/pbp/a.parquet")
    GitHubReleases("o/r", token="t0ken", backoff=0).fetch(asset, tmp_path / "a.parquet")

    assert (tmp_path / "a.parquet").read_bytes() == b"PAR1 bytes" and len(calls) == 2
    assert all(c.get_header("Authorization") is None for c in calls)


def test_download_gives_up_with_a_clear_error(monkeypatch, tmp_path):
    def urlopen(req, timeout):
        raise http_error(404)

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    asset = Asset("pbp", "a.parquet", 10, "t", None, "https://github.com/o/r/releases/download/pbp/a.parquet")
    with pytest.raises(SourceError, match="HTTP 404 while downloading a.parquet"):
        GitHubReleases("o/r", backoff=0).fetch(asset, tmp_path / "a.parquet")
