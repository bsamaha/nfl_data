"""GitHubReleasesSource against canned responses. No test here touches the network.

These are the only tests that replace a library call: a rate limit, a server error and a dropped connection
cannot be produced on demand any other way.
"""
import http.client
import io
import json
import urllib.error
from email.message import Message

import pytest
import yaml
from typer.testing import CliRunner

from nfl_lake import source as source_module
from nfl_lake.cli import app
from nfl_lake.source import Asset, GitHubReleasesSource, SourceError

UPDATED = "2026-11-21T16:46:06Z"
ASSET = Asset("pbp", "a.parquet", 10, UPDATED, None, "https://github.com/o/r/releases/download/pbp/a.parquet")
BODY = b"PAR1 bytes"  # the 10 bytes ASSET lists


def release(tag, *names, digest="sha256:" + "ab" * 32):
    return {"tag_name": tag, "assets": [
        {"name": n, "size": 10, "updated_at": UPDATED, "digest": digest,
         "browser_download_url": f"https://github.com/o/r/releases/download/{tag}/{n}"} for n in names]}


def http_error(code, headers=None):
    msg = Message()
    for k, v in (headers or {}).items():
        msg[k] = v
    return urllib.error.HTTPError("https://api.github.com/x", code, "error", msg, None)


def answer_with(monkeypatch, respond) -> list:
    """Answer every HTTP request with respond(n, request), n counting from 1. Returns the list of requests made."""
    requests = []

    def urlopen(request, timeout):
        requests.append(request)
        return respond(len(requests), request)

    monkeypatch.setattr(source_module.urllib.request, "urlopen", urlopen)
    return requests


class DroppedMidFile(io.BytesIO):
    def read(self, *args):
        raise ConnectionResetError("connection reset by peer")


class CutShort(io.BytesIO):
    def read(self, *args):
        raise http.client.IncompleteRead(b"")


def refuse_connection():
    raise urllib.error.URLError("connection refused")


def test_listing_pages_through_every_release_and_reads_the_checksum(monkeypatch):
    pages = {1: [release(f"r{i}", "a.parquet") for i in range(100)],
             2: [release("pbp", "play_by_play_2026.parquet"), release("old", "b.parquet", digest=None)]}
    requests = answer_with(monkeypatch, lambda n, request: io.BytesIO(json.dumps(pages[n]).encode()))

    listing = GitHubReleasesSource("o/r", token="t0ken").list_assets()

    assert len(listing) == 102 and len(requests) == 2
    assert listing["pbp"]["play_by_play_2026.parquet"] == Asset(
        "pbp", "play_by_play_2026.parquet", 10, UPDATED, "ab" * 32,
        "https://github.com/o/r/releases/download/pbp/play_by_play_2026.parquet")
    assert listing["old"]["b.parquet"].sha256 is None  # no checksum published for this file
    assert requests[0].get_header("Authorization") == "Bearer t0ken"


def test_rate_limit_is_final(monkeypatch):
    def limited(n, request):
        raise http_error(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1795300000"})

    requests = answer_with(monkeypatch, limited)
    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", backoff=0).list_assets()
    assert len(requests) == 1  # waiting a few seconds would not help


@pytest.mark.parametrize("first_answer", [refuse_connection, DroppedMidFile, CutShort, lambda: io.BytesIO(BODY[:4])],
                         ids=["connection refused", "dropped mid-file", "incomplete read", "body ends early"])
def test_download_is_retried_and_sends_no_token(monkeypatch, tmp_path, first_answer):
    requests = answer_with(monkeypatch, lambda n, request: first_answer() if n == 1 else io.BytesIO(BODY))

    GitHubReleasesSource("o/r", token="t0ken", backoff=0).fetch(ASSET, tmp_path / "a.parquet")

    assert (tmp_path / "a.parquet").read_bytes() == BODY and len(requests) == 2
    assert all(request.get_header("Authorization") is None for request in requests)  # the download host is not GitHub's API


def test_cli_lists_the_catalog_repo_on_github_with_the_token_when_no_source_folder_is_set(monkeypatch, tmp_path):
    catalog = tmp_path / "datasets.yml"
    catalog.write_text(yaml.safe_dump({"source": {"repo": "example/repo"},
                                       "datasets": {"pbp": {"release": "pbp", "file": "a.parquet", "key": ["play_id"]}}}))
    monkeypatch.setenv("NFL_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", "")  # empty, not unset, so a developer's .env cannot fill it in
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    requests = answer_with(monkeypatch, lambda n, request: io.BytesIO(json.dumps([release("pbp", "a.parquet")]).encode()))

    result = CliRunner().invoke(app, ["--catalog", str(catalog), "update", "--dry-run"])

    assert result.exit_code == 0, result.output
    (request,) = requests
    assert "/repos/example/repo/releases" in request.full_url and request.get_header("Authorization") == "Bearer t0ken"


@pytest.mark.parametrize("status, attempts", [(503, 2), (404, 1)], ids=["server error: every attempt", "not found: one"])
def test_download_gives_up(monkeypatch, tmp_path, status, attempts):
    def fail(n, request):
        raise http_error(status)

    requests = answer_with(monkeypatch, fail)
    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", retries=2, backoff=0).fetch(ASSET, tmp_path / "a.parquet")
    assert len(requests) == attempts
