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
from nfl_lake.source import GitHubReleasesSource, SourceError, UpstreamAsset

SAMPLE_UPDATED_AT = "2026-11-21T16:46:06Z"
SAMPLE_ASSET = UpstreamAsset(
    release_tag="pbp",
    filename="a.parquet",
    size_bytes=10,
    updated_at=SAMPLE_UPDATED_AT,
    sha256=None,
    download_url="https://github.com/o/r/releases/download/pbp/a.parquet",
)
SAMPLE_PARQUET_BYTES = b"PAR1 bytes"  # the 10 bytes SAMPLE_ASSET lists


def make_release_payload(release_tag, *filenames, digest="sha256:" + "ab" * 32):
    return {
        "tag_name": release_tag,
        "assets": [
            {
                "name": filename,
                "size": 10,
                "updated_at": SAMPLE_UPDATED_AT,
                "digest": digest,
                "browser_download_url": f"https://github.com/o/r/releases/download/{release_tag}/{filename}",
            }
            for filename in filenames
        ],
    }


def make_http_error(http_status, headers=None):
    response_headers = Message()
    for header_name, header_value in (headers or {}).items():
        response_headers[header_name] = header_value
    return urllib.error.HTTPError("https://api.github.com/x", http_status, "error", response_headers, None)


def stub_urlopen(monkeypatch, respond) -> list:
    """Answer every HTTP request with respond(attempt_number, request), counting from 1. Returns requests made."""
    recorded_requests = []

    def fake_urlopen(request, timeout):
        recorded_requests.append(request)
        return respond(len(recorded_requests), request)

    monkeypatch.setattr(source_module.urllib.request, "urlopen", fake_urlopen)
    return recorded_requests


class DroppedMidFileResponse(io.BytesIO):
    def read(self, *args):
        raise ConnectionResetError("connection reset by peer")


class IncompleteReadResponse(io.BytesIO):
    def read(self, *args):
        raise http.client.IncompleteRead(b"")


class ResponseWithLength(io.BytesIO):
    """A body that comes with a Content-Length header, as a real download does."""

    def __init__(self, body: bytes, content_length: int):
        super().__init__(body)
        self.headers = {"Content-Length": str(content_length)}


def raise_connection_refused():
    raise urllib.error.URLError("connection refused")


def test_listing_pages_through_every_release_and_reads_the_checksum(monkeypatch):
    release_pages = {
        1: [make_release_payload(f"r{index}", "a.parquet") for index in range(100)],
        2: [
            make_release_payload("pbp", "play_by_play_2026.parquet"),
            make_release_payload("old", "b.parquet", digest=None),
        ],
    }
    requests = stub_urlopen(
        monkeypatch, lambda page_number, request: io.BytesIO(json.dumps(release_pages[page_number]).encode())
    )

    listing = GitHubReleasesSource("o/r", token="t0ken").list_assets()

    assert len(listing) == 102 and len(requests) == 2
    assert listing["pbp"]["play_by_play_2026.parquet"] == UpstreamAsset(
        release_tag="pbp",
        filename="play_by_play_2026.parquet",
        size_bytes=10,
        updated_at=SAMPLE_UPDATED_AT,
        sha256="ab" * 32,
        download_url="https://github.com/o/r/releases/download/pbp/play_by_play_2026.parquet",
    )
    assert listing["old"]["b.parquet"].sha256 is None  # no checksum published for this file
    assert requests[0].get_header("Authorization") == "Bearer t0ken"


def test_rate_limit_is_final(monkeypatch):
    def raise_rate_limited(attempt_number, request):
        raise make_http_error(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1795300000"})

    requests = stub_urlopen(monkeypatch, raise_rate_limited)
    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", backoff_seconds=0).list_assets()
    assert len(requests) == 1  # waiting a few seconds would not help


@pytest.mark.parametrize(
    "first_response",
    [
        raise_connection_refused,
        DroppedMidFileResponse,
        IncompleteReadResponse,
        lambda: io.BytesIO(SAMPLE_PARQUET_BYTES[:4]),
        lambda: ResponseWithLength(SAMPLE_PARQUET_BYTES[:4], content_length=10),
    ],
    ids=["connection refused", "dropped mid-file", "incomplete read", "body ends early", "shorter than its length"],
)
def test_download_is_retried_and_sends_no_token(monkeypatch, tmp_path, first_response):
    requests = stub_urlopen(
        monkeypatch,
        lambda attempt_number, request: first_response() if attempt_number == 1 else io.BytesIO(SAMPLE_PARQUET_BYTES),
    )

    GitHubReleasesSource("o/r", token="t0ken", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")

    assert (tmp_path / "a.parquet").read_bytes() == SAMPLE_PARQUET_BYTES and len(requests) == 2
    assert all(request.get_header("Authorization") is None for request in requests)  # download host is not GitHub's API


def test_complete_download_of_another_size_than_listed_is_left_to_the_run(monkeypatch, tmp_path):
    # The listing can describe an earlier file. The body is whole by its own length, so nothing is retried here.
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: ResponseWithLength(b"PAR1", 4))

    GitHubReleasesSource("o/r", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")

    assert (tmp_path / "a.parquet").read_bytes() == b"PAR1" and len(requests) == 1


def test_download_to_a_folder_that_cannot_be_written_makes_no_request(monkeypatch, tmp_path):
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: io.BytesIO(SAMPLE_PARQUET_BYTES))

    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "no_such_folder" / "a.parquet")
    assert requests == []  # a local problem, so nothing is downloaded or retried


def test_cli_lists_the_catalog_repo_on_github_with_the_token_when_no_source_folder_is_set(monkeypatch, tmp_path):
    catalog_path = tmp_path / "datasets.yml"
    catalog_path.write_text(yaml.safe_dump({
        "source": {"repo": "example/repo"},
        "datasets": {"pbp": {"release": "pbp", "file": "a.parquet", "key": ["play_id"]}},
    }))
    monkeypatch.setenv("NFL_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("NFL_LAKE_SOURCE_DIR", "")  # empty, not unset, so a developer's .env cannot fill it in
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    requests = stub_urlopen(
        monkeypatch,
        lambda attempt_number, request: io.BytesIO(json.dumps([make_release_payload("pbp", "a.parquet")]).encode()),
    )

    result = CliRunner().invoke(app, ["--catalog", str(catalog_path), "update", "--dry-run"])

    assert result.exit_code == 0, result.output
    (request,) = requests
    assert "/repos/example/repo/releases" in request.full_url and request.get_header("Authorization") == "Bearer t0ken"


@pytest.mark.parametrize(
    "http_status, expected_attempts",
    [(503, 2), (404, 1)],
    ids=["server error: every attempt", "not found: one"],
)
def test_download_gives_up(monkeypatch, tmp_path, http_status, expected_attempts):
    def raise_error(attempt_number, request):
        raise make_http_error(http_status)

    requests = stub_urlopen(monkeypatch, raise_error)
    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", max_attempts=2, backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")
    assert len(requests) == expected_attempts
