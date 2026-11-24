"""GitHubReleasesSource against canned responses. No test here touches the network.

These are the only tests that replace a library call: a rate limit, a server error and a dropped connection
cannot be produced on demand any other way.
"""

import hashlib
import http.client
import io
import json
import urllib.error
from email.message import Message

import pytest
import yaml

from nfl_lake import source as source_module
from nfl_lake.cli import main
from nfl_lake.source import FetchResult, GitHubReleasesSource, SourceError, UpstreamAsset

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


class NetworkGoneResponse(io.BytesIO):
    """The network drops mid-file and the socket raises a plain OSError, as Windows does for 'network is unreachable'."""

    def read(self, *args):
        raise OSError(10051, "A socket operation was attempted to an unreachable network")


class ResponseWithHeaders(io.BytesIO):
    """A response body carrying HTTP headers (e.g. Content-Length or ETag)."""

    def __init__(self, body: bytes, headers: dict[str, str]):
        super().__init__(body)
        self.headers = headers


class ResponseWithLength(ResponseWithHeaders):
    """A body that comes with a Content-Length header, as a real download does."""

    def __init__(self, body: bytes, content_length: int):
        super().__init__(body, {"Content-Length": str(content_length)})


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


def test_listing_caches_etags_and_reuses_cached_pages_on_304(monkeypatch, tmp_path):
    cache_path = tmp_path / "manifest" / "listing_cache.json"
    release_page = [make_release_payload("pbp", "play_by_play_2026.parquet")]

    def respond(attempt_number, request):
        if request.get_header("If-none-match") == '"etag-page-1"':
            raise make_http_error(304)
        return ResponseWithHeaders(json.dumps(release_page).encode(), {"ETag": '"etag-page-1"'})

    requests = stub_urlopen(monkeypatch, respond)

    source = GitHubReleasesSource("o/r", cache_path=cache_path)
    first_listing = source.list_assets()
    assert cache_path.is_file()
    assert requests[0].get_header("If-none-match") is None

    second_listing = source.list_assets()
    assert requests[1].get_header("If-none-match") == '"etag-page-1"'
    assert second_listing == first_listing

    dry_cache_path = tmp_path / "dry" / "listing_cache.json"
    dry_listing = GitHubReleasesSource("o/r", cache_path=dry_cache_path, write_cache=False).list_assets()
    assert dry_listing == first_listing and not dry_cache_path.exists()

    # A cache page that lost a field is not used, and a cache that cannot be written is not written.
    cache_path.write_text(json.dumps({requests[0].full_url: {"etag": '"etag-page-1"', "releases": [{"assets": []}]}}))
    assert source.list_assets() == first_listing and requests[-1].get_header("If-none-match") is None
    unwritable_cache_path = cache_path / "listing_cache.json"  # its folder is a file
    assert GitHubReleasesSource("o/r", cache_path=unwritable_cache_path).list_assets() == first_listing


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
        NetworkGoneResponse,
        lambda: io.BytesIO(SAMPLE_PARQUET_BYTES[:4]),
        lambda: ResponseWithLength(SAMPLE_PARQUET_BYTES[:4], content_length=10),
    ],
    ids=[
        "connection refused",
        "dropped mid-file",
        "incomplete read",
        "network gone mid-file",
        "body ends early",
        "shorter than its length",
    ],
)
def test_download_is_retried_and_sends_no_token(monkeypatch, tmp_path, first_response):
    requests = stub_urlopen(
        monkeypatch,
        lambda attempt_number, request: first_response() if attempt_number == 1 else io.BytesIO(SAMPLE_PARQUET_BYTES),
    )

    fetched = GitHubReleasesSource("o/r", token="t0ken", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")

    assert fetched == FetchResult(
        size_bytes=len(SAMPLE_PARQUET_BYTES), sha256=hashlib.sha256(SAMPLE_PARQUET_BYTES).hexdigest()
    )
    assert (tmp_path / "a.parquet").read_bytes() == SAMPLE_PARQUET_BYTES and len(requests) == 2
    assert all(request.get_header("Authorization") is None for request in requests)  # download host is not GitHub's API


def test_complete_download_of_another_size_than_listed_is_left_to_the_run(monkeypatch, tmp_path):
    # The listing can describe an earlier file. The body is whole by its own length, so nothing is retried here.
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: ResponseWithLength(b"PAR1", 4))

    fetched = GitHubReleasesSource("o/r", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")

    assert fetched == FetchResult(size_bytes=4, sha256=hashlib.sha256(b"PAR1").hexdigest())
    assert (tmp_path / "a.parquet").read_bytes() == b"PAR1" and len(requests) == 1


def test_download_to_a_path_that_cannot_be_opened_is_not_retried(monkeypatch, tmp_path):
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: io.BytesIO(SAMPLE_PARQUET_BYTES))
    (tmp_path / "a.parquet").mkdir()  # a folder where the file should go

    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "a.parquet")
    assert len(requests) == 1  # the disk's problem, so the download is not tried again


def test_download_to_a_folder_that_cannot_be_written_makes_no_request(monkeypatch, tmp_path):
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: io.BytesIO(SAMPLE_PARQUET_BYTES))

    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", backoff_seconds=0).fetch(SAMPLE_ASSET, tmp_path / "no_such_folder" / "a.parquet")
    assert requests == []  # a local problem, so nothing is downloaded or retried


@pytest.mark.parametrize(
    "body",
    [b"<html>sign in to the network</html>", b'[{"tag_name": "pbp", "assets": [{"na', b'{"message": "not a list"}'],
    ids=["a web page", "cut short", "another shape"],
)
def test_listing_reply_that_is_not_a_release_listing_is_tried_again_and_then_refused(monkeypatch, body):
    requests = stub_urlopen(monkeypatch, lambda attempt_number, request: io.BytesIO(body))
    with pytest.raises(SourceError):
        GitHubReleasesSource("o/r", max_attempts=2, backoff_seconds=0).list_assets()
    assert len(requests) == 2


def test_cli_lists_the_catalog_repo_on_github_with_the_token_when_no_source_folder_is_set(
    monkeypatch, tmp_path, capsys
):
    catalog_path = tmp_path / "datasets.yml"
    catalog_path.write_text(
        yaml.safe_dump(
            {
                "source": {"repo": "example/repo"},
                "datasets": {"pbp": {"release": "pbp", "file": "a.parquet", "key": ["play_id"]}},
            }
        )
    )
    monkeypatch.setenv("NFL_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.delenv("NFL_LAKE_SOURCE_DIR", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken\n")  # as read from a file: the newline is not part of the token
    requests = stub_urlopen(
        monkeypatch,
        lambda attempt_number, request: io.BytesIO(json.dumps([make_release_payload("pbp", "a.parquet")]).encode()),
    )

    main(["--catalog", str(catalog_path), "update", "--dry-run"])

    (request,) = requests
    assert "/repos/example/repo/releases" in request.full_url and request.get_header("Authorization") == "Bearer t0ken"

    monkeypatch.setenv("GITHUB_TOKEN", "s3cret\nvalue")  # not a token: refused before any request, and not printed
    with pytest.raises(SystemExit):
        main(["--catalog", str(catalog_path), "update", "--dry-run"])
    assert len(requests) == 1 and "s3cret" not in capsys.readouterr().err


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
    assert not (tmp_path / "a.parquet").exists()
