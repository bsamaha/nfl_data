"""Read nflverse Parquet files over HTTP without saving them: footers for schema, range reads for columns."""

import io
import json
import re
import sys
import threading
import urllib.request
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# The probes count key problems with the function ingest uses, so the two cannot disagree about what a
# duplicate is. The scripts are run by path, so the repo root is put on the import path here.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from nfl_lake.contract import count_key_violations  # noqa: F401

USER_AGENT_HEADERS = {"User-Agent": "source-inventory"}
SEASON_YEAR_PATTERN = re.compile(r"(?<!\d)(19\d\d|20\d\d)(?!\d)")


def load_releases_by_tag(output_dir):
    releases = json.loads((Path(output_dir) / "releases.json").read_text())
    return {release["tag"]: release for release in releases}


# "release: pattern" for each find_release_assets() call that matched nothing; callers exit non-zero if non-empty
UNMATCHED_ASSET_PATTERNS: list[str] = []


def find_release_assets(releases_by_tag, release_tag, filename_regex):
    compiled_pattern = re.compile(filename_regex)
    matched_assets = sorted(
        (
            asset
            for asset in releases_by_tag.get(release_tag, {}).get("assets", [])
            if compiled_pattern.fullmatch(asset["name"])
        ),
        key=lambda asset: asset["name"],
    )
    if not matched_assets:
        UNMATCHED_ASSET_PATTERNS.append(f"{release_tag}: {filename_regex}")
        print(f"NO FILES: release {release_tag} has no file matching {filename_regex}")
    return matched_assets


def extract_season_year(filename):
    year_match = SEASON_YEAR_PATTERN.search(filename)
    return int(year_match.group(1)) if year_match else None


TOTAL_BYTES_FETCHED = 0  # total bytes downloaded by this process, across all helpers
_BYTES_FETCHED_LOCK = threading.Lock()  # the probes download from several threads


def _fetch_byte_range(url, start_byte, end_byte):
    global TOTAL_BYTES_FETCHED
    request = urllib.request.Request(url, headers={**USER_AGENT_HEADERS, "Range": f"bytes={start_byte}-{end_byte}"})
    with urllib.request.urlopen(request, timeout=90) as response:
        payload = response.read()
        with _BYTES_FETCHED_LOCK:
            TOTAL_BYTES_FETCHED += len(payload)
        return payload, response.geturl()


def print_bytes_fetched():
    print(f"\nbytes fetched: {TOTAL_BYTES_FETCHED / 1e6:.1f} MB")


def read_parquet_footer(url, file_size_bytes):
    """Return pyarrow FileMetaData using only the file's tail (64 KB first, then the exact footer if it is larger)."""
    initial_tail_bytes = min(file_size_bytes, 65_536)
    tail_bytes, _ = _fetch_byte_range(url, file_size_bytes - initial_tail_bytes, file_size_bytes - 1)
    if tail_bytes[-4:] != b"PAR1":
        raise ValueError("not a parquet tail")
    footer_length = int.from_bytes(tail_bytes[-8:-4], "little")
    if footer_length + 8 > len(tail_bytes):
        tail_bytes, _ = _fetch_byte_range(url, file_size_bytes - footer_length - 8, file_size_bytes - 1)
    return pq.read_metadata(pa.BufferReader(tail_bytes))


class RemoteRangeFile(io.RawIOBase):
    """Seekable read-only view of a remote file, fetched in cached blocks."""

    BLOCK_SIZE_BYTES = 1 << 20

    def __init__(self, url, file_size_bytes):
        self.url = url
        self.file_size_bytes = file_size_bytes
        self.byte_offset = 0
        self.block_cache: dict[int, bytes] = {}

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.byte_offset

    def seek(self, offset, whence=0):
        if whence == 0:
            self.byte_offset = offset
        elif whence == 1:
            self.byte_offset += offset
        else:
            self.byte_offset = self.file_size_bytes + offset
        return self.byte_offset

    def _fetch_block(self, block_index):
        if block_index not in self.block_cache:
            block_start = block_index * self.BLOCK_SIZE_BYTES
            block_end = min(block_start + self.BLOCK_SIZE_BYTES, self.file_size_bytes) - 1
            block_data, resolved_url = _fetch_byte_range(self.url, block_start, block_end)
            self.url = resolved_url  # reuse the resolved redirect location
            self.block_cache[block_index] = block_data
        return self.block_cache[block_index]

    def read(self, num_bytes=-1):
        if num_bytes < 0:
            num_bytes = self.file_size_bytes - self.byte_offset
        num_bytes = max(0, min(num_bytes, self.file_size_bytes - self.byte_offset))
        buffer = bytearray()
        cursor = self.byte_offset
        while len(buffer) < num_bytes:
            block_index, offset_in_block = divmod(cursor, self.BLOCK_SIZE_BYTES)
            chunk = self._fetch_block(block_index)[offset_in_block : offset_in_block + (num_bytes - len(buffer))]
            if not chunk:
                break
            buffer += chunk
            cursor += len(chunk)
        self.byte_offset = cursor
        return bytes(buffer)


def read_remote_columns(asset, columns=None):
    """Read selected columns of one remote parquet asset into a polars DataFrame (in memory only)."""
    range_file = RemoteRangeFile(asset["url"], asset["size"])
    parquet_file = pq.ParquetFile(range_file)
    available_columns = set(parquet_file.schema_arrow.names)
    selected_columns = [col_name for col_name in columns if col_name in available_columns] if columns else None
    arrow_table = parquet_file.read(columns=selected_columns)
    return pl.from_arrow(arrow_table)
