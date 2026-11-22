"""Read nflverse Parquet files over HTTP without saving them: footers for schema, range reads for columns."""
import io, json, re, threading, urllib.request
from pathlib import Path
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

UA = {"User-Agent": "source-inventory"}
YEAR = re.compile(r"(?<!\d)(19\d\d|20\d\d)(?!\d)")


def load_listing(out_dir):
    rels = json.loads((Path(out_dir) / "releases.json").read_text())
    return {r["tag"]: r for r in rels}


NO_FILES = []  # "release: pattern" for each assets() call that matched nothing; a script that sees any exits non-zero


def assets(listing, tag, regex):
    rx = re.compile(regex)
    found = sorted((a for a in listing.get(tag, {}).get("assets", []) if rx.fullmatch(a["name"])), key=lambda a: a["name"])
    if not found:
        NO_FILES.append(f"{tag}: {regex}")
        print(f"NO FILES: release {tag} has no file matching {regex}")
    return found


def season_of(name):
    m = YEAR.search(name)
    return int(m.group(1)) if m else None


FETCHED = 0  # total bytes downloaded by this process, across all helpers
_FETCHED_LOCK = threading.Lock()  # the probes download from several threads


def _get(url, start, end):
    global FETCHED
    req = urllib.request.Request(url, headers={**UA, "Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        data = r.read()
        with _FETCHED_LOCK:
            FETCHED += len(data)
        return data, r.geturl()


def print_fetched():
    print(f"\nbytes fetched: {FETCHED / 1e6:.1f} MB")


def read_footer(url, size):
    """Return pyarrow FileMetaData using only the file's tail (64 KB first, then the exact footer if it is larger)."""
    n = min(size, 65_536)
    tail, _ = _get(url, size - n, size - 1)
    if tail[-4:] != b"PAR1":
        raise ValueError("not a parquet tail")
    flen = int.from_bytes(tail[-8:-4], "little")
    if flen + 8 > len(tail):
        tail, _ = _get(url, size - flen - 8, size - 1)
    return pq.read_metadata(pa.BufferReader(tail))


class RangeFile(io.RawIOBase):
    """Seekable read-only view of a remote file, fetched in cached blocks."""
    BLOCK = 1 << 20

    def __init__(self, url, size):
        self.url, self.size, self.pos, self.cache = url, size, 0, {}

    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos

    def _block(self, i):
        if i not in self.cache:
            s = i * self.BLOCK
            data, final = _get(self.url, s, min(s + self.BLOCK, self.size) - 1)
            self.url = final  # reuse the resolved location
            self.cache[i] = data
        return self.cache[i]

    def read(self, n=-1):
        if n < 0:
            n = self.size - self.pos
        n = max(0, min(n, self.size - self.pos))
        out, pos = bytearray(), self.pos
        while len(out) < n:
            i, o = divmod(pos, self.BLOCK)
            chunk = self._block(i)[o:o + (n - len(out))]
            if not chunk:
                break
            out += chunk
            pos += len(chunk)
        self.pos = pos
        return bytes(out)


def read_columns(asset, columns=None):
    """Read selected columns of one remote parquet asset into a polars DataFrame (in memory only)."""
    f = RangeFile(asset["url"], asset["size"])
    pf = pq.ParquetFile(f)
    have = set(pf.schema_arrow.names)
    cols = [c for c in columns if c in have] if columns else None
    tbl = pf.read(columns=cols)
    return pl.from_arrow(tbl)


def count_key_problems(df, key):
    """Return (rows with a null or empty-string key column, rows repeating a key among the remaining rows)."""
    k = df.select(key)
    k = k.with_columns(pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in key if k[c].dtype == pl.String)
    kept = k.drop_nulls()
    return k.height - kept.height, kept.height - kept.unique().height
