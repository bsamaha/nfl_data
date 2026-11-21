"""Read nflverse parquet files remotely without saving them: footers for schema, range reads for a few columns."""
import io, json, re, urllib.request
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

UA = {"User-Agent": "source-inventory"}
YEAR = re.compile(r"(?<!\d)(19\d\d|20\d\d)(?!\d)")


def load_listing(out_dir):
    rels = json.loads((Path(out_dir) / "releases.json").read_text())
    return {r["tag"]: r for r in rels}


def assets(listing, tag, regex):
    rx = re.compile(regex)
    return sorted((a for a in listing[tag]["assets"] if rx.fullmatch(a["name"])), key=lambda a: a["name"])


def season_of(name):
    m = YEAR.search(name)
    return int(m.group(1)) if m else None


FETCHED = 0  # total bytes downloaded by this process, across all helpers


def _get(url, start, end):
    global FETCHED
    req = urllib.request.Request(url, headers={**UA, "Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        data = r.read()
        FETCHED += len(data)
        return data, r.geturl()


def read_footer(url, size):
    """Return pyarrow FileMetaData using only the file's tail (64 KB first, then the exact footer if it is larger)."""
    n = min(size, 65_536)
    tail, _ = _get(url, size - n, size - 1)
    if tail[-4:] != b"PAR1":
        raise ValueError("not a parquet tail")
    flen = int.from_bytes(tail[-8:-4], "little")
    if flen + 8 > len(tail):
        tail, _ = _get(url, size - flen - 8, size - 1)
    return pq.read_metadata(pa.BufferReader(b"PAR1" + tail[-(flen + 8):]))


class RangeFile(io.RawIOBase):
    """Seekable read-only view of a remote file, fetched in cached blocks."""
    BLOCK = 1 << 20

    def __init__(self, url, size):
        self.url, self.size, self.pos, self.cache, self.fetched = url, size, 0, {}, 0

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
            self.fetched += len(data)
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

    def readinto(self, b):
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)


def read_columns(asset, columns=None):
    """Read selected columns of one remote parquet asset into a polars DataFrame (in memory only)."""
    import polars as pl
    f = RangeFile(asset["url"], asset["size"])
    pf = pq.ParquetFile(f)
    have = set(pf.schema_arrow.names)
    cols = [c for c in columns if c in have] if columns else None
    tbl = pf.read(columns=cols)
    return pl.from_arrow(tbl), f.fetched
