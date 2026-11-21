"""scripts/source_inventory/nfl_remote.py, with the HTTP range request served from a Parquet file held in memory."""
import io

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import nfl_remote

TAIL = 65_536  # bytes read_footer asks for first


def serve(monkeypatch, table):
    """Point nfl_remote._get at an in-memory copy of the table. Returns the asset record and the ranges asked for."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="none", use_dictionary=False)
    data, ranges = buf.getvalue(), []

    def fake_get(url, start, end):
        ranges.append((start, end))
        return data[start:end + 1], url

    monkeypatch.setattr(nfl_remote, "_get", fake_get)
    return {"url": "memory://file.parquet", "size": len(data)}, ranges


ROWS = 60_000  # enough for the file to outgrow both the first tail read and one RangeFile block


def long_table():
    return pa.table({"a": list(range(ROWS)), "b": [i % 7 for i in range(ROWS)], "c": [f"p{i}" for i in range(ROWS)]})


def test_read_footer_when_the_footer_fits_in_the_first_tail_read(monkeypatch):
    asset, ranges = serve(monkeypatch, long_table())
    assert asset["size"] > TAIL
    md = nfl_remote.read_footer(asset["url"], asset["size"])
    assert (md.num_rows, md.schema.to_arrow_schema().names) == (ROWS, ["a", "b", "c"])
    assert ranges == [(asset["size"] - TAIL, asset["size"] - 1)]


def test_read_footer_when_the_footer_is_larger_than_the_first_tail_read(monkeypatch):
    asset, ranges = serve(monkeypatch, pa.table({f"column_{i:04d}": [i] for i in range(1500)}))
    md = nfl_remote.read_footer(asset["url"], asset["size"])
    assert (md.num_rows, md.num_columns) == (1, 1500)
    assert md.serialized_size + 8 > TAIL
    assert ranges[1:] == [(asset["size"] - md.serialized_size - 8, asset["size"] - 1)]


def test_read_columns_reads_a_subset_or_the_whole_file(monkeypatch):
    table = long_table()
    asset, _ = serve(monkeypatch, table)
    assert asset["size"] > nfl_remote.RangeFile.BLOCK  # the read crosses a block boundary
    whole = pl.from_arrow(table)
    assert nfl_remote.read_columns(asset).equals(whole)
    assert nfl_remote.read_columns(asset, ["a", "c"]).equals(whole.select("a", "c"))
    # a name the file does not have is left out, so a caller that needs the column fails when it uses it
    assert nfl_remote.read_columns(asset, ["a", "renamed"]).columns == ["a"]


def test_count_key_problems_sets_null_and_empty_keys_aside_before_counting_repeats():
    df = pl.DataFrame({
        "game_id": ["g1", "g1", "g1", "g1", "g2", "g2"],
        "player_id": ["p1", "p1", None, None, "", "p2"],
    })
    # two null ids and one empty id are set aside; of the three rows left, one repeats (g1, p1)
    assert nfl_remote.count_key_problems(df, ["game_id", "player_id"]) == (3, 1)
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        nfl_remote.count_key_problems(df, ["game_id", "renamed"])
