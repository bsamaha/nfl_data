# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def remove_dir(path: str | Path) -> None:
    p = Path(path)
    if p.exists() and p.is_dir():
        # Use rmtree without importing shutil globally to keep dependencies minimal
        import shutil

        shutil.rmtree(p)


def move_replace(src: str | Path, dest: str | Path) -> None:
    import os
    import shutil

    src_p = Path(src)
    dest_p = Path(dest)
    dest_p.parent.mkdir(parents=True, exist_ok=True)
    if dest_p.exists():
        if dest_p.is_dir():
            shutil.rmtree(dest_p)
        else:
            dest_p.unlink()
    os.replace(src_p, dest_p)


def write_parquet_dataset(
    df: pd.DataFrame,
    root: str,
    dataset: str,
    layer: str,
    partitions: List[str],
    compression: str = "zstd",
    row_group_mb: int = 96,
    max_rows_per_file: Optional[int] = None,
    sort_by: Optional[List[str]] = None,
) -> None:
    target_root = Path(root) / layer / dataset
    ensure_dir(target_root)

    if sort_by:
        cols = [c for c in sort_by if c in df.columns]
        if cols:
            df = df.sort_values(by=cols, kind="mergesort")
    table = pa.Table.from_pandas(df, preserve_index=False)
    file_size = row_group_mb * 1024 * 1024

    # Configure Parquet writer options
    parquet_format = ds.ParquetFileFormat()
    file_options = parquet_format.make_write_options(compression=compression)

    # Ensure row-group setting does not violate Arrow constraint (group <= file)
    rows_per_group = None
    if max_rows_per_file is not None:
        rows_per_group = max_rows_per_file

    ds.write_dataset(
        table,
        base_dir=str(target_root),
        format=parquet_format,
        file_options=file_options,
        partitioning=partitions if partitions else None,
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        file_visitor=None,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=rows_per_group,
    )


def read_parquet_dataset(root: str, dataset: str, layer: str) -> pa.Table:
    path = Path(root) / layer / dataset
    return ds.dataset(str(path), format="parquet").to_table()

