"""What a core build changes: a staged table compared with the published one, row by row on its key.

The result is for a person to read before trusting a build. It blocks nothing; the checks do that.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .checks import sql_path


@dataclass
class TableChange:
    table_name: str
    rows_before: int = 0
    rows_after: int = 0
    added_rows: int = 0
    removed_rows: int = 0
    changed_rows: int = 0  # rows in both whose value differs in a column both have
    changed_columns: dict[str, int] = field(default_factory=dict)  # column -> rows that differ in it
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    retyped_columns: list[str] = field(default_factory=list)

    @property
    def unchanged(self) -> bool:
        return not (
            self.added_rows
            or self.removed_rows
            or self.changed_rows
            or self.added_columns
            or self.removed_columns
            or self.retyped_columns
        )


def _count_rows(connection: duckdb.DuckDBPyConnection, parquet_path: Path) -> int:
    return int(connection.sql(f"SELECT count(*) FROM read_parquet('{sql_path(parquet_path)}')").fetchall()[0][0])


def _column_types(connection: duckdb.DuckDBPyConnection, parquet_path: Path) -> dict[str, str]:
    described = connection.sql(f"DESCRIBE SELECT * FROM read_parquet('{sql_path(parquet_path)}')").fetchall()
    return {column_name: column_type for column_name, column_type, *_ in described}


def compare_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    key_columns: Sequence[str],
    published_files: Sequence[Path],
    staged_files: Sequence[Path],
) -> TableChange:
    """Compare a table file by file; a table in several files pairs them by name."""
    change = TableChange(table_name)
    published_by_name = {path.name: path for path in published_files}
    staged_by_name = {path.name: path for path in staged_files}
    key_list = ", ".join(f'"{column_name}"' for column_name in key_columns)
    changed_columns: dict[str, int] = {}
    for file_name in sorted(published_by_name.keys() | staged_by_name.keys()):
        published_path, staged_path = published_by_name.get(file_name), staged_by_name.get(file_name)
        if published_path is None or staged_path is None:  # a file only one side has: all its rows come or go
            if staged_path is not None:
                row_count = _count_rows(connection, staged_path)
                change.rows_after += row_count
                change.added_rows += row_count
            else:
                row_count = _count_rows(connection, published_path)
                change.rows_before += row_count
                change.removed_rows += row_count
            continue

        published_types, staged_types = (
            _column_types(connection, published_path),
            _column_types(connection, staged_path),
        )
        shared_columns = [column_name for column_name in staged_types if column_name in published_types]
        change.added_columns = [column_name for column_name in staged_types if column_name not in published_types]
        change.removed_columns = [column_name for column_name in published_types if column_name not in staged_types]
        change.retyped_columns = [
            f"{column_name} {published_types[column_name]} -> {staged_types[column_name]}"
            for column_name in shared_columns
            if published_types[column_name] != staged_types[column_name]
        ]
        shared_list = ", ".join(f'"{column_name}"' for column_name in shared_columns)
        published, staged = sql_path(published_path), sql_path(staged_path)
        # One hash per row keeps this to a pass over each file, however wide the table is.
        row_hashes = f"""
            WITH published AS (
                SELECT {key_list}, hash({shared_list}) AS row_hash, true AS present FROM read_parquet('{published}')
            ),
            staged AS (
                SELECT {key_list}, hash({shared_list}) AS row_hash, true AS present FROM read_parquet('{staged}')
            )
        """
        rows_before, rows_after, added_rows, removed_rows, changed_rows = connection.sql(
            f"""{row_hashes}
            SELECT count(published.present), count(staged.present),
                   count(*) FILTER (WHERE published.present IS NULL),
                   count(*) FILTER (WHERE staged.present IS NULL),
                   count(*) FILTER (WHERE published.row_hash != staged.row_hash)
            FROM published FULL OUTER JOIN staged USING ({key_list})"""
        ).fetchall()[0]
        change.rows_before += rows_before
        change.rows_after += rows_after
        change.added_rows += added_rows
        change.removed_rows += removed_rows
        change.changed_rows += changed_rows
        value_columns = [column_name for column_name in shared_columns if column_name not in key_columns]
        if changed_rows and value_columns:
            differing = ", ".join(
                f'count(*) FILTER (WHERE old."{column_name}" IS DISTINCT FROM new."{column_name}")'
                for column_name in value_columns
            )
            differing_counts = connection.sql(
                f"""{row_hashes},
                changed AS (
                    SELECT {key_list} FROM published JOIN staged USING ({key_list})
                    WHERE published.row_hash != staged.row_hash
                )
                SELECT {differing}
                FROM changed
                JOIN read_parquet('{published}') old USING ({key_list})
                JOIN read_parquet('{staged}') new USING ({key_list})"""
            ).fetchall()[0]
            for column_name, differing_rows in zip(value_columns, differing_counts, strict=True):
                if differing_rows:
                    changed_columns[column_name] = changed_columns.get(column_name, 0) + differing_rows
    change.changed_columns = dict(sorted(changed_columns.items(), key=lambda item: (-item[1], item[0])))
    return change


def format_table_change(change: TableChange, column_limit: int = 6) -> str:
    """One line a person can scan: the row counts, then what differs."""
    if change.rows_before == change.rows_after:
        row_counts = f"{change.rows_after:,} rows"
    else:
        row_counts = f"{change.rows_before:,} -> {change.rows_after:,} rows"
    if change.unchanged:
        return f"{change.table_name:<22}{row_counts}: no change"
    details: list[str] = []
    if change.added_rows:
        details.append(f"{change.added_rows:,} added")
    if change.removed_rows:
        details.append(f"{change.removed_rows:,} removed")
    if change.changed_rows:
        columns = [
            f"{column_name} {count:,}" for column_name, count in list(change.changed_columns.items())[:column_limit]
        ]
        if len(change.changed_columns) > column_limit:
            columns.append(f"and {len(change.changed_columns) - column_limit} more columns")
        details.append(f"{change.changed_rows:,} changed ({', '.join(columns)})")
    if change.added_columns:
        details.append(f"new column(s) {', '.join(change.added_columns)}")
    if change.removed_columns:
        details.append(f"dropped column(s) {', '.join(change.removed_columns)}")
    if change.retyped_columns:
        details.append(f"retyped {', '.join(change.retyped_columns)}")
    return f"{change.table_name:<22}{row_counts}: {'; '.join(details)}"
