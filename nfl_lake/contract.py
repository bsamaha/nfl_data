"""Turn one raw file into its clean version, or raise ContractError saying why it cannot be published.

The clean file keeps the raw columns and types, sorted by key. Rows with a null or empty key are set
aside when the catalog allows it, and repeated keys are resolved by `keep_last_by`. The build fails if
the key is still not unique and non-null, or if, against the published version, the file lost more
than `max_row_drop` of its rows, lost a column or changed a column type (the last two pass with
accept_schema_change).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import polars as pl

from .catalog import Dataset


class ContractError(Exception):
    pass


class KeyViolationCounts(NamedTuple):
    null_key_rows: int
    duplicate_key_rows: int


@dataclass
class CleanBuildResult:
    clean_frame: pl.DataFrame
    rejected_frame: pl.DataFrame | None
    input_row_count: int
    deduped_row_count: int
    column_schema: list[list[str]]  # [column_name, dtype] pairs, as stored in clean_state.json
    notes: list[str]

    @property
    def rejected_row_count(self) -> int:
        return 0 if self.rejected_frame is None else self.rejected_frame.height


def _any_key_is_null_expr(key_columns: list[str]) -> pl.Expr:
    return pl.any_horizontal([pl.col(col_name).is_null() for col_name in key_columns])


def _normalize_blank_key_strings(frame: pl.DataFrame, key_columns: list[str]) -> pl.DataFrame:
    """Treat an empty string in a string key column as a missing (null) key."""
    string_key_columns = [col_name for col_name in key_columns if frame.schema[col_name] == pl.String]
    if not string_key_columns:
        return frame
    return frame.with_columns(
        [
            pl.when(pl.col(col_name) == "").then(None).otherwise(pl.col(col_name)).alias(col_name)
            for col_name in string_key_columns
        ]
    )


def count_key_violations(frame: pl.DataFrame, key_columns: list[str]) -> KeyViolationCounts:
    """Count rows with a null/blank key column, and rows repeating a key among the remaining rows."""
    key_frame = _normalize_blank_key_strings(frame.select(key_columns), key_columns)
    non_null_key_frame = key_frame.filter(~_any_key_is_null_expr(key_columns))
    return KeyViolationCounts(
        null_key_rows=key_frame.height - non_null_key_frame.height,
        duplicate_key_rows=non_null_key_frame.height - non_null_key_frame.unique().height,
    )


def build_clean(
    raw_frame: pl.DataFrame,
    dataset: Dataset,
    previous_clean_state: dict | None,
    accept_schema_change: bool,
) -> CleanBuildResult:
    """Apply a dataset's contract to one raw frame.

    `previous_clean_state` is the clean_state.json record of the currently published version, if one exists.
    """
    key_columns = dataset.key_columns
    input_row_count = raw_frame.height

    missing_key_columns = [col_name for col_name in key_columns if col_name not in raw_frame.columns]
    if missing_key_columns:
        raise ContractError(f"key column(s) missing from the file: {', '.join(missing_key_columns)}")

    clean_frame = _normalize_blank_key_strings(raw_frame, key_columns)
    rejected_frame = clean_frame.filter(_any_key_is_null_expr(key_columns))
    if rejected_frame.height:
        if not dataset.spec.reject_null_keys:
            raise ContractError(
                f"{rejected_frame.height} row(s) have a null key column ({', '.join(key_columns)}). "
                "Set reject_null_keys in the catalog if such rows should be set aside."
            )
        rejected_row_share = rejected_frame.height / input_row_count
        if rejected_row_share > dataset.spec.max_reject_share:
            raise ContractError(
                f"{rejected_frame.height} of {input_row_count} rows ({rejected_row_share:.1%}) have a null key, "
                f"above the {dataset.spec.max_reject_share:.1%} limit"
            )
        clean_frame = clean_frame.filter(~_any_key_is_null_expr(key_columns))

    deduped_row_count = 0
    order_column = dataset.spec.keep_last_by
    if order_column and order_column in clean_frame.columns:
        row_count_before_dedup = clean_frame.height
        clean_frame = clean_frame.sort(order_column, nulls_last=False, maintain_order=True).unique(
            subset=key_columns, keep="last", maintain_order=True
        )
        deduped_row_count = row_count_before_dedup - clean_frame.height

    violations = count_key_violations(clean_frame, key_columns)
    if violations.duplicate_key_rows:
        sample_duplicate_keys = (
            clean_frame.filter(pl.struct(key_columns).is_duplicated()).select(key_columns).unique().head(3).rows()
        )
        unused_rule = ""
        if order_column and order_column not in clean_frame.columns:
            unused_rule = f". keep_last_by names {order_column!r}, which this file does not have"
        raise ContractError(
            f"{violations.duplicate_key_rows} row(s) repeat a key ({', '.join(key_columns)}), "
            f"e.g. {sample_duplicate_keys}{unused_rule}"
        )

    if clean_frame.height == 0:
        raise ContractError("no rows left to publish")

    clean_frame = clean_frame.sort(key_columns)
    column_schema = [[col_name, str(dtype)] for col_name, dtype in clean_frame.schema.items()]
    notes: list[str] = []

    if previous_clean_state:
        previous_row_count = previous_clean_state["rows"]
        if clean_frame.height < previous_row_count * (1 - dataset.spec.max_row_drop):
            raise ContractError(
                f"rows fell from {previous_row_count} to {clean_frame.height}, more than the "
                f"{dataset.spec.max_row_drop:.1%} allowed. Upstream may have published a truncated file."
            )
        previous_column_types = dict(previous_clean_state["columns"])
        current_column_types = dict(column_schema)
        removed_columns = [col_name for col_name in previous_column_types if col_name not in current_column_types]
        retyped_columns = [
            f"{col_name} {previous_column_types[col_name]} -> {current_column_types[col_name]}"
            for col_name in current_column_types
            if col_name in previous_column_types and previous_column_types[col_name] != current_column_types[col_name]
        ]
        added_columns = [col_name for col_name in current_column_types if col_name not in previous_column_types]
        if added_columns:
            notes.append(f"new column(s) {', '.join(added_columns)}")
        if removed_columns or retyped_columns:
            schema_change_summary = "; ".join(
                filter(
                    None,
                    [
                        f"removed: {', '.join(removed_columns)}" if removed_columns else "",
                        f"type changed: {', '.join(retyped_columns)}" if retyped_columns else "",
                    ],
                )
            )
            if not accept_schema_change:
                raise ContractError(
                    f"schema changed ({schema_change_summary}). Re-run with --accept-schema-change "
                    "once downstream code is ready for it."
                )
            notes.append(f"accepted schema change ({schema_change_summary})")

    return CleanBuildResult(
        clean_frame=clean_frame,
        rejected_frame=rejected_frame if rejected_frame.height else None,
        input_row_count=input_row_count,
        deduped_row_count=deduped_row_count,
        column_schema=column_schema,
        notes=notes,
    )
