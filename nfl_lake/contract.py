"""Turn one raw file into its clean version, or raise ContractError saying why it cannot be published.

The clean file keeps the raw columns and types, sorted by key. Rows with a null or empty key are set
aside when the catalog allows it, and repeated keys are resolved by `keep_last_by`. The build fails if
the key is still not unique and non-null, or if, against the published version, the file lost more
than `max_row_drop` of its rows, lost a column or changed a column type (the last two pass with
accept_schema_change).
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .catalog import Dataset


class ContractError(Exception):
    pass


@dataclass
class Built:
    frame: pl.DataFrame
    rejects: pl.DataFrame | None
    rows_in: int
    deduped: int
    columns: list[list[str]]  # [name, type] pairs, as stored in clean_state.json
    notes: list[str]

    @property
    def rejected(self) -> int:
        return 0 if self.rejects is None else self.rejects.height


def _null_key(key: list[str]) -> pl.Expr:
    return pl.any_horizontal([pl.col(k).is_null() for k in key])


def check_key(df: pl.DataFrame, key: list[str]) -> tuple[int, int]:
    """(rows with a null key column, rows whose key repeats) for a frame holding the key columns."""
    nulls = df.filter(_null_key(key)).height
    dups = df.height - df.select(key).unique().height
    return nulls, dups


def build_clean(df: pl.DataFrame, ds: Dataset, previous: dict | None, accept_schema_change: bool) -> Built:
    """`previous` is the clean_state record of the version already published, if there is one."""
    key = ds.key
    rows_in = df.height

    missing = [k for k in key if k not in df.columns]
    if missing:
        raise ContractError(f"key column(s) missing from the file: {', '.join(missing)}")

    # An empty string in an id column is a missing id.
    text_keys = [k for k in key if df.schema[k] == pl.String]
    if text_keys:
        df = df.with_columns([pl.when(pl.col(k) == "").then(None).otherwise(pl.col(k)).alias(k) for k in text_keys])

    rejects = df.filter(_null_key(key))
    if rejects.height:
        if not ds.spec.reject_null_keys:
            raise ContractError(f"{rejects.height} row(s) have a null key column ({', '.join(key)}). "
                                "Set reject_null_keys in the catalog if such rows should be set aside.")
        share = rejects.height / rows_in
        if share > ds.spec.max_reject_share:
            raise ContractError(f"{rejects.height} of {rows_in} rows ({share:.1%}) have a null key, "
                                f"above the {ds.spec.max_reject_share:.1%} limit")
        df = df.filter(~_null_key(key))

    deduped = 0
    order = ds.spec.keep_last_by
    if order and order in df.columns:
        before = df.height
        df = df.sort(order, nulls_last=False, maintain_order=True).unique(subset=key, keep="last", maintain_order=True)
        deduped = before - df.height

    _, dups = check_key(df, key)
    if dups:
        sample = df.filter(pl.struct(key).is_duplicated()).select(key).unique().head(3).rows()
        raise ContractError(f"{dups} row(s) repeat a key ({', '.join(key)}), e.g. {sample}")

    if df.height == 0:
        raise ContractError("no rows left to publish")

    df = df.sort(key)
    columns = [[name, str(dtype)] for name, dtype in df.schema.items()]
    notes: list[str] = []

    if previous:
        was = previous["rows"]
        if df.height < was * (1 - ds.spec.max_row_drop):
            raise ContractError(f"rows fell from {was} to {df.height}, more than the {ds.spec.max_row_drop:.1%} "
                                "allowed. Upstream may have published a truncated file.")
        old, new = dict(previous["columns"]), dict(columns)
        removed = [c for c in old if c not in new]
        retyped = [f"{c} {old[c]} -> {new[c]}" for c in new if c in old and old[c] != new[c]]
        added = [c for c in new if c not in old]
        if added:
            notes.append(f"new column(s) {', '.join(added)}")
        if removed or retyped:
            change = "; ".join(filter(None, [
                f"removed: {', '.join(removed)}" if removed else "",
                f"type changed: {', '.join(retyped)}" if retyped else "",
            ]))
            if not accept_schema_change:
                raise ContractError(f"schema changed ({change}). Re-run with --accept-schema-change "
                                    "once downstream code is ready for it.")
            notes.append(f"accepted schema change ({change})")

    return Built(frame=df, rejects=rejects if rejects.height else None, rows_in=rows_in, deduped=deduped,
                 columns=columns, notes=notes)
