"""Turn one raw file into its clean version, or say exactly why it cannot be published.

The clean file has the same columns and types as the raw one. The only changes are the ones the
catalog declares for the dataset: rows with a null key set aside, and repeated keys resolved by
`keep_last_by`. After that the key must be unique and non-null, and the file must not have shrunk
or lost columns compared with the version already published.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from .catalog import Dataset


class ContractError(Exception):
    def __init__(self, key: str, message: str):
        super().__init__(f"{key}: {message}")
        self.key = key


@dataclass
class Built:
    frame: pl.DataFrame
    rejects: pl.DataFrame | None
    rows_in: int
    rejected: int
    deduped: int
    notes: list[str] = field(default_factory=list)

    @property
    def columns(self) -> list[list[str]]:
        return [[name, str(dtype)] for name, dtype in self.frame.schema.items()]


def _null_key(key: list[str]) -> pl.Expr:
    return pl.any_horizontal([pl.col(k).is_null() for k in key])


def check_key(df: pl.DataFrame, key: list[str]) -> tuple[int, int]:
    """(rows with a null key column, rows whose key repeats) for a frame holding the key columns."""
    nulls = df.filter(_null_key(key)).height
    dups = df.height - df.select(key).unique().height
    return nulls, dups


def build_clean(df: pl.DataFrame, ds: Dataset, key_name: str, previous: dict | None = None,
                accept_schema_change: bool = False) -> Built:
    key = ds.key
    rows_in = df.height
    notes: list[str] = []

    missing = [k for k in key if k not in df.columns]
    if missing:
        raise ContractError(key_name, f"key column(s) missing from the file: {', '.join(missing)}")

    # An empty string in an id column is a missing id.
    text_keys = [k for k in key if df.schema[k] == pl.String]
    if text_keys:
        df = df.with_columns([pl.when(pl.col(k) == "").then(None).otherwise(pl.col(k)).alias(k) for k in text_keys])

    rejects = df.filter(_null_key(key))
    if rejects.height:
        if not ds.spec.reject_null_keys:
            raise ContractError(key_name, f"{rejects.height} row(s) have a null key column ({', '.join(key)}). "
                                          "Set reject_null_keys in the catalog if such rows should be set aside.")
        share = rejects.height / rows_in
        if share > ds.max_reject_share:
            raise ContractError(key_name, f"{rejects.height} of {rows_in} rows ({share:.1%}) have a null key, "
                                          f"above the {ds.max_reject_share:.1%} limit")
        df = df.filter(~_null_key(key))

    deduped = 0
    order = ds.spec.keep_last_by
    if order and order in df.columns:
        before = df.height
        df = df.sort(order, nulls_last=False, maintain_order=True).unique(subset=key, keep="last", maintain_order=True)
        deduped = before - df.height

    dups = df.height - df.select(key).unique().height
    if dups:
        sample = df.filter(pl.struct(key).is_duplicated()).select(key).unique().head(3).rows()
        raise ContractError(key_name, f"{dups} row(s) repeat a key ({', '.join(key)}), e.g. {sample}")

    if df.height == 0:
        raise ContractError(key_name, "no rows left to publish")

    df = df.sort(key)
    built = Built(frame=df, rejects=rejects if rejects.height else None, rows_in=rows_in,
                  rejected=rejects.height, deduped=deduped, notes=notes)

    if previous:
        was = previous["rows"]
        if was and df.height < was * (1 - ds.max_row_drop):
            raise ContractError(key_name, f"rows fell from {was} to {df.height}, more than the {ds.max_row_drop:.1%} "
                                          "allowed. Upstream may have published a truncated file.")
        old = {name: dtype for name, dtype in previous["columns"]}
        new = {name: dtype for name, dtype in built.columns}
        removed = [c for c in old if c not in new]
        retyped = [f"{c} {old[c]} -> {new[c]}" for c in new if c in old and old[c] != new[c]]
        added = [c for c in new if c not in old]
        if added:
            notes.append(f"{key_name}: new column(s) {', '.join(added)}")
        if removed or retyped:
            change = "; ".join(filter(None, [
                f"removed: {', '.join(removed)}" if removed else "",
                f"type changed: {', '.join(retyped)}" if retyped else "",
            ]))
            if not accept_schema_change:
                raise ContractError(key_name, f"layout changed ({change}). Re-run with --accept-schema-change "
                                              "once downstream models are ready for it.")
            notes.append(f"{key_name}: accepted layout change ({change})")
    return built
