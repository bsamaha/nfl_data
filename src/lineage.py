# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Any
import orjson
import hashlib


@dataclass
class PartitionStats:
    row_count: int
    sha256_fingerprint: str
    max_ingested_at: str | None = None
    min_ingested_at: str | None = None


def compute_sha256_for_keys(rows: List[str]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update(row.encode("utf-8"))
    return h.hexdigest()


def load_lineage(path: str = "catalog/lineage.json") -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return orjson.loads(p.read_bytes())


def save_lineage(data: Dict[str, Any], path: str = "catalog/lineage.json") -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))


def update_dataset_lineage(
    lineage: Dict[str, Any],
    dataset: str,
    last_ingest_utc: str,
    rows_last_batch: int,
    changed_partitions: List[str],
    partition_stats: Dict[str, PartitionStats] | None = None,
) -> Dict[str, Any]:
    ds = lineage.get(dataset, {})
    ds.update(
        {
            "last_ingest_utc": last_ingest_utc,
            "rows_last_batch": rows_last_batch,
            "changed_partitions": list(changed_partitions),
            "partitions": ds.get("partitions", {}),
        }
    )
    if partition_stats:
        for part, st in partition_stats.items():
            ds["partitions"][part] = asdict(st)
    lineage[dataset] = ds
    return lineage


def record_partition_counts(
    lineage: Dict[str, Any], dataset: str, partition: str, row_count: int
) -> Dict[str, Any]:
    ds = lineage.get(dataset, {})
    parts = ds.get("partitions", {})
    p = parts.get(partition, {})
    p.update({"row_count": row_count})
    parts[partition] = p
    ds["partitions"] = parts
    lineage[dataset] = ds
    return lineage

