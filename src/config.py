from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml
from pydantic import BaseModel, Field, validator


class DatasetConfigModel(BaseModel):
    importer: str
    years: Optional[str] = None
    partitions: List[str]
    key: List[str]
    options: Dict[str, Any] | None = None
    enabled: bool = True
    sort_by: Optional[List[str]] = None
    max_rows_per_file: Optional[int] = None

    @validator("key")
    def non_empty_keys(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("key must not be empty")
        return v


class CatalogModel(BaseModel):
    root: str
    compression: str = Field("zstd")
    row_group_mb: int = Field(96)
    datasets: Dict[str, DatasetConfigModel]


@dataclass
class DatasetConfig:
    name: str
    importer: str
    years: Optional[str]
    partitions: List[str]
    key: List[str]
    options: Dict[str, Any]
    enabled: bool
    sort_by: Optional[List[str]]
    max_rows_per_file: Optional[int]


@dataclass
class DatasetCatalog:
    root: str
    compression: str
    row_group_mb: int
    datasets: Dict[str, DatasetConfig]


def load_dataset_catalog(path: Optional[str] = None) -> DatasetCatalog:
    yaml_path = Path(path or "catalog/datasets.yml")
    data = yaml.safe_load(yaml_path.read_text())
    parsed = CatalogModel.parse_obj(data)

    datasets: Dict[str, DatasetConfig] = {}
    for name, cfg in parsed.datasets.items():
        datasets[name] = DatasetConfig(
            name=name,
            importer=cfg.importer,
            years=cfg.years,
            partitions=cfg.partitions,
            key=cfg.key,
            options=cfg.options or {},
            enabled=cfg.enabled,
            sort_by=cfg.sort_by,
            max_rows_per_file=cfg.max_rows_per_file,
        )

    return DatasetCatalog(
        root=parsed.root,
        compression=parsed.compression,
        row_group_mb=parsed.row_group_mb,
        datasets=datasets,
    )

