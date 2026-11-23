"""The dataset catalog, `catalog/datasets.yml`: one contract per dataset. Unknown settings are rejected."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Bump when build_clean changes what it writes, so every clean file is rebuilt on the next run.
CLEAN_BUILD_VERSION = 1
SEASON_PLACEHOLDER = "{season}"
_DATASET_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSpec(_StrictModel):
    repo: str


class DatasetSpec(_StrictModel):
    """YAML schema for one dataset entry in `catalog/datasets.yml`."""

    release: str
    file: str
    key: list[str] = Field(min_length=1)
    min_season: int | None = None
    history: Literal["latest", "all"] = "latest"
    reject_null_keys: bool = False
    keep_last_by: str | None = None
    max_row_drop: float = Field(0.02, ge=0, le=1)
    max_reject_share: float = Field(0.01, ge=0, le=1)

    @model_validator(mode="after")
    def _validate_spec_consistency(self) -> DatasetSpec:
        is_seasonal = SEASON_PLACEHOLDER in self.file
        if self.file.count(SEASON_PLACEHOLDER) > 1:
            raise ValueError("a file pattern can contain {season} only once")
        if is_seasonal and self.min_season is None:
            raise ValueError("a file pattern containing {season} needs min_season")
        if not is_seasonal and self.min_season is not None:
            raise ValueError("min_season only applies to a file pattern containing {season}")
        if len(set(self.key)) != len(self.key):
            raise ValueError("key lists a column twice")
        return self


@dataclass(frozen=True)
class Dataset:
    """One resolved catalog entry with flat access to its contract fields."""

    name: str
    release: str
    file: str
    key_columns: tuple[str, ...]
    min_season: int | None
    history: Literal["latest", "all"]
    reject_null_keys: bool
    keep_last_by: str | None
    max_row_drop: float
    max_reject_share: float
    _filename_pattern: re.Pattern[str] = field(repr=False, compare=False)

    @classmethod
    def from_spec(cls, name: str, spec: DatasetSpec) -> Dataset:
        regex = re.escape(spec.file).replace(re.escape(SEASON_PLACEHOLDER), r"(?P<season>\d{4})")
        return cls(
            name=name,
            release=spec.release,
            file=spec.file,
            key_columns=tuple(spec.key),
            min_season=spec.min_season,
            history=spec.history,
            reject_null_keys=spec.reject_null_keys,
            keep_last_by=spec.keep_last_by,
            max_row_drop=spec.max_row_drop,
            max_reject_share=spec.max_reject_share,
            _filename_pattern=re.compile(regex),
        )

    @property
    def is_seasonal(self) -> bool:
        return SEASON_PLACEHOLDER in self.file

    def match_asset_filename(self, asset_filename: str) -> tuple[bool, int | None]:
        """Return (matched, season) for an upstream filename against this dataset's pattern."""
        regex_match = self._filename_pattern.fullmatch(asset_filename)
        if not regex_match:
            return False, None
        return True, int(regex_match.group("season")) if self.is_seasonal else None

    @property
    def contract_fingerprint(self) -> str:
        """Changes whenever the rules that shape the clean file change, forcing a rebuild."""
        payload = {
            "build": CLEAN_BUILD_VERSION,
            "key": list(self.key_columns),
            "reject_null_keys": self.reject_null_keys,
            "keep_last_by": self.keep_last_by,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


class Catalog(_StrictModel):
    source: SourceSpec
    datasets: dict[str, DatasetSpec]

    @model_validator(mode="after")
    def _validate_dataset_names(self) -> Catalog:
        invalid_names = [
            dataset_name for dataset_name in self.datasets if not _DATASET_NAME_PATTERN.match(dataset_name)
        ]
        if invalid_names:
            raise ValueError(f"dataset names must be lower_snake_case: {invalid_names}")
        return self

    def _require_known(self, dataset_names: list[str]) -> None:
        unknown_names = [dataset_name for dataset_name in dataset_names if dataset_name not in self.datasets]
        if unknown_names:
            raise ValueError(f"not in the catalog: {', '.join(unknown_names)}")

    def get_dataset(self, dataset_name: str) -> Dataset:
        return Dataset.from_spec(dataset_name, self.datasets[dataset_name])

    def select_datasets(
        self,
        dataset_names: list[str] | None = None,
        *,
        exclude_names: list[str] | None = None,
    ) -> list[Dataset]:
        """Datasets in catalog order: all for None, otherwise those in `dataset_names`, minus `exclude_names`."""
        if dataset_names is not None:
            self._require_known(dataset_names)
        if exclude_names is not None:
            self._require_known(exclude_names)
        excluded = set(exclude_names or ())
        chosen = (
            list(self.datasets) if dataset_names is None else [name for name in self.datasets if name in dataset_names]
        )
        return [self.get_dataset(name) for name in chosen if name not in excluded]


def load_catalog(catalog_path: str | Path) -> Catalog:
    return Catalog.model_validate(yaml.safe_load(Path(catalog_path).read_text(encoding="utf-8")))
