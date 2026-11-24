"""The dataset catalog, `catalog/datasets.yml`: one contract per dataset. Unknown settings are rejected."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Bump when build_clean changes what it writes, so every clean file is rebuilt on the next run.
CLEAN_BUILD_VERSION = 1
SEASON_PLACEHOLDER = "{season}"
_DATASET_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_REPO_PATTERN = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_RELEASE_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSpec(_StrictModel):
    repo: str

    @model_validator(mode="after")
    def _validate_repo(self) -> SourceSpec:
        if not _REPO_PATTERN.fullmatch(self.repo):
            raise ValueError("repo must have the form owner/name")
        return self


class DatasetSpec(_StrictModel):
    """YAML schema for one dataset entry in `catalog/datasets.yml`."""

    release: str
    file: str
    key: tuple[str, ...] = Field(min_length=1)
    min_season: int | None = None
    history: Literal["latest", "all"] = "latest"
    reject_null_keys: bool = False
    keep_last_by: str | None = None
    max_row_drop: float = Field(0.02, ge=0, le=1)
    max_reject_share: float = Field(0.01, ge=0, le=1)

    @model_validator(mode="after")
    def _validate_spec_consistency(self) -> DatasetSpec:
        if not _RELEASE_PATTERN.fullmatch(self.release) or self.release in (".", ".."):
            raise ValueError("release must be a single release tag with no path separators or whitespace")
        if not self.file or "/" in self.file or "\\" in self.file or self.file in (".", ".."):
            raise ValueError("file must be a single file name with no path separators")
        is_seasonal = SEASON_PLACEHOLDER in self.file
        if self.file.count(SEASON_PLACEHOLDER) > 1:
            raise ValueError("a file pattern can contain {season} only once")
        if is_seasonal and self.min_season is None:
            raise ValueError("a file pattern containing {season} needs min_season")
        if not is_seasonal and self.min_season is not None:
            raise ValueError("min_season only applies to a file pattern containing {season}")
        if any(not col_name or col_name.strip() != col_name for col_name in self.key):
            raise ValueError("key columns must be non-empty names with no surrounding whitespace")
        if len(set(self.key)) != len(self.key):
            raise ValueError("key lists a column twice")
        return self


class Dataset(DatasetSpec):
    """One catalog entry together with its name. Every setting of the entry is an attribute, declared once above."""

    name: str

    @property
    def key_columns(self) -> tuple[str, ...]:
        return self.key

    @property
    def is_seasonal(self) -> bool:
        return SEASON_PLACEHOLDER in self.file

    def match_asset_filename(self, asset_filename: str) -> tuple[bool, int | None]:
        """Return (matched, season) for an upstream filename against this dataset's pattern."""
        pattern = re.escape(self.file).replace(re.escape(SEASON_PLACEHOLDER), r"(?P<season>\d{4})")
        regex_match = re.fullmatch(pattern, asset_filename)
        if not regex_match:
            return False, None
        return True, int(regex_match.group("season")) if self.is_seasonal else None

    @property
    def contract(self) -> dict:
        """Rules that shape the clean file; stored verbatim in clean_state.json so any change forces a rebuild."""
        return {
            "build": CLEAN_BUILD_VERSION,
            "key": list(self.key),
            "reject_null_keys": self.reject_null_keys,
            "keep_last_by": self.keep_last_by,
        }


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
        return Dataset(name=dataset_name, **self.datasets[dataset_name].model_dump())

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
