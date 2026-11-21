"""The dataset catalog: one source contract per nflverse file family.

`catalog/datasets.yml` is the only place a dataset's source file, key and rules are defined.
Unknown settings are rejected, so the file cannot drift away from what the code reads.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

#: Bump when build_clean changes what it writes, so every clean file is rebuilt on the next run.
BUILD_VERSION = 1
SEASON = "{season}"
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSpec(_Strict):
    kind: Literal["github_releases"] = "github_releases"
    repo: str


class Defaults(_Strict):
    history: Literal["latest", "all"] = "latest"
    max_row_drop: float = Field(0.02, ge=0, le=1)
    max_reject_share: float = Field(0.01, ge=0, le=1)


class DatasetSpec(_Strict):
    release: str
    file: str
    key: list[str] = Field(min_length=1)
    min_season: int | None = None
    history: Literal["latest", "all"] | None = None
    reject_null_keys: bool = False
    keep_last_by: str | None = None
    max_row_drop: float | None = Field(None, ge=0, le=1)
    max_reject_share: float | None = Field(None, ge=0, le=1)
    enabled: bool = True

    @model_validator(mode="after")
    def _consistent(self) -> "DatasetSpec":
        seasonal = SEASON in self.file
        if seasonal and self.min_season is None:
            raise ValueError("a file pattern containing {season} needs min_season")
        if not seasonal and self.min_season is not None:
            raise ValueError("min_season only applies to a file pattern containing {season}")
        if len(set(self.key)) != len(self.key):
            raise ValueError("key lists a column twice")
        return self


@dataclass(frozen=True)
class Dataset:
    """A dataset's spec with the catalog defaults applied."""

    name: str
    spec: DatasetSpec
    defaults: Defaults

    @property
    def seasonal(self) -> bool:
        return SEASON in self.spec.file

    @property
    def key(self) -> list[str]:
        return list(self.spec.key)

    @property
    def history(self) -> str:
        return self.spec.history or self.defaults.history

    @property
    def max_row_drop(self) -> float:
        return self.defaults.max_row_drop if self.spec.max_row_drop is None else self.spec.max_row_drop

    @property
    def max_reject_share(self) -> float:
        return self.defaults.max_reject_share if self.spec.max_reject_share is None else self.spec.max_reject_share

    def match(self, asset_name: str) -> tuple[bool, int | None]:
        """Does an upstream file name belong to this dataset, and for which season?"""
        pattern = re.escape(self.spec.file).replace(re.escape(SEASON), r"(?P<season>\d{4})")
        m = re.fullmatch(pattern, asset_name)
        if not m:
            return False, None
        return True, int(m.group("season")) if self.seasonal else None

    @property
    def fingerprint(self) -> str:
        """Changes whenever the rules that shape the clean file change, which forces a rebuild."""
        payload = {
            "build": BUILD_VERSION,
            "key": self.key,
            "reject_null_keys": self.spec.reject_null_keys,
            "keep_last_by": self.spec.keep_last_by,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


class Catalog(_Strict):
    root: str = "data"
    source: SourceSpec
    defaults: Defaults = Defaults()
    datasets: dict[str, DatasetSpec]

    @model_validator(mode="after")
    def _names(self) -> "Catalog":
        bad = [n for n in self.datasets if not _NAME.match(n)]
        if bad:
            raise ValueError(f"dataset names must be lower_snake_case: {bad}")
        return self

    def dataset(self, name: str) -> Dataset:
        return Dataset(name=name, spec=self.datasets[name], defaults=self.defaults)

    def select(self, names: list[str] | None = None) -> list[Dataset]:
        """Enabled datasets, optionally limited to `names`. Unknown or disabled names are errors."""
        if names:
            unknown = [n for n in names if n not in self.datasets]
            if unknown:
                raise ValueError(f"not in the catalog: {', '.join(unknown)}")
            disabled = [n for n in names if not self.datasets[n].enabled]
            if disabled:
                raise ValueError(f"disabled in the catalog: {', '.join(disabled)}")
            return [self.dataset(n) for n in self.datasets if n in names]
        return [self.dataset(n) for n, spec in self.datasets.items() if spec.enabled]


def load_catalog(path: str | Path = "catalog/datasets.yml") -> Catalog:
    return Catalog.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
