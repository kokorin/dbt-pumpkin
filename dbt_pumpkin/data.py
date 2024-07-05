from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from dbt_pumpkin.exception import PropertyNotAllowedError, PropertyRequiredError

if TYPE_CHECKING:
    from pathlib import Path


class ResourceType(Enum):
    SEED = "seed"
    SOURCE = "source"
    MODEL = "model"
    SNAPSHOT = "snapshot"

    @property
    def plural_name(self) -> str:
        # all resource types conform to this rule
        return self.value + "s"

    @classmethod
    def values(cls) -> set[str]:
        return cls._value2member_map_.keys()

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class TableColumn:
    name: str
    dtype: str
    data_type: str
    is_numeric: bool
    is_string: bool


@dataclass(frozen=True)
class Table:
    resource_id: ResourceID
    columns: list[TableColumn]

    def __post_init__(self):
        if not self.columns:
            raise PropertyRequiredError("columns", self.resource_id)  # noqa: EM101

    def __hash__(self):
        return hash(self.resource_id)


@dataclass(frozen=True)
class ResourceConfig:
    yaml_path_template: str | None


@dataclass(frozen=True)
class ResourceID:
    unique_id: str

    @property
    def name(self) -> str:
        return self.unique_id.split(".")[-1]

    def __str__(self):
        return self.unique_id


@dataclass(frozen=True)
class ResourceColumn:
    name: str
    quote: bool
    data_type: str | None
    description: str | None


@dataclass(frozen=True)
class Resource:
    unique_id: ResourceID
    name: str
    source_name: str | None
    database: str
    schema: str
    identifier: str
    type: ResourceType
    path: Path | None
    yaml_path: Path | None
    columns: list[ResourceColumn]
    config: ResourceConfig | None

    def __post_init__(self):
        # Validate invariants
        if self.type == ResourceType.SOURCE:
            if not self.source_name:
                raise PropertyRequiredError("source_name", self.unique_id)  # noqa: EM101
            if self.path:
                raise PropertyNotAllowedError("path", self.unique_id)  # noqa: EM101
            if not self.yaml_path:
                raise PropertyRequiredError("yaml_path", self.unique_id)  # noqa: EM101
        else:
            if self.source_name is not None:
                raise PropertyNotAllowedError("source_name", self.unique_id)  # noqa: EM101
            if not self.path:
                raise PropertyRequiredError("path", self.unique_id)  # noqa: EM101

    def __hash__(self):
        return hash(self.unique_id)
