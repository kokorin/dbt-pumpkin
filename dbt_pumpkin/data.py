from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

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
class Column:
    name: str
    data_type: str | None
    description: str | None


@dataclass(frozen=True)
class Table:
    resource_id: ResourceID
    columns: list[Column]

    def __hash__(self):
        return hash(self.resource_id)


@dataclass(frozen=True)
class ResourceConfig:
    yaml_path: str | None


@dataclass(frozen=True)
class ResourceID:
    unique_id: str

    @property
    def name(self) -> str:
        return self.unique_id.split(".")[-1]

    def __str__(self):
        return self.unique_id


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
    columns: list[Column]
    config: ResourceConfig | None

    def __hash__(self):
        return hash(self.unique_id)
