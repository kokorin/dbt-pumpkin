from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from dbt_pumpkin.exception import PropertyNotAllowedError, PropertyRequiredError

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class YamlFormat:
    indent: int | None = None
    offset: int | None = None
    preserve_quotes: bool | None = None
    max_width: int | None = None

    def __post_init__(self):
        details = "Both indent and offset are required for YAML formatting"
        if self.indent is None and self.offset is not None:
            raise PropertyRequiredError("indent", details)  # noqa: EM101
        if self.indent is not None and self.offset is None:
            raise PropertyRequiredError("offset", details)  # noqa: EM101

    @classmethod
    def from_dict(cls, data: dict) -> YamlFormat:
        # Using mashumaro.DataClassDictMixin produces "TypeError: unsupported operand type(s) for |" with Python 3.9
        return YamlFormat(
            indent=int(data["indent"]) if "indent" in data else None,
            offset=int(data["offset"]) if "offset" in data else None,
            preserve_quotes=bool(data["preserve_quotes"]) if "preserve_quotes" in data else None,
            max_width=int(data["max_width"]) if "max_width" in data else None,
        )

# TODO Delete this
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
        return set(cls._value2member_map_.keys())

    def __str__(self):
        return self.value


def get_resource_type(resource: Resource) -> ResourceType:
    """Returns the ResourceType for a given resource."""
    match resource:
        case Source():
            return ResourceType.SOURCE
        case Model():
            return ResourceType.MODEL
        case Seed():
            return ResourceType.SEED
        case Snapshot():
            return ResourceType.SNAPSHOT
        case _:
            msg = f"Unknown resource type: {type(resource)}"
            raise ValueError(msg)


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
    numeric_precision_and_scale: bool
    string_length: bool


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
    quote: bool | None
    data_type: str | None
    description: str | None


@dataclass(frozen=True)
class Resource(ABC):
    """Abstract base class for all dbt resources."""
    unique_id: ResourceID
    name: str
    database: str
    schema: str
    identifier: str
    yaml_path: Path | None
    columns: list[ResourceColumn]
    config: ResourceConfig | None

    @property
    @abstractmethod
    def type(self) -> ResourceType:
        """Returns the ResourceType for this resource."""

    def __hash__(self):
        return hash(self.unique_id)


@dataclass(frozen=True)
class Source(Resource):
    """Represents a dbt source table."""
    source_name: str

    @property
    def type(self) -> ResourceType:
        return ResourceType.SOURCE

    def __post_init__(self):
        if not self.source_name:
            raise PropertyRequiredError("source_name", self.unique_id)  # noqa: EM101
        if not self.yaml_path:
            raise PropertyRequiredError("yaml_path", self.unique_id)  # noqa: EM101

    def __hash__(self):
        return hash(self.unique_id)


@dataclass(frozen=True)
class Model(Resource):
    """Represents a dbt model (versioned or non-versioned)."""
    path: Path
    version: str | float | None = None

    @property
    def type(self) -> ResourceType:
        return ResourceType.MODEL

    def __post_init__(self):
        if not self.path:
            raise PropertyRequiredError("path", self.unique_id)  # noqa: EM101

    def is_versioned(self) -> bool:
        """Returns True if this is a versioned model."""
        return self.version is not None

    def __hash__(self):
        return hash(self.unique_id)


@dataclass(frozen=True)
class Seed(Resource):
    """Represents a dbt seed."""
    path: Path

    @property
    def type(self) -> ResourceType:
        return ResourceType.SEED

    def __post_init__(self):
        if not self.path:
            raise PropertyRequiredError("path", self.unique_id)  # noqa: EM101

    def __hash__(self):
        return hash(self.unique_id)


@dataclass(frozen=True)
class Snapshot(Resource):
    """Represents a dbt snapshot."""
    path: Path

    @property
    def type(self) -> ResourceType:
        return ResourceType.SNAPSHOT

    def __post_init__(self):
        if not self.path:
            raise PropertyRequiredError("path", self.unique_id)  # noqa: EM101

    def __hash__(self):
        return hash(self.unique_id)
