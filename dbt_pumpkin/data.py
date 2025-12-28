from __future__ import annotations

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


class CaseFolding(Enum):
    """Database identifier case folding behavior."""

    UPPER = "upper"  # Database folds unquoted identifiers to uppercase (e.g., Snowflake, Oracle)
    LOWER = "lower"  # Database folds unquoted identifiers to lowercase (e.g., PostgreSQL, Redshift)
    PRESERVE = "preserve"  # Database preserves case of unquoted identifiers (e.g., some MySQL configurations)
    UNKNOWN = "unknown"  # Could not detect folding behavior, fall back to always quoting


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

    def __str__(self):
        return self.unique_id


@dataclass(frozen=True)
class ResourceColumn:
    name: str
    quote: bool | None
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
    version: str | float | None

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
