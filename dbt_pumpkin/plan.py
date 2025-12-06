from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from dbt_pumpkin.data import ResourceType
from dbt_pumpkin.exception import PropertyNotAllowedError, PropertyRequiredError, PumpkinError, ResourceNotFoundError

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_pumpkin.storage import Storage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Action(ABC):
    @abstractmethod
    def affected_files(self) -> set[Path]:
        """
        Returns a set of files (paths) which would be affected by this action
        """

    @abstractmethod
    def describe(self) -> str:
        pass

    @abstractmethod
    def execute(self, files: dict[Path, dict]):
        """
        Applies changes to files in memory
        """


@dataclass(frozen=True)
class ResourceAction(Action, ABC):
    resource_type: ResourceType
    resource_name: str


@dataclass(frozen=True)
class RelocateResource(ResourceAction):
    from_path: Path
    to_path: Path

    def affected_files(self) -> set[Path]:
        return {self.from_path, self.to_path}

    def describe(self) -> str:
        return f"Move {self.resource_type}:{self.resource_name} from {self.from_path} to {self.to_path}"

    def execute(self, files: dict[Path, dict]):
        if self.from_path not in files:
            raise ResourceNotFoundError(self.resource_name, self.from_path)

        from_yaml_file = files[self.from_path]
        from_yaml_resources: list = from_yaml_file[self.resource_type.plural_name]
        from_yaml_resource: dict = next(r for r in from_yaml_resources if r["name"] == self.resource_name)
        from_yaml_resources.remove(from_yaml_resource)

        to_file = files.setdefault(self.to_path, {"version": 2})

        to_file.setdefault(self.resource_type.plural_name, []).append(from_yaml_resource)


@dataclass(frozen=True)
class DeleteEmptyDescriptor(Action):
    path: Path

    def affected_files(self) -> set[Path]:
        return {self.path}

    def describe(self) -> str:
        return f"Delete if empty {self.path}"

    def execute(self, files: dict[Path, dict]):
        content = files.get(self.path)
        if content is None:
            return

        no_content = True
        for res_type in ResourceType:
            resources = content.get(res_type.plural_name)
            if resources:
                no_content = False

        if no_content:
            files[self.path] = None


@dataclass(frozen=True)
class BootstrapResource(ResourceAction):
    path: Path

    def __post_init__(self):
        if self.resource_type == ResourceType.SOURCE:
            msg = "Sources must be bootstrapped manually"
            raise PumpkinError(msg)

    def affected_files(self) -> set[Path]:
        return {self.path}

    def describe(self) -> str:
        return f"Bootstrap {self.resource_type}:{self.resource_name} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        to_file = files.setdefault(self.path, {"version": 2})
        to_resources = to_file.setdefault(self.resource_type.plural_name, [])
        to_resources.append({"name": self.resource_name, "columns": []})


@dataclass(frozen=True)
class ResourceColumnAction(ResourceAction, ABC):
    source_name: str | None
    path: Path
    version: str | float | None

    def __post_init__(self):
        if self.resource_type == ResourceType.SOURCE and not self.source_name:
            raise PropertyRequiredError("source_name", self.resource_name)  # noqa: EM101
        if self.resource_type != ResourceType.SOURCE and self.source_name is not None:
            raise PropertyNotAllowedError("source_name", self.resource_name)  # noqa: EM101
        if not self.path:
            raise PropertyRequiredError("path", self.resource_name)  # noqa: EM101

    def affected_files(self) -> set[Path]:
        return {self.path}

    def _get_or_create_columns(self, files: dict[Path, dict]) -> list[dict[str, any]]:
        if self.path not in files:
            raise ResourceNotFoundError(self.resource_name, self.path)

        yaml_content = files[self.path]
        yaml_resources: list = yaml_content[self.resource_type.plural_name]

        if self.resource_type == ResourceType.SOURCE:
            # We need to go 1 level deeper for sources
            yaml_source = next((r for r in yaml_resources if r["name"] == self.source_name), None)
            if not yaml_source:
                msg = f"Source {self.source_name} not found in {self.path}"
                raise PumpkinError(msg)

            yaml_resources = yaml_source.setdefault("tables", [])

        yaml_resource = next((r for r in yaml_resources if r["name"] == self.resource_name), None)
        if not yaml_resource:
            msg = f"Resource {self.resource_name} not found in {self.path}"
            raise PumpkinError(msg)

        columns_parent = yaml_resource
        # For versioned models, navigate to the specific version
        if self.version is not None:
            yaml_versions = yaml_resource.setdefault("versions", [])
            yaml_version = next((v for v in yaml_versions if v.get("v") == self.version), None)
            if not yaml_version:
                msg = f"Version {self.version} not found for {self.resource_name} in {self.path}"
                raise PumpkinError(msg)
            columns_parent = yaml_version

        return columns_parent.setdefault("columns", [])


@dataclass(frozen=True)
class AddResourceColumn(ResourceColumnAction):
    column_name: str
    column_quote: bool
    column_type: str

    def describe(self) -> str:
        return (
            f"Add column {self.resource_type} {self.resource_name} {self.column_name} {self.column_type} at {self.path}"
        )

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)

        # make sure properties are ordered as expected
        yaml_column = {"name": self.column_name}
        if self.column_quote:
            yaml_column["quote"] = True
        yaml_column["data_type"] = self.column_type

        yaml_columns.append(yaml_column)


@dataclass(frozen=True)
class UpdateResourceColumn(ResourceColumnAction):
    column_index: int
    column_name: str
    column_quote: bool
    column_type: str

    def describe(self) -> str:
        return f"Update column {self.resource_type} {self.resource_name} {self.column_name} {self.column_type} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        if self.column_index >= len(yaml_columns):
            msg = f"Column index {self.column_index} out of range in {self.resource_type} {self.resource_name}"
            raise PumpkinError(msg)

        yaml_column = yaml_columns[self.column_index]

        # Verify column name matches (case-insensitive) to catch bugs
        if yaml_column["name"].upper() != self.column_name.upper():
            msg = f"Column name mismatch at index {self.column_index}: expected {self.column_name}, found {yaml_column['name']}"
            raise PumpkinError(msg)

        # Update name only if different (to preserve ruamel comments)
        if yaml_column["name"] != self.column_name:
            yaml_column["name"] = self.column_name

        # Update quote attribute - set if True, remove if False
        if self.column_quote:
            yaml_column["quote"] = True
        else:
            yaml_column.pop("quote", None)

        yaml_column["data_type"] = self.column_type


@dataclass(frozen=True)
class DeleteResourceColumn(ResourceColumnAction):
    column_index: int

    def describe(self) -> str:
        return f"Delete column {self.resource_type} {self.resource_name} at index {self.column_index} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        if self.column_index >= len(yaml_columns):
            msg = f"Column index {self.column_index} out of range in {self.resource_type} {self.resource_name}"
            raise PumpkinError(msg)

        del yaml_columns[self.column_index]


@dataclass(frozen=True)
class ReorderResourceColumns(ResourceColumnAction):
    columns_order: list[str]

    def __post_init__(self):
        if len(self.columns_order) != len(set(self.columns_order)):
            msg = f"Column names must be unique: {self.columns_order}"
            raise PumpkinError(msg)

    def describe(self) -> str:
        return f"Reorder columns {self.resource_type} {self.resource_name} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        column_by_name = {yc["name"]: yc for yc in yaml_columns}

        if column_by_name.keys() != set(self.columns_order):
            msg = f"Column names in YAML and provided don't match: {column_by_name.keys()} vs {self.columns_order}"
            raise PumpkinError(msg)

        reordered_columns = [column_by_name[name] for name in self.columns_order]

        yaml_columns.clear()
        yaml_columns.extend(reordered_columns)


class ExecutionMode(Enum):
    RUN = "run"
    DRY_RUN = "dry_run"


class Plan:
    def __init__(self, actions: list[Action]):
        self.actions = actions

    def _affected_files(self) -> set[Path]:
        return {f for a in self.actions for f in a.affected_files()}

    def execute(self, storage: Storage, mode: ExecutionMode):
        if not self.actions:
            logger.info("Nothing to do")
            return

        affected_files = self._affected_files()
        logger.info("Files affected by plan: %s", len(affected_files))

        files = storage.load_yaml(affected_files)

        for index, action in enumerate(self.actions):
            logger.info("Action %s: %s", index + 1, action.describe())
            action.execute(files)

        if mode == ExecutionMode.RUN:
            logger.info("Persisting changes to files: %s", len(affected_files))
            storage.save_yaml(files)

    def describe(self) -> str:
        return "\n".join(a.describe() for a in self.actions)
