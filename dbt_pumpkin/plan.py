from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from dbt_pumpkin.data import Model, Resource, ResourceColumn, ResourceType, Source, get_resource_type
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
    resource: Resource

    @property
    def resource_type(self) -> ResourceType:
        return get_resource_type(self.resource)

    @property
    def resource_name(self) -> str:
        return self.resource.name


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

        # For sources, we need to match by source_name, not by name (table name)
        if isinstance(self.resource, Source):
            from_yaml_resource: dict = next(r for r in from_yaml_resources if r["name"] == self.resource.source_name)
        else:
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
        if isinstance(self.resource, Source):
            msg = "Sources must be bootstrapped manually"
            raise PumpkinError(msg)
        if isinstance(self.resource, Model) and self.resource.version:
            msg = "Versioned models cannot be bootstrapped - versions must be defined in YAML first"
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
    path: Path

    def __post_init__(self):
        if not self.path:
            raise PropertyRequiredError("path", self.resource_name)  # noqa: EM101

    def affected_files(self) -> set[Path]:
        return {self.path}

    def _get_or_create_columns(self, files: dict[Path, dict]) -> list[dict[str, any]]:
        if self.path not in files:
            raise ResourceNotFoundError(self.resource_name, self.path)

        yaml_content = files[self.path]
        yaml_resources: list = yaml_content[self.resource_type.plural_name]

        if isinstance(self.resource, Source):
            # Sources: navigate to source -> tables -> table
            yaml_source = next((r for r in yaml_resources if r["name"] == self.resource.source_name), None)
            if not yaml_source:
                msg = f"Source {self.resource.source_name} not found in {self.path}"
                raise PumpkinError(msg)

            yaml_resources = yaml_source.setdefault("tables", [])

        # Find the resource entry
        yaml_resource = next((r for r in yaml_resources if r["name"] == self.resource_name), None)
        if not yaml_resource:
            msg = f"Resource {self.resource_name} not found in {self.path}"
            raise PumpkinError(msg)

        # For versioned models, navigate to versions[i].columns
        if isinstance(self.resource, Model) and self.resource.version:
            versions = yaml_resource.get("versions", [])
            if not versions:
                msg = f"Versioned model {self.resource_name} has no versions array in {self.path}"
                raise PumpkinError(msg)

            version_entry = next((v for v in versions if v.get("v") == self.resource.version), None)
            if not version_entry:
                msg = f"Version {self.resource.version} not found for model {self.resource_name} in {self.path}"
                raise PumpkinError(msg)

            return version_entry.setdefault("columns", [])

        # For non-versioned resources, use model-level columns
        return yaml_resource.setdefault("columns", [])


@dataclass(frozen=True)
class AddResourceColumn(ResourceColumnAction):
    column: ResourceColumn

    def describe(self) -> str:
        version_suffix = f".v{self.resource.version}" if isinstance(self.resource, Model) and self.resource.version else ""
        return (
            f"Add column {self.resource_type} {self.resource_name}{version_suffix} {self.column.name} {self.column.data_type} at {self.path}"
        )

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)

        # make sure properties are ordered as expected
        yaml_column = {"name": self.column.name}
        if self.column.quote:
            yaml_column["quote"] = True
        yaml_column["data_type"] = self.column.data_type

        yaml_columns.append(yaml_column)


@dataclass(frozen=True)
class UpdateResourceColumn(ResourceColumnAction):
    column: ResourceColumn

    def describe(self) -> str:
        version_suffix = f".v{self.resource.version}" if isinstance(self.resource, Model) and self.resource.version else ""
        return f"Update column {self.resource_type} {self.resource_name}{version_suffix} {self.column.name} {self.column.data_type} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        yaml_column = next((c for c in yaml_columns if c["name"] == self.column.name), None)
        if not yaml_column:
            msg = f"Column {self.column.name} not found in {self.resource_type} {self.resource_name}"
            raise PumpkinError(msg)

        yaml_column["data_type"] = self.column.data_type


@dataclass(frozen=True)
class DeleteResourceColumn(ResourceColumnAction):
    column_name: str

    def describe(self) -> str:
        version_suffix = f".v{self.resource.version}" if isinstance(self.resource, Model) and self.resource.version else ""
        return f"Delete column {self.resource_type} {self.resource_name}{version_suffix} {self.column_name} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        yaml_column = next((c for c in yaml_columns if c["name"] == self.column_name), None)
        if not yaml_column:
            msg = f"Column {self.column_name} not found in {self.resource_type} {self.resource_name}"
            raise PumpkinError(msg)

        yaml_columns.remove(yaml_column)


@dataclass(frozen=True)
class ReorderResourceColumns(ResourceColumnAction):
    columns_order: list[str]

    def __post_init__(self):
        if len(self.columns_order) != len(set(self.columns_order)):
            msg = f"Column names must be unique: {self.columns_order}"
            raise PumpkinError(msg)

    def describe(self) -> str:
        version_suffix = f".v{self.resource.version}" if isinstance(self.resource, Model) and self.resource.version else ""
        return f"Reorder columns {self.resource_type} {self.resource_name}{version_suffix} at {self.path}"

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
