from __future__ import annotations

import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from pathlib import Path

from dbt_pumpkin.data import ResourceType, Column
from dbt_pumpkin.exception import PumpkinError, ResourceNotFoundError, PropertyRequiredError, PropertyNotAllowedError
from dbt_pumpkin.storage import Storage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Action:
    resource_type: ResourceType
    resource_name: str

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
class RelocateResource(Action):
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
class BootstrapResource(Action):
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
class ResourceColumnAction(Action, ABC):
    path: Path
    source_name: str | None

    def __post_init__(self):
        if self.resource_type == ResourceType.SOURCE and not self.source_name:
            raise PropertyRequiredError("source_name", self.resource_name)
        if self.resource_type != ResourceType.SOURCE and self.source_name is not None:
            raise PropertyNotAllowedError("source_name", self.resource_name)

    def affected_files(self) -> set[Path]:
        return {self.path}

    def _get_or_create_columns(self, files: dict[Path, dict]) -> list[dict[str, any]]:
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

        return yaml_resource.setdefault("columns", [])


@dataclass(frozen=True)
class AddResourceColumn(ResourceColumnAction):
    column_name: str
    column_quote:bool
    column_type: str

    def describe(self) -> str:
        return f"Add column {self.resource_type} {self.resource_name} {self.column_name} {self.column_type} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        yaml_columns.append({
            "name": self.column_name,
            "quote": self.column_quote,
            "data_type": self.column_type
        })


@dataclass(frozen=True)
class UpdateResourceColumn(ResourceColumnAction):
    column_name: str
    column_type: str

    def describe(self) -> str:
        return f"Update column {self.resource_type} {self.resource_name} {self.column_name} {self.column_type} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        yaml_column = next((c for c in yaml_columns if c["name"] == self.column_name), None)
        if not yaml_column:
            msg = f"Column {self.column_name} not found in {self.resource_type} {self.resource_type}"
            raise PumpkinError(msg)

        yaml_column["data_type"] = self.column_type


@dataclass(frozen=True)
class DeleteResourceColumn(ResourceColumnAction):
    column_name: str

    def describe(self) -> str:
        return f"Delete column {self.resource_type} {self.resource_name} {self.column_name} {self.column_type} at {self.path}"

    def execute(self, files: dict[Path, dict]):
        yaml_columns = self._get_or_create_columns(files)
        yaml_column = next((c for c in yaml_columns if c["name"] == self.column_name), None)
        if not yaml_column:
            msg = f"Column {self.column_name} not found in {self.resource_type} {self.resource_type}"
            raise PumpkinError(msg)

        yaml_columns.remove(yaml_column)


@dataclass(frozen=True)
class ReorderResourceColumns(ResourceColumnAction):
    columns_order: list[str]

    def __post_init__(self):
        pass


class Plan:
    def __init__(self, actions: list[Action]):
        self.actions = actions

    def _affected_files(self) -> set[Path]:
        return {f for a in self.actions for f in a.affected_files()}

    def execute(self, storage: Storage):
        affected_files = self._affected_files()
        logger.info("Files affected by plan: %s", len(affected_files))

        files = storage.load_yaml(affected_files)

        for index, action in enumerate(self.actions):
            logger.info("Action %s: %s", index + 1, action.describe())
            action.execute(files)

        storage.save_yaml(files)

    def describe(self) -> str:
        return "\n".join(a.describe() for a in self.actions)
