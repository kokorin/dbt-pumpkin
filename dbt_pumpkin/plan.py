from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path

from ruamel.yaml import YAML

from dbt_pumpkin.data import Resource


class Action:
    @abstractmethod
    def affected_files(self) -> set[Path]:
        """
        Returns a set of files (paths) which would be affected by this action
        """

    @abstractmethod
    def describe(self) -> str:
        pass

    @abstractmethod
    def apply(self, files: dict[Path, dict]):
        """
        Applies changes to files in memory
        """


@dataclass
class MoveResource(Action):
    resource: Resource
    from_path: Path
    to_path: Path

    def affected_files(self) -> set[Path]:
        return {self.from_path, self.to_path}

    def describe(self) -> str:
        return f"Move {self.resource.unique_id} from {self.from_path} to {self.to_path}"

    def apply(self, files: dict[Path, dict]):
        from_yaml_file = files[self.from_path]
        from_yaml_resources: list = from_yaml_file[self.resource.type.plural_name]
        from_yaml_resource: dict = next(r for r in from_yaml_resources if r["name"] == self.resource.name)
        from_yaml_resources.remove(from_yaml_resource)

        to_file = files.setdefault(self.to_path, {"version": 2})

        to_file.setdefault(self.resource.type.plural_name, []).append(from_yaml_resource)


@dataclass
class AddResource(Action):
    resource: Resource
    to_path: Path

    def affected_files(self) -> set[Path]:
        return {self.to_path}

    def describe(self) -> str:
        return f"Add {self.resource.unique_id} to {self.to_path}"

    def _resource_columns(self) -> list:
        result = []
        for column in self.resource.columns:
            column_yaml = {"name": column.name}
            if column.data_type:
                column_yaml["data_type"] = column.data_type

            if column.description:
                column_yaml["description"] = column.description

            result.append(column_yaml)

        return result

    def apply(self, files: dict[Path, dict]):
        to_file = files.setdefault(self.to_path, {"version": 2})
        to_resources = to_file.setdefault(self.resource.type.plural_name, [])
        to_resources.append({"name": self.resource.name, "columns": self._resource_columns()})


class Plan:
    def __init__(self, actions: list[Action]):
        self.actions = actions
        self._yaml = YAML(typ="safe")

    def _affected_files(self) -> set[Path]:
        return {f for a in self.actions for f in a.affected_files()}

    def apply(self):
        files: dict[Path, dict] = {}
        for file in self._affected_files():
            if file.exists():
                files[file] = self._yaml.load(file)
        for action in self.actions:
            action.apply(files)

        for file, data in files.items():
            self._yaml.dump(data, file)

    def describe(self) -> str:
        return "\n".join(a.describe() for a in self.actions)
