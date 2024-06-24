from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path

from ruamel.yaml import YAML

from dbt_pumpkin.data import Resource, ResourceType


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
class RelocateResource(Action):
    resource_type: ResourceType
    resource_name: str
    from_path: Path
    to_path: Path

    def affected_files(self) -> set[Path]:
        return {self.from_path, self.to_path}

    def describe(self) -> str:
        return f"Move {self.resource_type}:{self.resource_name} from {self.from_path} to {self.to_path}"

    def apply(self, files: dict[Path, dict]):
        from_yaml_file = files[self.from_path]
        from_yaml_resources: list = from_yaml_file[self.resource_type.plural_name]
        from_yaml_resource: dict = next(r for r in from_yaml_resources if r["name"] == self.resource_name)
        from_yaml_resources.remove(from_yaml_resource)

        to_file = files.setdefault(self.to_path, {"version": 2})

        to_file.setdefault(self.resource_type.plural_name, []).append(from_yaml_resource)


@dataclass
class InitializeResource(Action):
    resource_type: ResourceType
    resource_name: str
    path: Path

    def affected_files(self) -> set[Path]:
        return {self.path}

    def describe(self) -> str:
        return f"Initialize {self.resource_type}:{self.resource_name} at {self.path}"

    def apply(self, files: dict[Path, dict]):
        to_file = files.setdefault(self.path, {"version": 2})
        to_resources = to_file.setdefault(self.resource_type.plural_name, [])
        to_resources.append({"name": self.resource_name, "columns": []})


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
