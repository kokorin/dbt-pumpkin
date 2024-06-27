from abc import abstractmethod
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML


class Storage:
    @abstractmethod
    def load_yaml(self, files: set[Path]) -> dict[Path, Any]:
        raise NotImplementedError

    @abstractmethod
    def save_yaml(self, files: dict[Path, Any]):
        raise NotImplementedError


class DiskStorage(Storage):
    def __init__(self, root_dir: Path, *, read_only: bool):
        self._root_dir = root_dir
        self._read_only = read_only
        self._yaml = YAML(typ="safe")

    def load_yaml(self, files: set[Path]) -> dict[Path, Any]:
        return {file: self._yaml.load(self._root_dir / file) for file in files if file.exists()}

    def save_yaml(self, files: dict[Path, Any]):
        if self._read_only:
            return

        for file, content in files.items():
            file_path = self._root_dir / file
            file_path.parent.mkdir(exist_ok=True)
            self._yaml.dump(content, file_path)
