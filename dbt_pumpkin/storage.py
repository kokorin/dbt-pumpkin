from abc import abstractmethod
from pathlib import Path
from typing import Any


class Storage:
    @abstractmethod
    def load_yaml(self, files: set[Path]) -> dict[Path, Any]:
        raise NotImplementedError

    @abstractmethod
    def save_yaml(self, files: dict[Path, Any]) -> None:
        raise NotImplementedError


class DiskStorage:
    def __init__(self, root_dir: Path):
        self._root_dir = root_dir
