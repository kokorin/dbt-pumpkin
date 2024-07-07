import logging
from abc import abstractmethod
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)


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
        self._yaml = YAML(typ="rt")

    def load_yaml(self, files: set[Path]) -> dict[Path, Any]:
        result: dict[Path, Any] = {}

        for file in files:
            resolved_file = self._root_dir / file
            if not resolved_file.exists():
                logger.debug("File doesn't exist, skipping: %s", resolved_file)
                continue

            logger.debug("Loading file: %s", resolved_file)
            result[file] = self._yaml.load(resolved_file)

        return result

    def save_yaml(self, files: dict[Path, Any]):
        if self._read_only:
            return

        for file, content in files.items():
            resolved_file = self._root_dir / file
            logger.debug("Saving file: %s", resolved_file)
            resolved_file = self._root_dir / file
            resolved_file.parent.mkdir(exist_ok=True)
            self._yaml.dump(content, resolved_file)
