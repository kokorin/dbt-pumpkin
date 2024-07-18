from __future__ import annotations

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING

from ruamel.yaml import YAML

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_pumpkin.data import YamlFormat

logger = logging.getLogger(__name__)


class Storage:
    @abstractmethod
    def load_yaml(self, files: set[Path]) -> dict[Path, any]:
        raise NotImplementedError

    @abstractmethod
    def save_yaml(self, files: dict[Path, any]):
        raise NotImplementedError


class DiskStorage(Storage):
    def __init__(self, root_dir: Path, yaml_format: YamlFormat | None, *, read_only: bool):
        self._root_dir = root_dir
        self._read_only = read_only

        self._yaml = YAML(typ="rt")
        self._yaml.preserve_quotes = True
        if yaml_format:
            self._yaml.map_indent = yaml_format.indent
            self._yaml.sequence_indent = yaml_format.indent + yaml_format.offset
            self._yaml.sequence_dash_offset = yaml_format.offset

    def load_yaml(self, files: set[Path]) -> dict[Path, any]:
        result: dict[Path, any] = {}

        for file in files:
            resolved_file = self._root_dir / file
            if not resolved_file.exists():
                logger.debug("File doesn't exist, skipping: %s", resolved_file)
                continue

            logger.debug("Loading file: %s", resolved_file)
            result[file] = self._yaml.load(resolved_file)

        return result

    def save_yaml(self, files: dict[Path, any]):
        if self._read_only:
            return

        for file, content in files.items():
            resolved_file = self._root_dir / file
            logger.debug("Saving file: %s", resolved_file)
            resolved_file = self._root_dir / file
            resolved_file.parent.mkdir(exist_ok=True)
            self._yaml.dump(content, resolved_file)
