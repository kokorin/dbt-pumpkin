from __future__ import annotations

import logging
import os
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
    def __init__(self, root_dir: Path, yaml_format: YamlFormat | None):
        self._root_dir = root_dir

        self._yaml = YAML(typ="rt")
        if yaml_format:
            if yaml_format.indent is not None and yaml_format.offset is not None:
                self._yaml.map_indent = yaml_format.indent
                self._yaml.sequence_indent = yaml_format.indent + yaml_format.offset
                self._yaml.sequence_dash_offset = yaml_format.offset

            self._yaml.preserve_quotes = yaml_format.preserve_quotes
            self._yaml.width = yaml_format.max_width

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
        for file, content in files.items():
            resolved_file = self._root_dir / file

            if content is not None:
                logger.debug("Saving file: %s", resolved_file)
                resolved_file.parent.mkdir(exist_ok=True)
                self._yaml.dump(content, resolved_file)
            elif resolved_file.exists():
                logger.debug("Deleting file: %s", resolved_file)
                os.remove(resolved_file)
