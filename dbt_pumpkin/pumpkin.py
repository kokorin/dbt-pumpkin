from __future__ import annotations

import json
import os
import shutil
import tempfile
from functools import cached_property
from pathlib import Path
from typing import Union

from ruamel.yaml import YAML

from dbt_pumpkin.dbt_compat import (
    ColumnInfo,
    EventMsg,
    Manifest,
    ModelNode,
    SeedNode,
    SnapshotNode,
    SourceDefinition,
    dbtRunner,
    dbtRunnerResult,
    default_project_dir,
)
from dbt_pumpkin.exception import PumpkinError

Resource = Union[SourceDefinition, ModelNode, SnapshotNode, SeedNode]


class Pumpkin:
    def __init__(
        self,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        selects: list[str] | None = None,
        excludes: list[str] | None = None,
    ) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.selects = selects or []
        self.excludes = excludes or []
        self._yaml = YAML(typ="safe")

    @cached_property
    def manifest(self) -> Manifest:
        args = ["parse"]
        if self.project_dir:
            args += ["--project-dir", self.project_dir]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]

        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            raise res.exception

        return res.result

    @cached_property
    def selected_resource_ids(self) -> dict[str, set[str]]:
        """
        Returns a dictionary mapping resource type to a set of resource identifiers
        """
        args = ["list"]
        if self.project_dir:
            args += ["--project-dir", self.project_dir]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]
        for select in self.selects:
            args += ["--select", select]
        for exclude in self.excludes:
            args += ["--exclude", exclude]
        args += ["--output", "json"]

        res: dbtRunnerResult = dbtRunner(self.manifest).invoke(args)

        if not res.success:
            raise res.exception

        result: dict[str, set[str]] = {}
        for raw_resource in res.result:
            resource = json.loads(raw_resource)
            resource_type = resource["resource_type"]
            if resource_type in {"seed", "model", "snapshot", "source"}:
                result.setdefault(resource_type, set()).add(resource["unique_id"])

        return result

    @cached_property
    def selected_resources(self) -> list[Resource]:
        results: list[Resource] = []

        for resource_type, resource_ids in self.selected_resource_ids.items():
            resource_by_id = self.manifest.sources if resource_type == "source" else self.manifest.nodes
            results += [resource_by_id[res_id] for res_id in resource_ids]

        return results

    @cached_property
    def selected_resource_actual_schemas(self) -> dict[str, list[ColumnInfo]]:
        src_macros_path = Path(__file__).parent / "macros"

        if not src_macros_path.exists() or not src_macros_path.is_dir():
            msg = f"Macros directory is not found or doesn't exist: {src_macros_path}"
            raise PumpkinError(msg)

        project_dir = Path(self.project_dir or os.environ.get("DBT_PROJECT_DIR", None) or default_project_dir())

        project_yml_path = project_dir / "dbt_project.yml"

        if not project_yml_path.exists() or not project_yml_path.is_file():
            msg = f"dbt_project.yml is not found or doesn't exist: {project_yml_path}"
            raise PumpkinError(msg)

        operation_args = {
            resource.unique_id: [resource.database, resource.schema, resource.identifier]
            for resource in self.selected_resources
        }

        project_yml = self._yaml.load(project_yml_path)
        pumpkin_yml = {
            "name": "dbt_pumpkin",
            "version": "0.1.0",
            "profile": project_yml["profile"],
            # TODO: copy vars?
            "vars": {
                # workaround for too long CMD on Windows
                "get_column_types_args": operation_args
            },
        }

        jinja_log_messages: list[str] = []

        def event_callback(event: EventMsg):
            if event.info.name == "JinjaLogInfo":
                jinja_log_messages.append(event.info.msg)

        # Can't use as TemporaryDirectory Context Manager: DBT doesn't release log file when dbtRunner completes
        # On Python 3.10 and higher it's possible to set ignore_cleanup_errors = True
        pumpkin_dir_str = tempfile.mkdtemp(prefix="dbt_pumpkin_")
        pumpkin_dir = Path(pumpkin_dir_str)
        shutil.copytree(src_macros_path, pumpkin_dir / "macros")
        self._yaml.dump(pumpkin_yml, pumpkin_dir / "dbt_project.yml")

        args = ["run-operation", "get_column_types", "--project-dir", pumpkin_dir_str]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]

        res: dbtRunnerResult = dbtRunner(callbacks=[event_callback]).invoke(args)

        if not res.success:
            raise res.exception

        if not jinja_log_messages:
            msg = "No schema retrieved from database"
            raise PumpkinError(msg)

        column_types_response = json.loads(jinja_log_messages[0])

        return {
            res_id: [ColumnInfo(name=c["name"], data_type=c["data_type"]) for c in columns]
            for res_id, columns in column_types_response.items()
        }
