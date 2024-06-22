from __future__ import annotations

import json
import os
import shutil
import tempfile
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

from ruamel.yaml import YAML

from dbt_pumpkin.data import Column, Resource, ResourceConfig, ResourceID, ResourceType, Table
from dbt_pumpkin.dbt_compat import (
    EventMsg,
    Manifest,
    dbtRunner,
    dbtRunnerResult,
    default_project_dir,
)
from dbt_pumpkin.exception import PumpkinError

if TYPE_CHECKING:
    from dbt_pumpkin.dbt_compat import ModelNode, SeedNode, SnapshotNode, SourceDefinition


class ResourceLoader:
    def __init__(
        self,
        project_dir: str | None = None,
        profiles_dir: str | None = None,
        selects: list[str] | None = None,
        excludes: list[str] | None = None,
    ) -> None:
        # Project Directory must be set, as we rely on it in some methods
        self.project_dir = project_dir or os.environ.get("DBT_PROJECT_DIR", None) or str(default_project_dir())
        self.profiles_dir = profiles_dir
        self.selects = selects or []
        self.excludes = excludes or []
        self._yaml = YAML(typ="safe")

    @cached_property
    def manifest(self) -> Manifest:
        args = ["parse"]
        if self.project_dir:
            args += ["--project-dir", str(self.project_dir)]
        if self.profiles_dir:
            args += ["--profiles-dir", str(self.profiles_dir)]

        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            raise res.exception

        return res.result

    @cached_property
    def resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
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

        result: dict[ResourceType, set[ResourceID]] = {}
        for raw_resource in res.result:
            resource = json.loads(raw_resource)
            resource_type_str = resource["resource_type"]
            if resource_type_str in ResourceType.values():
                res_type = ResourceType(resource_type_str)
                res_id = ResourceID(resource["unique_id"])
                result.setdefault(res_type, set()).add(res_id)

        return result

    @property
    def _raw_resources(self) -> list[SourceDefinition | SeedNode | ModelNode | SnapshotNode]:
        results: list[SourceDefinition | SeedNode | ModelNode | SnapshotNode] = []

        for resource_type, resource_ids in self.resource_ids.items():
            resource_by_id = self.manifest.sources if resource_type == ResourceType.SOURCE else self.manifest.nodes
            for res_id in resource_ids:
                raw_resource = resource_by_id[str(res_id)]
                results.append(raw_resource)

        return results

    def _pumpkin_project_vars(self, project_yml: dict):
        get_column_types_args = {
            str(resource.unique_id): [resource.database, resource.schema, resource.identifier]
            for resource in self._raw_resources
        }

        get_resource_root_paths_args = {
            # Don't put sources here, as sources are always defined in YML
            # SourceDefinition.path point to YML
            "model": project_yml.get("model-paths", ["models"]),
            "seed": project_yml.get("seed-paths", ["seeds"]),
            "snapshot": project_yml.get("snapshot-paths", ["snapshots"]),
        }

        return {
            "get_column_types_args": get_column_types_args,
            "get_resource_root_paths_args": get_resource_root_paths_args,
        }

    def _create_pumpkin_project(self) -> Path:
        """
        Creates fake DBT project with some important configurations copied to "vars" section.
        Allows hacking into DBT without using any internal DBT API.
        """
        src_macros_path = Path(__file__).parent / "macros"

        if not src_macros_path.exists() or not src_macros_path.is_dir():
            msg = f"Macros directory is not found or doesn't exist: {src_macros_path}"
            raise PumpkinError(msg)

        project_yml_path = Path(self.project_dir) / "dbt_project.yml"

        if not project_yml_path.exists() or not project_yml_path.is_file():
            msg = f"dbt_project.yml is not found or doesn't exist: {project_yml_path}"
            raise PumpkinError(msg)

        project_yml = self._yaml.load(project_yml_path)

        pumpkin_yml = {
            "name": "dbt_pumpkin",
            "version": "0.1.0",
            "profile": project_yml["profile"],
            # TODO: copy vars?
            "vars": self._pumpkin_project_vars(project_yml),
        }

        # OS may delete temp directory, keep fingers crossed
        pumpkin_dir_str = tempfile.mkdtemp(prefix="dbt_pumpkin_")
        pumpkin_dir = Path(pumpkin_dir_str)
        shutil.copytree(src_macros_path, pumpkin_dir / "macros")
        self._yaml.dump(pumpkin_yml, pumpkin_dir / "dbt_project.yml")

        return pumpkin_dir

    def _run_operation(self, operation_name: str) -> dict:
        pumpkin_dir = self._create_pumpkin_project()
        args = ["run-operation", operation_name, "--project-dir", str(pumpkin_dir)]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]

        jinja_log_messages: list[str] = []

        def event_callback(event: EventMsg):
            if event.info.name == "JinjaLogInfo":
                jinja_log_messages.append(event.info.msg)

        res: dbtRunnerResult = dbtRunner(callbacks=[event_callback]).invoke(args)

        if not res.success:
            raise res.exception

        if not jinja_log_messages:
            msg = f"No response retrieved from operation {operation_name}"
            raise PumpkinError(msg)

        return json.loads(jinja_log_messages[0])

    @cached_property
    def resource_tables(self) -> list[Table]:
        column_types_response = self._run_operation("get_column_types")
        result = []
        for res_id, columns in column_types_response.items():
            result.append(
                Table(
                    resource_id=ResourceID(res_id),
                    columns=[Column(name=c["name"], data_type=c["data_type"], description=None) for c in columns],
                )
            )

        return result

    @cached_property
    def resource_yaml_paths(self) -> dict[ResourceID, Path]:
        # It's possible to use vars, env_vars in DBT paths, so we have to evaluate using operation
        resource_root_paths_response: dict[str, list[str]] = self._run_operation("get_resource_root_paths")

        result: dict[ResourceID, Path] = {}
        for resource_type_str, root_paths in resource_root_paths_response.items():
            res_type = ResourceType(resource_type_str)
            res_ids: list[ResourceID] = self.resource_ids.get(res_type, [])
            res_name_to_id: dict[str, ResourceID] = {res_id.name: res_id for res_id in res_ids}
            for root_path_str in root_paths:
                root_path = Path(self.project_dir) / root_path_str
                for yml_path in root_path.rglob("*.yml"):
                    yml: dict = self._yaml.load(yml_path)
                    resources = yml.get(res_type.plural_name, [])
                    for resource in resources:
                        res_id = res_name_to_id.get(resource["name"], None)
                        if res_id:
                            result[res_id] = yml_path.relative_to(self.project_dir)

        # Source's path is a path to YAML file where resource is defined,
        # For other resource types path is a path to SQL or Python file
        for raw_resource in self._raw_resources:
            if ResourceType(raw_resource.resource_type) == ResourceType.SOURCE:
                resource_id = ResourceID(raw_resource.unique_id)
                result[resource_id] = Path(raw_resource.path)

        return result

    @property
    def resources(self) -> list[Resource]:
        results: list[Resource] = []

        for raw_resource in self._raw_resources:
            resource_id = ResourceID(raw_resource.unique_id)
            resource_type = ResourceType(raw_resource.resource_type)
            yaml_path = self.resource_yaml_paths.get(resource_id, None)

            results.append(
                Resource(
                    unique_id=resource_id,
                    name=raw_resource.name,
                    database=raw_resource.database,
                    schema=raw_resource.schema,
                    identifier=raw_resource.identifier,
                    type=resource_type,
                    yaml_path=yaml_path,
                    columns=[
                        Column(name=c.name, data_type=c.data_type, description=c.description)
                        for c in raw_resource.columns.values()
                    ],
                    config=ResourceConfig(),
                )
            )

        return results
