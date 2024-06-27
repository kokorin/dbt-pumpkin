from __future__ import annotations

import json
import logging
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
    from dbt_pumpkin.params import ProjectParams, ResourceParams

logger = logging.getLogger(__name__)


class ResourceLoader:
    def __init__(self, project_params: ProjectParams, resource_params: ResourceParams) -> None:
        self.project_params = project_params
        self.resource_params = resource_params
        self._yaml = YAML(typ="safe")

    @cached_property
    def manifest(self) -> Manifest:
        args = ["parse", *self.project_params.to_args()]

        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            raise res.exception

        return res.result

    @cached_property
    def resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
        """
        Returns a dictionary mapping resource type to a set of resource identifiers
        """
        args = ["list", *self.project_params.to_args(), *self.resource_params.to_args(), "--output", "json"]

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

    def _pumpkin_project_vars(self):
        get_column_types_args = {
            str(resource.unique_id): [resource.database, resource.schema, resource.identifier]
            for resource in self._raw_resources
        }

        return {
            "get_column_types_args": get_column_types_args,
        }

    def locate_project_dir(self) -> Path:
        # Project Directory must be set, as we rely on it in some methods
        return Path(self.project_params.project_dir or os.environ.get("DBT_PROJECT_DIR", None) or default_project_dir())

    def _create_pumpkin_project(self) -> Path:
        """
        Creates fake DBT project with some important configurations copied to "vars" section.
        Allows hacking into DBT without using any internal DBT API.
        """
        src_macros_path = Path(__file__).parent / "macros"

        if not src_macros_path.exists() or not src_macros_path.is_dir():
            msg = f"Macros directory is not found or doesn't exist: {src_macros_path}"
            raise PumpkinError(msg)

        project_yml_path = self.locate_project_dir() / "dbt_project.yml"

        if not project_yml_path.exists() or not project_yml_path.is_file():
            msg = f"dbt_project.yml is not found or doesn't exist: {project_yml_path}"
            raise PumpkinError(msg)

        project_yml = self._yaml.load(project_yml_path)

        pumpkin_yml = {
            "name": "dbt_pumpkin",
            "version": "0.1.0",
            "profile": project_yml["profile"],
            # TODO: copy vars?
            "vars": self._pumpkin_project_vars(),
        }

        # OS may delete temp directory, keep fingers crossed
        pumpkin_dir_str = tempfile.mkdtemp(prefix="dbt_pumpkin_")
        pumpkin_dir = Path(pumpkin_dir_str)
        shutil.copytree(src_macros_path, pumpkin_dir / "macros")
        self._yaml.dump(pumpkin_yml, pumpkin_dir / "dbt_project.yml")

        return pumpkin_dir

    def _run_operation(self, operation_name: str) -> dict:
        pumpkin_dir = self._create_pumpkin_project()
        project_params = self.project_params.with_project_dir(str(pumpkin_dir))

        args = ["run-operation", operation_name, *project_params.to_args()]

        jinja_log_messages: list[str] = []

        def event_callback(event: EventMsg):
            if event.info.name == "JinjaLogInfo":
                jinja_log_messages.append(event.info.msg)

        res: dbtRunnerResult = dbtRunner(callbacks=[event_callback]).invoke(args)

        if not res.success:
            logger.exception("Failed to load resources", exc_info=res.exception)

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

    @property
    def resources(self) -> list[Resource]:
        results: list[Resource] = []

        for raw_resource in self._raw_resources:
            resource_id = ResourceID(raw_resource.unique_id)
            resource_type = ResourceType(raw_resource.resource_type)

            source_name: str = None
            path: Path = None
            yaml_path: Path = None

            if resource_type == ResourceType.SOURCE:
                source_name = raw_resource.source_name
                yaml_path = Path(raw_resource.original_file_path)
            else:
                path = Path(raw_resource.original_file_path)
                if raw_resource.patch_path:
                    # path_path starts with "project_name://", we just remove it
                    # DBT 1.5 has no manifest.metadata.project_name, so we use resource FQN which starts with project name
                    # patch_path_prefix = self.manifest.metadata.project_name + "://"
                    patch_path_prefix = raw_resource.fqn[0] + "://"
                    fixed_patch_path = raw_resource.patch_path.removeprefix(patch_path_prefix)
                    yaml_path = Path(fixed_patch_path)

            config: ResourceConfig = ResourceConfig(
                yaml_path_template=raw_resource.config.get("dbt-pumpkin-path", None)
            )

            results.append(
                Resource(
                    unique_id=resource_id,
                    name=raw_resource.name,
                    source_name=source_name,
                    database=raw_resource.database,
                    schema=raw_resource.schema,
                    identifier=raw_resource.identifier,
                    type=resource_type,
                    path=path,
                    yaml_path=yaml_path,
                    columns=[
                        Column(name=c.name, data_type=c.data_type, description=c.description)
                        for c in raw_resource.columns.values()
                    ],
                    config=config,
                )
            )

        return results
