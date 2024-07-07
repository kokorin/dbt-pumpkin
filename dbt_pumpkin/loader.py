from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from ruamel.yaml import YAML

from dbt_pumpkin.data import Resource, ResourceColumn, ResourceConfig, ResourceID, ResourceType, Table, TableColumn
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
        self._project_params = project_params
        self._resource_params = resource_params
        self._manifest: Manifest = None
        self._resource_ids: dict[ResourceType, set[ResourceID]] = None
        self._resources: list[Resource] = None
        self._tables: list[Table] = None
        self._yaml = YAML(typ="safe")

    def _do_load_manifest(self) -> Manifest:
        logger.debug("Parsing manifest")

        args = ["parse", *self._project_params.to_args()]
        logger.debug("Command line: %s", args)

        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            logger.error("Parsing manifest failed, dbt exception %s", res.exception)
            raise res.exception

        result: Manifest = res.result

        logger.info("Manifest parsed. Sources: %s, Nodes: %s", len(result.sources), len(result.nodes))

        return result

    def load_manifest(self) -> Manifest:
        if self._manifest is None:
            self._manifest = self._do_load_manifest()
        return self._manifest

    def _do_select_resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
        """
        Returns a dictionary mapping resource type to a set of resource identifiers
        """
        manifest = self.load_manifest()
        logger.debug("Listing selected resources")
        args = ["list", *self._project_params.to_args(), *self._resource_params.to_args(), "--output", "json"]

        logger.debug("Command line: %s", args)
        res: dbtRunnerResult = dbtRunner(manifest).invoke(args)

        if not res.success:
            logger.error("Listing failed, dbt exception %s", res.exception)
            raise res.exception

        result: dict[ResourceType, set[ResourceID]] = {}
        resource_counter = Counter()

        for raw_resource in res.result:
            resource = json.loads(raw_resource)
            resource_type_str = resource["resource_type"]
            if resource_type_str in ResourceType.values():
                res_type = ResourceType(resource_type_str)
                res_id = ResourceID(resource["unique_id"])

                result.setdefault(res_type, set()).add(res_id)
                resource_counter[str(res_type)] += 1

                logger.debug("Selected %s %s", res_type, res_id)

        logger.info("Selected: %s", resource_counter)

        return result

    def select_resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
        if self._resource_ids is None:
            self._resource_ids = self._do_select_resource_ids()

        return self._resource_ids

    @property
    def _raw_resources(self) -> list[SourceDefinition | SeedNode | ModelNode | SnapshotNode]:
        manifest = self.load_manifest()
        results: list[SourceDefinition | SeedNode | ModelNode | SnapshotNode] = []

        selected_resource_ids = self.select_resource_ids()

        for res_type, res_ids in selected_resource_ids.items():
            resource_by_id = manifest.sources if res_type == ResourceType.SOURCE else manifest.nodes
            for res_id in res_ids:
                raw_resource = resource_by_id[str(res_id)]
                results.append(raw_resource)

        return results

    def _do_select_resources(self) -> list[Resource]:
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
                        ResourceColumn(name=c.name, quote=c.quote, data_type=c.data_type, description=c.description)
                        for c in raw_resource.columns.values()
                    ],
                    config=config,
                )
            )

        return results

    def select_resources(self) -> list[Resource]:
        if self._resources is None:
            self._resources = self._do_select_resources()

        return self._resources

    def locate_project_dir(self) -> Path:
        """
        Locates project directory according to DBT project look up rules.

        Helps in cases when we need to read some files from DBT project.
        """

        return Path(
            self._project_params.project_dir or os.environ.get("DBT_PROJECT_DIR", None) or default_project_dir()
        )

    def _create_pumpkin_project(self, project_vars: dict[str, any]) -> Path:
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
            "vars": project_vars,
        }

        # OS may delete temp directory, keep fingers crossed
        pumpkin_dir_str = tempfile.mkdtemp(prefix="dbt_pumpkin_")
        pumpkin_dir = Path(pumpkin_dir_str)
        target_macros_dir = pumpkin_dir / "macros"

        logger.debug("Copying macros to %s", target_macros_dir)
        shutil.copytree(src_macros_path, target_macros_dir)

        target_project_yml = pumpkin_dir / "dbt_project.yml"
        logger.debug("Creating temp DBT project at %s", target_project_yml)
        self._yaml.dump(pumpkin_yml, target_project_yml)

        logger.debug("Created temp DBT project %s", pumpkin_dir)

        return pumpkin_dir

    def _run_operation(
        self, operation_name: str, project_vars: dict[str, any] | None, result_callback: Callable[[any], None]
    ):
        pumpkin_dir = self._create_pumpkin_project(project_vars)

        project_params = self._project_params.with_project_dir(str(pumpkin_dir))

        args = ["run-operation", operation_name, *project_params.to_args()]
        logger.debug("Command line: %s", args)

        def event_callback(event: EventMsg):
            if event.info.name in {"JinjaLogInfo", "JinjaLogDebug"}:
                try:
                    potential_result = json.loads(str(event.info.msg))
                    if operation_name in potential_result:
                        result_callback(potential_result[operation_name])
                    else:
                        logger.debug("Ignoring potential result: no '%s' key: %s", operation_name, potential_result)
                except Exception:
                    logger.exception("Failed to parse potential result %s", event.info)

        res: dbtRunnerResult = dbtRunner(callbacks=[event_callback]).invoke(args)

        if not res.success:
            msg = f"Run operation failure: {operation_name}. Exception: {res.exception}"
            raise PumpkinError(msg)

    def _do_lookup_tables(self) -> list[Table]:
        logger.info("Looking up tables")

        raw_resources = self._raw_resources
        project_vars = {
            "lookup_tables_args": {
                str(resource.unique_id): [resource.database, resource.schema, resource.identifier]
                for resource in raw_resources
            },
        }

        tables: list[Table] = []
        processed: list[str] = []

        def on_result(result: dict):
            resource_id: str = result["resource_id"]
            processed.append(resource_id)
            logger.info("Processing %s / %s: %s", len(processed), len(raw_resources), resource_id)

            columns: list[dict] = result["columns"]
            # If tables doesn't exist columns is None
            if not columns:
                logger.warning("Relation doesn't exist: %s", resource_id)
                return

            tables.append(
                Table(
                    resource_id=ResourceID(resource_id),
                    columns=[TableColumn(**c) for c in columns],
                )
            )

        self._run_operation("lookup_tables", project_vars, on_result)

        logger.info("Found %s tables", len(tables))

        return tables

    def lookup_tables(self):
        if self._tables is None:
            self._tables = self._do_lookup_tables()
        return self._tables
