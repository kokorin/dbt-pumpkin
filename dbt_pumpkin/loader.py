from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from dbt.cli.main import (
    EventMsg,
    Manifest,
    dbtRunner,
    dbtRunnerResult,
)
from dbt.cli.resolvers import default_project_dir
from ruamel.yaml import YAML

from dbt_pumpkin.data import (
    CaseFolding,
    Resource,
    ResourceColumn,
    ResourceConfig,
    ResourceID,
    ResourceType,
    Table,
    TableColumn,
    YamlFormat,
)
from dbt_pumpkin.exception import PumpkinError

if TYPE_CHECKING:
    from dbt.contracts.graph.nodes import ModelNode, SeedNode, SnapshotNode, SourceDefinition

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
        self._case_folding: CaseFolding = None
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

    def _do_list_all_resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
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

        for raw_resource in res.result:
            resource = json.loads(raw_resource)
            resource_type_str = resource["resource_type"]
            if resource_type_str in ResourceType.values():
                res_type = ResourceType(resource_type_str)
                res_id = ResourceID(resource["unique_id"])

                result.setdefault(res_type, set()).add(res_id)

                logger.debug("Selected %s", res_id)

        return result

    def list_all_resource_ids(self) -> dict[ResourceType, set[ResourceID]]:
        """
        Returns all Resource Identifiers (grouped by Resource type) defined in DBT project (including packages)
        """
        if self._resource_ids is None:
            self._resource_ids = self._do_list_all_resource_ids()

        return self._resource_ids

    def select_raw_resources(self) -> list[SourceDefinition | SeedNode | ModelNode | SnapshotNode]:
        """
        Returns a list of raw Resources that can be processed by dbt-pumpkin.

        Resources defined in a package or having YAML description defined in a package are filtered out.
        """
        manifest = self.load_manifest()
        results: list[SourceDefinition | SeedNode | ModelNode | SnapshotNode] = []

        project_name = self.get_project_name()
        project_path_path_prefix = project_name + "://"
        raw_resources_by_type = {
            ResourceType.SOURCE: manifest.sources,
            ResourceType.MODEL: manifest.nodes,
            ResourceType.SEED: manifest.nodes,
            ResourceType.SNAPSHOT: manifest.nodes,
        }
        for res_type, res_ids in self.list_all_resource_ids().items():
            for res_id in res_ids:
                raw_resource = raw_resources_by_type[res_type][str(res_id)]
                if raw_resource.package_name != project_name:
                    logger.debug(
                        "Skipping resource %s defined in package %s", raw_resource.unique_id, raw_resource.package_name
                    )
                    continue
                if raw_resource.patch_path and not raw_resource.patch_path.startswith(project_path_path_prefix):
                    logger.warning(
                        "Skipping resource %s: YAML descriptor is not in root package %s",
                        raw_resource.unique_id,
                        raw_resource.package_name,
                    )
                    continue
                results.append(raw_resource)

        return results

    def _do_select_resources(self) -> list[Resource]:
        results: list[Resource] = []

        logger.info("Selecting resources")

        resource_counter = Counter()

        for raw_resource in self.select_raw_resources():
            resource_id = ResourceID(raw_resource.unique_id)
            resource_type = ResourceType(raw_resource.resource_type)
            resource_counter[str(resource_type)] += 1

            source_name: str = None
            path: Path = None
            yaml_path: Path = None

            if resource_type == ResourceType.SOURCE:
                source_name = raw_resource.source_name
                yaml_path = Path(raw_resource.original_file_path)
            else:
                path = Path(raw_resource.original_file_path)
                if raw_resource.patch_path:
                    fixed_patch_path = raw_resource.patch_path.split("://")[-1]
                    yaml_path = Path(fixed_patch_path)

            pumpkin_types = raw_resource.config.get("dbt-pumpkin-types", {})
            config: ResourceConfig = ResourceConfig(
                yaml_path_template=raw_resource.config.get("dbt-pumpkin-path", None),
                numeric_precision_and_scale=pumpkin_types.get("numeric-precision-and-scale", False),
                string_length=pumpkin_types.get("string-length", False),
            )

            # Extract version for versioned models
            version = getattr(raw_resource, "version", None)

            logger.info("Selected %s", resource_id)

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
                    version=version,
                )
            )

        logger.info("Selected: %s", resource_counter)

        return results

    def select_resources(self) -> list[Resource]:
        """
        Returns a list of Resources that can be processed by dbt-pumpkin.

        Resources defined in a package or having YAML description defined in a package are filtered out.
        """
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
        Creates fake DBT project with provided "vars" section.
        Allows hacking into DBT without using any internal DBT API.
        """
        src_macros_path = Path(__file__).parent / "macros"

        if not src_macros_path.exists() or not src_macros_path.is_dir():
            msg = f"Macros directory is not found or doesn't exist: {src_macros_path}"
            raise PumpkinError(msg)

        project_yml = self._parse_project_yml()

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
        logger.debug("Creating temp dbt_project.yml at %s", target_project_yml)
        self._yaml.dump(pumpkin_yml, target_project_yml)

        logger.debug("Created temp DBT project %s", pumpkin_dir)

        return pumpkin_dir

    def _parse_project_yml(self) -> dict[str, any]:
        logger.debug("Parsing dbt_project.yml")

        project_yml_path = self.locate_project_dir() / "dbt_project.yml"
        if not project_yml_path.exists() or not project_yml_path.is_file():
            msg = f"dbt_project.yml not found: {project_yml_path}"
            raise PumpkinError(msg)
        return self._yaml.load(project_yml_path)

    def get_project_name(self) -> str:
        # TODO: after dropping DBT 1.5 support we can get project name from Manifest
        # self.load_manifest().metadata.project_name
        return self._parse_project_yml()["name"]

    def detect_yaml_format(self) -> YamlFormat | None:
        pumpkin_var = self._parse_project_yml().get("vars", {}).get("dbt-pumpkin")
        if pumpkin_var is None:
            return None
        if not isinstance(pumpkin_var, dict):
            msg = "YAML property is not an object: vars.dbt-pumpkin"
            raise PumpkinError(msg)

        yaml_format = pumpkin_var.get("yaml_format")

        if not yaml_format and "yaml" in pumpkin_var:
            yaml_format = pumpkin_var.get("yaml")
            logger.warning("Variable 'yaml' was renamed to 'yaml_format'.")

        if not yaml_format:
            logger.info("No YAML format set in DBT project vars, using default")
            return None

        return YamlFormat.from_dict(yaml_format)

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
                except KeyboardInterrupt as e:
                    raise e  # noqa: TRY201
                except Exception:  # noqa: BLE001
                    # We DO need to catch any exceptions while handling events
                    # otherwise dbtRunner will exit with exception
                    logger.warning("Failed to parse potential result %s", event.info)

        res: dbtRunnerResult = dbtRunner(callbacks=[event_callback]).invoke(args)

        if not res.success:
            msg = f"Run operation failure: {operation_name}. Exception: {res.exception}"
            raise PumpkinError(msg)

    def _do_lookup_tables(self) -> list[Table]:
        logger.info("Looking up tables")

        raw_resources = self.select_raw_resources()
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

    def _do_detect_case_folding(self) -> CaseFolding:
        """
        Detect database identifier case folding behavior.

        Uses a test query with known case variations to determine how the database
        handles unquoted identifiers.
        """
        logger.info("Detecting database case folding behavior")

        project_vars = {
            "detect_case_folding": True,
        }

        detected_folding: CaseFolding | None = None

        def on_result(result: dict):
            nonlocal detected_folding

            columns: list[str] = result.get("columns", [])
            logger.debug("Test query returned columns: %s", columns)

            if len(columns) != 2:  # noqa: PLR2004
                logger.warning("Unexpected result from case folding detection query")
                detected_folding = CaseFolding.UNKNOWN
                return

            # Test columns were: test_lower, TEST_UPPER
            lower_col = columns[0]
            upper_col = columns[1]

            if lower_col == "TEST_LOWER" and upper_col == "TEST_UPPER":
                detected_folding = CaseFolding.UPPER
                logger.info("Detected case folding: UPPER (e.g., Snowflake, Oracle)")
            elif lower_col == "test_lower" and upper_col == "test_upper":
                detected_folding = CaseFolding.LOWER
                logger.info("Detected case folding: LOWER (e.g., PostgreSQL, Redshift)")
            elif lower_col == "test_lower" and upper_col == "TEST_UPPER":
                detected_folding = CaseFolding.PRESERVE
                logger.info("Detected case folding: PRESERVE (e.g., some MySQL configurations)")
            else:
                logger.warning("Unexpected case folding behavior: %s, %s", lower_col, upper_col)
                detected_folding = CaseFolding.UNKNOWN

        try:
            self._run_operation("detect_case_folding", project_vars, on_result)
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to detect case folding behavior: %s", e)
            detected_folding = CaseFolding.UNKNOWN

        if detected_folding is None:
            logger.warning("No result from case folding detection, falling back to UNKNOWN")
            detected_folding = CaseFolding.UNKNOWN

        return detected_folding

    def detect_case_folding(self) -> CaseFolding:
        """Get the database's case folding behavior, detecting it on first call."""
        if self._case_folding is None:
            self._case_folding = self._do_detect_case_folding()
        return self._case_folding
