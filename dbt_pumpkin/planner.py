import logging
import re
from abc import ABC, abstractmethod

from dbt_pumpkin.data import Resource, ResourceColumn, ResourceConfig, ResourceType, Table, TableColumn
from dbt_pumpkin.exception import PumpkinError
from dbt_pumpkin.plan import (
    Action,
    AddResourceColumn,
    BootstrapResource,
    DeleteResourceColumn,
    Plan,
    RelocateResource,
    ReorderResourceColumns,
    UpdateResourceColumn,
)
from dbt_pumpkin.resolver import PathResolver

logger = logging.getLogger(__name__)


class ActionPlanner(ABC):
    @abstractmethod
    def plan(self) -> Plan:
        raise NotImplementedError


class BootstrapPlanner(ActionPlanner):
    def __init__(self, resources: list[Resource]):
        self._resources = resources

    def plan(self) -> Plan:
        actions: list[Action] = []
        path_resolver = PathResolver()

        for resource in self._resources:
            if resource.type == ResourceType.SOURCE:
                # sources can be initialized only manually
                continue

            if resource.yaml_path:
                # Resource already initialized
                continue

            if not resource.config or not resource.config.yaml_path_template:
                logger.warning(
                    "Resource %s %s has no YAML path defined, add dbt-pumpkin-path configuration property",
                    resource.type,
                    resource.name,
                )
                continue

            yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            actions.append(BootstrapResource(resource.type, resource.name, yaml_path))

        return Plan(actions)


class RelocationPlanner(ActionPlanner):
    def __init__(self, resources: list[Resource]):
        self._resources = resources

    def plan(self) -> Plan:
        actions: list[Action] = []
        path_resolver = PathResolver()

        sources: dict[str, list[Resource]] = {}

        for resource in self._resources:
            if resource.type == ResourceType.SOURCE:
                # sources with the same source_name must be defined in one file
                sources.setdefault(resource.source_name, []).append(resource)
                continue

            if not resource.yaml_path:
                logger.warning(
                    "Resource %s %s has no YAML schema definition, run bootstrap command instead",
                    resource.type,
                    resource.name,
                )
                continue

            if not resource.config or not resource.config.yaml_path_template:
                logger.warning(
                    "Resource %s %s has no YAML path defined, add dbt-pumpkin-path configuration property",
                    resource.type,
                    resource.name,
                )
                continue

            to_yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            if resource.yaml_path != to_yaml_path:
                actions.append(RelocateResource(resource.type, resource.name, resource.yaml_path, to_yaml_path))

        for source_name, source_tables in sources.items():
            # make sure all source's resources have exactly the same configuration
            configs = {r.config for r in source_tables}
            if len(configs) > 1:
                msg = f"Sources in {source_name} have different configurations: {configs}"
                raise PumpkinError(msg)

            config = configs.pop()

            if not config or not config.yaml_path_template:
                logger.warning(
                    "Source %s has no YAML path defined, add dbt-pumpkin-path configuration property", source_name
                )
                continue

            yaml_path = source_tables[0].yaml_path
            to_yaml_path = path_resolver.resolve(config.yaml_path_template, source_name, resource_path=None)

            if yaml_path != to_yaml_path:
                actions.append(RelocateResource(ResourceType.SOURCE, source_name, yaml_path, to_yaml_path))

        return Plan(actions)


class SynchronizationPlanner(ActionPlanner):
    def __init__(self, resources: list[Resource], tables: list[Table]):
        self._resources = resources
        self._tables = tables
        self._dont_quote_re = re.compile("^[a-zA-Z_][a-zA-Z0-9_]*$")

    def _quote(self, name: str) -> bool:
        return self._dont_quote_re.match(name) is None

    def _column_type(self, column: TableColumn, config: ResourceConfig) -> str:
        if column.is_numeric and config.numeric_precision_and_scale or column.is_string and config.string_length:
            return column.data_type

        return column.dtype

    def _resource_plan(self, resource: Resource, table: Table) -> list[Action]:
        resource_column_by_uppercase_name: dict[str, ResourceColumn] = {c.name.upper(): c for c in resource.columns}
        if len(resource_column_by_uppercase_name) != len(resource.columns):
            logger.warning("Resource %s contains ambiguous columns (ignore case)", resource.name)
            return []

        table_column_name_by_uppercase_name: dict[str, TableColumn] = {c.name.upper(): c for c in table.columns}
        if len(table_column_name_by_uppercase_name) != len(table.columns):
            logger.warning("Table %s contains ambiguous columns (ignore case)", resource.name)
            return []

        # Now we can look up column by uppercase

        result: list[Action] = []

        # resource column names AFTER applying all Add and Delete actions
        # this list will be modified during planning
        resource_column_names: list[str] = [c.name for c in resource.columns]

        for table_column in table.columns:
            resource_column = resource_column_by_uppercase_name.get(table_column.name.upper())
            column_data_type = self._column_type(table_column, resource.config)

            if not resource_column:
                result.append(
                    AddResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        column_name=table_column.name,
                        column_quote=self._quote(table_column.name),
                        column_type=column_data_type,
                    )
                )
                resource_column_names.append(table_column.name)
                continue

            if resource_column.data_type is None or column_data_type.lower() != resource_column.data_type.lower():
                result.append(
                    UpdateResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        column_name=resource_column.name,
                        column_type=column_data_type,
                    )
                )

        for resource_column in resource.columns:
            table_column = table_column_name_by_uppercase_name.get(resource_column.name.upper())
            if not table_column:
                result.append(
                    DeleteResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        column_name=resource_column.name,
                    )
                )
                resource_column_names.remove(resource_column.name)

        resource_column_uppercase_names = [n.upper() for n in resource_column_names]
        table_column_uppercase_names = [c.name.upper() for c in table.columns]

        if resource_column_uppercase_names != table_column_uppercase_names:
            column_order = [resource_column_by_uppercase_name.get(c.name.upper(), c).name for c in table.columns]
            result.append(
                ReorderResourceColumns(
                    resource_type=resource.type,
                    resource_name=resource.name,
                    path=resource.yaml_path,
                    source_name=resource.source_name,
                    columns_order=column_order,
                )
            )

        return result

    def plan(self) -> Plan:
        actions: list[Action] = []

        table_by_id = {t.resource_id: t for t in self._tables}

        for resource in self._resources:
            table = table_by_id.get(resource.unique_id, None)

            if not table:
                logger.warning("Table not found for resource: %s", resource.unique_id)
                continue
            if not resource.yaml_path:
                logger.warning(
                    "Resource %s %s has no YAML path defined, consider using bootstrap command",
                    resource.type,
                    resource.unique_id,
                )
                continue

            actions += self._resource_plan(resource, table)

        return Plan(actions)
