import logging
from abc import ABC, abstractmethod

from dbt_pumpkin.canon import NamingCanon
from dbt_pumpkin.data import Resource, ResourceColumn, ResourceType, Table
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
    def __init__(self, resources: list[Resource], tables: list[Table], naming_canon: NamingCanon):
        self._resources = resources
        self._tables = tables
        self._naming_canon = naming_canon

    def _resource_plan(self, resource: Resource, table: Table) -> list[Action]:
        resource_column_by_table_column_name: dict[str, ResourceColumn] = {}
        table_column_name_by_resource_column_name: dict[str, str] = {}

        proceed = True
        for resource_column in resource.columns:
            if resource_column.quote:
                # columns, which need quotation, should be reported by DB by exact same name
                table_column_name = resource_column.name
            elif self._naming_canon.can_canonize(resource_column.name):
                table_column_name = self._naming_canon.canonize(resource_column.name)
            else:
                logger.warning(
                    "Resource %s column '%s' should have 'quote' property set to True",
                    resource.unique_id,
                    resource_column.name,
                )
                proceed = False
                break

            ambiguous_column = resource_column_by_table_column_name.get(table_column_name)
            if ambiguous_column:
                logger.warning(
                    "Resource %s columns '%s' and '%s' are ambiguous. Set 'quote' property to True or rename",
                    resource.unique_id,
                    ambiguous_column.name,
                    resource_column.name,
                )
                proceed = False
                break

            resource_column_by_table_column_name[table_column_name] = resource_column
            table_column_name_by_resource_column_name[resource_column.name] = table_column_name

        if not proceed:
            logger.warning("Can't plan actions for resource %s", resource.unique_id)
            return []

        # Now 'columns' variable contains database' column names mapped to resource column
        result: list[Action] = []

        # resource column names AFTER applying all Add and Delete actions
        resource_column_order: list[str] = [c.name for c in resource.columns]
        # resource column name ordered as columns are in a table
        # this list should contain not canonized names (like in YAML) if a column is specified in resource
        table_column_order: list[str] = []

        for table_column in table.columns:
            if table_column.name not in resource_column_by_table_column_name:
                result.append(
                    AddResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        column_name=table_column.name,
                        column_quote=not self._naming_canon.can_canonize(table_column.name),
                        column_type=table_column.data_type,
                    )
                )
                resource_column_order.append(table_column.name)
                table_column_order.append(table_column.name)
                continue

            resource_column = resource_column_by_table_column_name[table_column.name]
            if table_column.data_type != resource_column.data_type:
                result.append(
                    UpdateResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        # Careful, columns in YAML may be named non-canonically.
                        # We must provide column_name as it is in YAML
                        column_name=resource_column.name,
                        column_type=table_column.data_type,
                    )
                )

            table_column_order.append(resource_column.name)

        table_column_names = {c.name for c in table.columns}
        for resource_column in resource.columns:
            table_column_name = table_column_name_by_resource_column_name.get(resource_column.name)
            if table_column_name not in table_column_names:
                result.append(
                    DeleteResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        # Careful, columns in YAML may be named non-canonically.
                        # We must provide column_name as it is in YAML
                        column_name=resource_column.name,
                    )
                )
                resource_column_order.remove(resource_column.name)

        if resource_column_order != table_column_order:
            result.append(
                ReorderResourceColumns(
                    resource_type=resource.type,
                    resource_name=resource.name,
                    path=resource.yaml_path,
                    source_name=resource.source_name,
                    columns_order=table_column_order,
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

            actions += self._resource_plan(resource, table)

        return Plan(actions)
