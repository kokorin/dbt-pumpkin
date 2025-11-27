import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from dbt_pumpkin.data import CaseFolding, Resource, ResourceConfig, ResourceType, Table, TableColumn
from dbt_pumpkin.exception import PumpkinError, UnexpectedValueError
from dbt_pumpkin.plan import (
    Action,
    AddResourceColumn,
    BootstrapResource,
    DeleteEmptyDescriptor,
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
        logger.info("Planning actions for %s resources", len(self._resources))

        actions: list[Action] = []
        path_resolver = PathResolver()

        for resource in self._resources:
            if resource.type == ResourceType.SOURCE:
                # sources can be initialized only manually
                continue

            if resource.yaml_path:
                logger.debug("Resource already bootstrapped: %s", resource.unique_id)
                continue

            if not resource.config or not resource.config.yaml_path_template:
                logger.warning(
                    "Resource has no YAML path defined: %s. Add dbt-pumpkin-path configuration property",
                    resource.unique_id,
                )
                continue

            logger.debug("Planned bootstrap action: %s", resource.unique_id)

            yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            actions.append(BootstrapResource(resource.type, resource.name, yaml_path))

        return Plan(actions)


class RelocationPlanner(ActionPlanner):
    def __init__(self, resources: list[Resource]):
        self._resources = resources

    def plan(self) -> Plan:
        logger.info("Planning actions for %s resources", len(self._resources))

        actions: list[Action] = []
        path_resolver = PathResolver()

        sources: dict[str, list[Resource]] = {}
        cleanup_paths: set[Path] = set()

        for resource in self._resources:
            if resource.type == ResourceType.SOURCE:
                # sources with the same source_name must be defined in one file
                sources.setdefault(resource.source_name, []).append(resource)
                continue

            if not resource.yaml_path:
                logger.warning(
                    "Resource has no YAML schema defined: %s. Run bootstrap command first",
                    resource.unique_id,
                )
                continue

            if not resource.config or not resource.config.yaml_path_template:
                logger.warning(
                    "Resource has no YAML path defined: %s. Add dbt-pumpkin-path configuration property",
                    resource.unique_id,
                )
                continue

            to_yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            if resource.yaml_path != to_yaml_path:
                logger.debug("Planned relocate action: %s", resource.unique_id)
                actions.append(RelocateResource(resource.type, resource.name, resource.yaml_path, to_yaml_path))
                cleanup_paths.add(resource.yaml_path)

        for source_name, source_tables in sources.items():
            # make sure all source's resources have exactly the same configuration
            configs = {r.config for r in source_tables}
            if len(configs) > 1:
                msg = f"Sources in {source_name} have different configurations: {configs}"
                raise PumpkinError(msg)

            config = configs.pop()

            if not config or not config.yaml_path_template:
                logger.warning(
                    "Source has no YAML path defined: %s. Add dbt-pumpkin-path configuration property", source_name
                )
                continue

            yaml_path = source_tables[0].yaml_path
            to_yaml_path = path_resolver.resolve(config.yaml_path_template, source_name, resource_path=None)

            if yaml_path != to_yaml_path:
                logger.debug("Planned relocate action: %s", source_name)
                actions.append(RelocateResource(ResourceType.SOURCE, source_name, yaml_path, to_yaml_path))
                cleanup_paths.add(yaml_path)

        actions += [DeleteEmptyDescriptor(to_cleanup) for to_cleanup in sorted(cleanup_paths)]

        return Plan(actions)


class SynchronizationPlanner(ActionPlanner):
    # SQL reserved words that require quoting
    # Includes ANSI SQL standard keywords and common database-specific keywords
    _RESERVED_WORDS = frozenset(
        {
            # DML/DDL commands
            "ALTER",
            "CREATE",
            "DELETE",
            "DROP",
            "GRANT",
            "INSERT",
            "REVOKE",
            "SELECT",
            "TRUNCATE",
            "UPDATE",
            "RENAME",
            "COMMENT",
            # Database objects
            "DATABASE",
            "SCHEMA",
            "TABLE",
            "VIEW",
            "INDEX",
            "SEQUENCE",
            "PROCEDURE",
            "FUNCTION",
            "TRIGGER",
            "COLUMN",
            # Transaction control
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "START",
            "BEGIN",
            "TRANSACTION",
            # Clauses
            "FROM",
            "WHERE",
            "HAVING",
            "GROUP",
            "ORDER",
            "BY",
            "LIMIT",
            "OFFSET",
            "PARTITION",
            "OVER",
            "WINDOW",
            "QUALIFY",
            "RETURNING",
            # Joins
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
            "OUTER",
            "NATURAL",
            "LATERAL",
            # Set operations
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "MINUS",
            # Logical operators
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXISTS",
            "BETWEEN",
            "LIKE",
            "IS",
            # Comparison and sorting
            "ASC",
            "DESC",
            "DISTINCT",
            "ALL",
            "ANY",
            "SOME",
            # Conditionals
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "IF",
            "ELSEIF",
            "NULLIF",
            "COALESCE",
            # Data types
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "DECIMAL",
            "NUMERIC",
            "FLOAT",
            "REAL",
            "DOUBLE",
            "CHAR",
            "VARCHAR",
            "TEXT",
            "BLOB",
            "CLOB",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "INTERVAL",
            "BOOLEAN",
            "BOOL",
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            # Constraints
            "PRIMARY",
            "FOREIGN",
            "REFERENCES",
            "KEY",
            "CHECK",
            "UNIQUE",
            "CONSTRAINT",
            "DEFAULT",
            "DEFERRABLE",
            "INITIALLY",
            "IMMEDIATE",
            "DEFERRED",
            "CASCADE",
            "RESTRICT",
            "NO",
            "ACTION",
            # Special values
            "NULL",
            "TRUE",
            "FALSE",
            "UNKNOWN",
            # Functions and keywords
            "CAST",
            "EXTRACT",
            "SUBSTRING",
            "TRIM",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "SESSION_USER",
            "SYSTEM_USER",
            "USER",
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            # Window functions
            "ROWS",
            "RANGE",
            "UNBOUNDED",
            "PRECEDING",
            "FOLLOWING",
            "CURRENT",
            # Other common keywords
            "AS",
            "ON",
            "USING",
            "INTO",
            "VALUES",
            "SET",
            "FOR",
            "TO",
            "WITH",
            "FETCH",
            "RECURSIVE",
            "COLLATE",
            "DESCRIBE",
            # Control flow (stored procedures)
            "LOOP",
            "WHILE",
            "REPEAT",
            "UNTIL",
            "DECLARE",
            "CURSOR",
            "OPEN",
            "CLOSE",
            # PostgreSQL specific (but common)
            "CONFLICT",
            "NOTHING",
            "EXCLUDED",
        }
    )

    def __init__(self, resources: list[Resource], tables: list[Table], case_folding: CaseFolding):
        self._resources = resources
        self._tables = tables
        self._case_folding = case_folding
        self._dont_quote_re = re.compile("^[a-zA-Z_][a-zA-Z0-9_]*$")

    def _quote(self, name: str) -> bool:
        from dbt_pumpkin.data import CaseFolding

        # First check for special characters that always require quoting
        if self._dont_quote_re.match(name) is None:
            return True

        # Check if it's a reserved word (case-insensitive check)
        if name.upper() in self._RESERVED_WORDS:
            return True

        # Apply case folding rules
        if self._case_folding == CaseFolding.PRESERVE:
            # Be safe and quote everything when case is preserved
            return True
        if self._case_folding == CaseFolding.UPPER:
            # Quote if column name differs from uppercase version
            return name != name.upper()
        if self._case_folding == CaseFolding.LOWER:
            # Quote if column name differs from lowercase version
            return name != name.lower()
        if self._case_folding == CaseFolding.UNKNOWN:
            # Fall back to always quoting when we can't detect
            return True

        raise UnexpectedValueError(CaseFolding, self._case_folding)

    def _column_type(self, column: TableColumn, config: ResourceConfig) -> str:
        if column.is_numeric and config.numeric_precision_and_scale or column.is_string and config.string_length:
            return column.data_type

        return column.dtype

    def _resource_plan(self, resource: Resource, table: Table) -> list[Action]:
        exact_name_match = False

        resource_column_ambiguity = {}
        for c in resource.columns:
            resource_column_ambiguity.setdefault(c.name.upper(), []).append(c.name)
        for original_names in resource_column_ambiguity.values():
            if len(original_names) == 1:
                continue
            logger.warning(
                "Resource %s contains ambiguous columns (ignore case): %s. Will use exact match",
                resource.name,
                original_names,
            )
            exact_name_match = True

        table_column_ambiguity = {}
        for c in table.columns:
            table_column_ambiguity.setdefault(c.name.upper(), []).append(c.name)
        for original_names in table_column_ambiguity.values():
            if len(original_names) == 1:
                continue
            logger.warning(
                "Table %s contains ambiguous columns (ignore case): %s. Will use exact match",
                resource.name,
                original_names,
            )
            exact_name_match = True

        def normalize_name(name: str) -> str:
            if exact_name_match:
                return name
            return name.upper()

        resource_column_by_normalized_name = {normalize_name(c.name): c for c in resource.columns}
        table_column_by_normalized_name = {normalize_name(c.name): c for c in table.columns}

        # Now we can look up column by uppercase

        result: list[Action] = []

        # resource column names AFTER applying all Add and Delete actions
        # this list will be modified during planning
        resource_column_names: list[str] = [c.name for c in resource.columns]

        for table_column in table.columns:
            resource_column = resource_column_by_normalized_name.get(normalize_name(table_column.name))
            column_data_type = self._column_type(table_column, resource.config)

            if not resource_column:
                logger.debug("Planned add column action: %s %s", table_column.name, resource.unique_id)
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
                logger.debug("Planned update column action: %s %s", table_column.name, resource.unique_id)
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
            table_column = table_column_by_normalized_name.get(normalize_name(resource_column.name))
            if not table_column:
                logger.debug("Planned delete column action: %s %s", resource_column.name, resource.unique_id)
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

        resource_column_normalized_names = [normalize_name(n) for n in resource_column_names]
        table_column_normalized_names = [normalize_name(c.name) for c in table.columns]

        if resource_column_normalized_names != table_column_normalized_names:
            logger.debug("Planned reorder column action: %s", resource.unique_id)
            column_order = [
                resource_column_by_normalized_name.get(normalize_name(c.name), c).name for c in table.columns
            ]
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
        logger.info("Planning actions for %s resources", len(self._resources))

        actions: list[Action] = []
        table_by_id = {t.resource_id: t for t in self._tables}

        for resource in self._resources:
            table = table_by_id.get(resource.unique_id, None)

            if not table:
                logger.warning("Table not found for resource: %s", resource.unique_id)
                continue
            if not resource.yaml_path:
                logger.warning(
                    "Resource has no YAML path defined: %s. Consider using bootstrap command first",
                    resource.unique_id,
                )
                continue

            actions += self._resource_plan(resource, table)

        return Plan(actions)
