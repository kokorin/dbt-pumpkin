from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from dbt_pumpkin.data import CaseFolding, Resource, ResourceColumn, ResourceConfig, ResourceType, Table, TableColumn
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
    # fmt: off
    # SQL reserved words that require quoting
    # Includes ANSI SQL standard keywords and common database-specific keywords
    _RESERVED_WORDS = frozenset({
        # DML/DDL commands
        "ALTER", "CREATE", "DELETE", "DROP", "GRANT", "INSERT", "REVOKE", "SELECT", "TRUNCATE", "UPDATE", "RENAME",
        "COMMENT",
        # Database objects
        "DATABASE", "SCHEMA", "TABLE", "VIEW", "INDEX", "SEQUENCE", "PROCEDURE", "FUNCTION", "TRIGGER", "COLUMN",
        # Transaction control
        "COMMIT", "ROLLBACK", "SAVEPOINT", "START", "BEGIN", "TRANSACTION",
        # Clauses
        "FROM", "WHERE", "HAVING", "GROUP", "ORDER", "BY", "LIMIT", "OFFSET", "PARTITION", "OVER", "WINDOW", "QUALIFY",
        "RETURNING",
        # Joins
        "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "NATURAL", "LATERAL",
        # Set operations
        "UNION", "INTERSECT", "EXCEPT", "MINUS",
        # Logical operators
        "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS",
        # Comparison and sorting
        "ASC", "DESC", "DISTINCT", "ALL", "ANY", "SOME",
        # Conditionals
        "CASE", "WHEN", "THEN", "ELSE", "END", "IF", "ELSEIF", "NULLIF", "COALESCE",
        # Data types
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE",
        "CHAR", "VARCHAR", "TEXT", "BLOB", "CLOB", "DATE", "TIME", "TIMESTAMP", "INTERVAL", "BOOLEAN", "BOOL",
        "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
        # Constraints
        "PRIMARY", "FOREIGN", "REFERENCES", "KEY", "CHECK", "UNIQUE", "CONSTRAINT", "DEFAULT",
        "DEFERRABLE", "INITIALLY", "IMMEDIATE", "DEFERRED", "CASCADE", "RESTRICT", "NO", "ACTION",
        # Special values
        "NULL", "TRUE", "FALSE", "UNKNOWN",
        # Functions and keywords
        "CAST", "EXTRACT", "SUBSTRING", "TRIM", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "USER", "COUNT", "SUM", "AVG", "MIN", "MAX",
        # Window functions
        "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT",
        # Other common keywords
        "AS", "ON", "USING", "INTO", "VALUES", "SET", "FOR", "TO", "WITH", "FETCH", "RECURSIVE", "COLLATE", "DESCRIBE",
        # Control flow (stored procedures)
        "LOOP", "WHILE", "REPEAT", "UNTIL", "DECLARE", "CURSOR", "OPEN", "CLOSE",
        # PostgreSQL specific (but common)
        "CONFLICT", "NOTHING", "EXCLUDED",
    })

    # fmt: on

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
        if (column.is_numeric and config.numeric_precision_and_scale) or (column.is_string and config.string_length):
            return column.data_type

        return column.dtype

    def _fold_name(self, name: str, *, quoted: bool) -> str:
        """Apply case folding to a name (simulates how DB stores unquoted identifiers)."""
        if quoted:
            return name
        if self._case_folding == CaseFolding.UPPER:
            return name.upper()
        if self._case_folding == CaseFolding.LOWER:
            return name.lower()
        # PRESERVE and UNKNOWN - keep as-is
        return name

    def _find_table_column(self, name: str, *, quoted: bool, table_columns: list[TableColumn]) -> TableColumn | None:
        """Find matching table column for a resource column.

        Args:
            name: Resource column name
            quoted: Whether resource column is quoted
            table_columns: List of table columns

        Returns:
            Matching TableColumn or None if not found
        """
        # Step 1: If quoted, match by exact name only
        if quoted:
            for tc in table_columns:
                if tc.name == name:
                    return tc
            return None

        # Step 2: Try exact name (no folding) - only if unique
        exact_matches = [tc for tc in table_columns if tc.name == name]
        if len(exact_matches) == 1:
            return exact_matches[0]

        # Step 3: Try folded name - only if unique
        folded_name = self._fold_name(name, quoted=False)
        folded_matches = [tc for tc in table_columns if tc.name == folded_name]
        if len(folded_matches) == 1:
            return folded_matches[0]

        # Step 4: Fallback to case-insensitive lookup - only if unique
        name_upper = folded_name.upper()
        ci_matches = [tc for tc in table_columns if tc.name.upper() == name_upper]
        if len(ci_matches) == 1:
            return ci_matches[0]

        # No match or ambiguous (multiple matches)
        return None

    def _resource_plan(self, resource: Resource, table: Table) -> list[Action]:
        result: list[Action] = []
        resource_columns = resource.columns.copy()
        matched_table_columns: set[str] = set()

        # Step 1: Delete - iterate resource by position, find matching table column
        # Two passes: quoted first (explicit user intent), then unquoted
        # Enumerate in reversed order to simplify deletion
        for only_quoted in (True, False):
            for idx, resource_column in reversed(list(enumerate(resource_columns))):
                if (resource_column.quote is True) != only_quoted:
                    continue

                table_column = self._find_table_column(
                    resource_column.name, quoted=resource_column.quote or False, table_columns=table.columns
                )

                if table_column and table_column.name not in matched_table_columns:
                    # Match found and not yet matched - keep this column
                    matched_table_columns.add(table_column.name)
                else:
                    # No match or already matched - delete
                    logger.debug("Planned delete column action: %s %s", resource_column.name, resource.unique_id)
                    result.append(
                        DeleteResourceColumn(
                            resource_type=resource.type,
                            resource_name=resource.name,
                            path=resource.yaml_path,
                            source_name=resource.source_name,
                            version=resource.version,
                            column_index=idx,
                        )
                    )
                    del resource_columns[idx]

        # Step 2: Update - for kept resource columns, find matching table column
        for idx, resource_column in enumerate(resource_columns):
            table_column = self._find_table_column(
                resource_column.name, quoted=resource_column.quote or False, table_columns=table.columns
            )
            # table_column should always exist here since we deleted non-matches in Step 1

            column_quote = self._quote(table_column.name)
            column_data_type = self._column_type(table_column, resource.config)

            # Check if update is needed (name, quote, or data_type changed)
            name_changed = resource_column.name != table_column.name
            # Treat None as False (unspecified quote means don't quote)
            quote_changed = (resource_column.quote or False) != column_quote
            type_changed = (
                resource_column.data_type is None or column_data_type.lower() != resource_column.data_type.lower()
            )

            if name_changed or quote_changed or type_changed:
                logger.debug("Planned update column action: %s %s", table_column.name, resource.unique_id)
                result.append(
                    UpdateResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        version=resource.version,
                        column_index=idx,
                        column_name=table_column.name,
                        column_quote=column_quote,
                        column_type=column_data_type,
                    )
                )
                resource_columns[idx] = ResourceColumn(
                    name=table_column.name,
                    quote=column_quote,
                    data_type=column_data_type,
                    description=None,
                )

        # Step 3: Add - table columns not matched
        for table_column in table.columns:
            if table_column.name not in matched_table_columns:
                column_quote = self._quote(table_column.name)
                column_data_type = self._column_type(table_column, resource.config)

                logger.debug("Planned add column action: %s %s", table_column.name, resource.unique_id)
                result.append(
                    AddResourceColumn(
                        resource_type=resource.type,
                        resource_name=resource.name,
                        path=resource.yaml_path,
                        source_name=resource.source_name,
                        version=resource.version,
                        column_name=table_column.name,
                        column_quote=column_quote,
                        column_type=column_data_type,
                    )
                )
                resource_columns.append(
                    ResourceColumn(
                        name=table_column.name,
                        quote=column_quote,
                        data_type=column_data_type,
                        description=None,
                    )
                )

        # Step 4: Reorder (optional) - compare tracked names with table order
        resource_column_names = [c.name for c in resource_columns]
        table_column_names = [c.name for c in table.columns]

        if resource_column_names != table_column_names:
            logger.debug("Planned reorder column action: %s", resource.unique_id)
            result.append(
                ReorderResourceColumns(
                    resource_type=resource.type,
                    resource_name=resource.name,
                    path=resource.yaml_path,
                    source_name=resource.source_name,
                    version=resource.version,
                    columns_order=table_column_names,
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
