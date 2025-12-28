from pathlib import Path

import pytest

from dbt_pumpkin.data import (
    CaseFolding,
    Resource,
    ResourceColumn,
    ResourceConfig,
    ResourceID,
    ResourceType,
    Table,
    TableColumn,
)
from dbt_pumpkin.plan import (
    AddResourceColumn,
    BootstrapResource,
    DeleteEmptyDescriptor,
    DeleteResourceColumn,
    RelocateResource,
    ReorderResourceColumns,
    UpdateResourceColumn,
)
from dbt_pumpkin.planner import BootstrapPlanner, RelocationPlanner, SynchronizationPlanner


def resources_with_config(source_config: ResourceConfig, non_source_config: ResourceConfig):
    return [
        Resource(
            unique_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            name="customers",
            source_name="ingested",
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            type=ResourceType.SOURCE,
            path=None,
            yaml_path=Path("models/staging/_sources.yml"),
            columns=[],
            config=source_config,
            version=None,
        ),
        Resource(
            unique_id=ResourceID("model.my_pumpkin.stg_customers"),
            name="stg_customers",
            source_name=None,
            database="dev",
            schema="main",
            identifier="stg_customers",
            type=ResourceType.MODEL,
            path=Path("models/staging/stg_customers.sql"),
            yaml_path=Path("models/staging/_schema.yml"),
            columns=[ResourceColumn(name="id", quote=False, data_type=None, description="")],
            config=non_source_config,
            version=None,
        ),
        Resource(
            unique_id=ResourceID("model.my_pumpkin.stg_orders"),
            name="stg_orders",
            source_name=None,
            database="dev",
            schema="main",
            identifier="stg_orders",
            type=ResourceType.MODEL,
            path=Path("models/staging/stg_orders.sql"),
            yaml_path=None,
            columns=[ResourceColumn(name="id", quote=False, data_type=None, description="")],
            config=non_source_config,
            version=None,
        ),
    ]


@pytest.fixture
def no_resources() -> list[Resource]:
    return []


@pytest.fixture
def no_yaml_path_resources() -> list[Resource]:
    return resources_with_config(
        source_config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        non_source_config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
    )


@pytest.fixture
def separate_yaml_resources() -> list[Resource]:
    return resources_with_config(
        source_config=ResourceConfig(
            yaml_path_template="/models/staging/_{name}.yml",
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        non_source_config=ResourceConfig(
            yaml_path_template="_{name}.yml",
            numeric_precision_and_scale=False,
            string_length=False,
        ),
    )


@pytest.fixture
def actual_yaml_resources() -> [list]:
    return resources_with_config(
        source_config=ResourceConfig(
            yaml_path_template="/models/staging/_sources.yml",
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        non_source_config=ResourceConfig(
            yaml_path_template="_schema.yml",
            numeric_precision_and_scale=False,
            string_length=False,
        ),
    )


def test_bootstrap_no_resources(no_resources):
    assert BootstrapPlanner(no_resources).plan().actions == []


def test_bootstrap_no_yaml_path(no_yaml_path_resources):
    assert BootstrapPlanner(no_yaml_path_resources).plan().actions == []


def test_bootstrap_yaml_per_resource(separate_yaml_resources):
    assert set(BootstrapPlanner(separate_yaml_resources).plan().actions) == {
        BootstrapResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_stg_orders.yml"),
        )
    }


def test_bootstrap_yaml_actual_paths(actual_yaml_resources):
    assert set(BootstrapPlanner(actual_yaml_resources).plan().actions) == {
        BootstrapResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_schema.yml"),
        )
    }


def test_relocation_no_resources(no_resources):
    assert RelocationPlanner(no_resources).plan().actions == []


def test_relocation_no_yaml_path(no_yaml_path_resources):
    assert RelocationPlanner(no_yaml_path_resources).plan().actions == []


def test_relocation_yaml_per_resource(separate_yaml_resources):
    assert RelocationPlanner(separate_yaml_resources).plan().actions == [
        RelocateResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            from_path=Path("models/staging/_schema.yml"),
            to_path=Path("models/staging/_stg_customers.yml"),
        ),
        RelocateResource(
            resource_type=ResourceType.SOURCE,
            resource_name="ingested",
            from_path=Path("models/staging/_sources.yml"),
            to_path=Path("models/staging/_ingested.yml"),
        ),
        DeleteEmptyDescriptor(
            path=Path("models/staging/_schema.yml"),
        ),
        DeleteEmptyDescriptor(
            path=Path("models/staging/_sources.yml"),
        ),
    ]


def test_relocation_yaml_actual_paths(actual_yaml_resources):
    assert RelocationPlanner(actual_yaml_resources).plan().actions == []


def test_synchronization_no_resources():
    assert SynchronizationPlanner([], [], CaseFolding.UPPER).plan().actions == []
    assert SynchronizationPlanner([], [], CaseFolding.UPPER).plan().actions == []


def test_synchronization_only_add():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[ResourceColumn(name="ID", quote=False, data_type="INTEGER", description="")],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="NAME",
            column_quote=False,
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_add_numeric_precision_and_scale():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=True,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="NUMBER", data_type="NUMBER(38,0)", is_numeric=True, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="ID",
            column_quote=False,
            column_type="NUMBER(38,0)",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="NAME",
            column_quote=False,
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_add_string_length():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=True,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="NUMBER", data_type="NUMBER(38,0)", is_numeric=True, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="ID",
            column_quote=False,
            column_type="NUMBER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="NAME",
            column_quote=False,
            column_type="character varying(256)",
        ),
    ]


def test_synchronization_only_update():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type=None, description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="ID",
            column_quote=False,
            column_type="INTEGER",
        ),
    ]


def test_synchronization_no_update_when_datatypes_match_ignorecase():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type="integer", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VarChar", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == []


def test_synchronization_update_numeric_precision_and_scale():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type="NUMBER", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=True,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="NUMBER", data_type="NUMBER(38,0)", is_numeric=True, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="ID",
            column_quote=False,
            column_type="NUMBER(38,0)",
        ),
    ]


def test_synchronization_update_string_length():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type="NUMBER", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=True,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="NUMBER", data_type="NUMBER(38,0)", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
            column_name="NAME",
            column_quote=False,
            column_type="character varying(256)",
        ),
    ]


def test_synchronization_only_delete():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type="INTEGER", description=""),
            ResourceColumn(name="LAST NAME", quote=True, data_type="VARCHAR", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
        ),
    ]


def test_synchronization_all_actions():
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="id", quote=False, data_type="SHORT", description=""),
            ResourceColumn(name="LAST NAME", quote=True, data_type="VARCHAR", description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="BIRTH_DATE", dtype="DATE", data_type="DATE", is_numeric=False, is_string=False),
            TableColumn(
                name="NAME", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
            ),
        ],
    )

    # Resource columns: id (folds to ID), "LAST NAME" (quoted), name (folds to NAME)
    # Table columns: ID, BIRTH_DATE, NAME
    # Expected:
    # - Delete "LAST NAME" at index 1 (quoted, doesn't match any table column)
    # - Update id -> ID (name change + type change)
    # - Update name -> NAME (name change)
    # - Add BIRTH_DATE (not in resource)
    # - Reorder to match table order
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
        ),
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="ID",
            column_quote=False,
            column_type="INTEGER",
        ),
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
            column_name="NAME",
            column_quote=False,
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="BIRTH_DATE",
            column_quote=False,
            column_type="DATE",
        ),
        ReorderResourceColumns(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            columns_order=["ID", "BIRTH_DATE", "NAME"],
        ),
    ]


def test_synchronization_with_upper_case_folding():
    """Test that mixed-case columns get quoted with UPPER case folding."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            # Standard uppercase column - no quoting needed
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            # Mixed case column - needs quoting
            TableColumn(name="CustomerId", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            # Lowercase column - needs quoting (differs from uppercase)
            TableColumn(name="email", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    actions = SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions

    assert len(actions) == 3
    assert actions[0] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="ID",
        column_quote=False,  # Standard uppercase, no quoting
        column_type="INTEGER",
    )
    assert actions[1] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="CustomerId",
        column_quote=True,  # Mixed case, needs quoting
        column_type="VARCHAR",
    )
    assert actions[2] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="email",
        column_quote=True,  # Lowercase, needs quoting
        column_type="VARCHAR",
    )


def test_synchronization_with_lower_case_folding():
    """Test that mixed-case columns get quoted with LOWER case folding."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            # Standard lowercase column - no quoting needed
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            # Mixed case column - needs quoting
            TableColumn(name="CustomerId", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            # Uppercase column - needs quoting (differs from lowercase)
            TableColumn(name="EMAIL", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    actions = SynchronizationPlanner([resource], [table], CaseFolding.LOWER).plan().actions

    assert len(actions) == 3
    assert actions[0] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="id",
        column_quote=False,  # Standard lowercase, no quoting
        column_type="INTEGER",
    )
    assert actions[1] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="CustomerId",
        column_quote=True,  # Mixed case, needs quoting
        column_type="VARCHAR",
    )
    assert actions[2] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        version=None,
        column_name="EMAIL",
        column_quote=True,  # Uppercase, needs quoting
        column_type="VARCHAR",
    )


def test_synchronization_special_characters_always_quoted():
    """Test that columns with special characters are always quoted regardless of case folding."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="FIRST NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="USER-ID", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    actions = SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions

    # Both should be quoted due to special characters
    assert actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="FIRST NAME",
            column_quote=True,  # Space in name
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="USER-ID",
            column_quote=True,  # Hyphen in name
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_reserved_words_always_quoted():
    """Test that SQL reserved words are always quoted regardless of case folding."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ORDER", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="user", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="Select", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="Group", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="table", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="DESCRIBE", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Test with UPPER folding - reserved words should still be quoted
    actions = SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions

    assert actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="ORDER",
            column_quote=True,  # Reserved word
            column_type="INTEGER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="user",
            column_quote=True,  # Reserved word (case-insensitive)
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="Select",
            column_quote=True,  # Reserved word (case-insensitive)
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="Group",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="table",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="DESCRIBE",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_update_adds_quote_for_reserved_word():
    """Test that updating a column with a reserved word name adds quote=True."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            # Column named "ORDER" (reserved word) without quote attribute, needs data type update
            # With UPPER folding, unquoted "ORDER" folds to "ORDER" and matches table's "ORDER"
            ResourceColumn(name="ORDER", quote=False, data_type="INT", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ORDER", dtype="BIGINT", data_type="BIGINT", is_numeric=False, is_string=False),
        ],
    )

    # Should update both data type and quote attribute
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="ORDER",
            column_quote=True,  # Reserved word needs quoting
            column_type="BIGINT",
        ),
    ]


def test_synchronization_update_adds_quote_for_mixed_case():
    """Test that updating a column adds quote=True when table has mixed case.

    Resource has unquoted 'customerid' which folds to 'CUSTOMERID' with UPPER folding.
    This doesn't match table's 'CustomerId' directly, but matches via case-insensitive fallback.
    The update should rename to 'CustomerId' and add quote=True.
    """
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            # Unquoted column - with UPPER folding, 'customerid' folds to 'CUSTOMERID'
            # Doesn't match 'CustomerId' directly, but matches via case-insensitive fallback
            ResourceColumn(name="customerid", quote=False, data_type="INT", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            # Mixed case in table - requires quoting
            TableColumn(name="CustomerId", dtype="BIGINT", data_type="BIGINT", is_numeric=False, is_string=False),
        ],
    )

    # With UPPER folding and case-insensitive fallback matching:
    # - Resource 'customerid' (unquoted) folds to 'CUSTOMERID'
    # - No exact match with 'CustomerId', but case-insensitive fallback finds it
    # - Update renames to 'CustomerId' and adds quote=True
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="CustomerId",
            column_quote=True,  # Mixed case needs quoting with UPPER folding
            column_type="BIGINT",
        ),
    ]


def test_synchronization_unknown_case_folding():
    """Test fallback behavior when case folding detection fails (UNKNOWN)."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="CustomerId", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Pass UNKNOWN for case_folding - detection failed
    actions = SynchronizationPlanner([resource], [table], CaseFolding.UNKNOWN).plan().actions

    # With UNKNOWN, always quote for safety
    assert actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="ID",
            column_quote=True,  # UNKNOWN - quoted for safety
            column_type="INTEGER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="CustomerId",
            column_quote=True,  # UNKNOWN - quoted for safety
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_ambiguous_columns_in_resource():
    """Test that ambiguous columns in resource (e.g., 'Name' and 'NAME') use exact match first."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
            ResourceColumn(name="Name", quote=False, data_type="VARCHAR", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="Name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # With LOWER folding: resource columns fold to 'id', 'name', 'name' (duplicate!)
    # Table has exact matches for 'id', 'Name', 'NAME'
    # Resource 'Name' matches table 'Name' via case-insensitive fallback
    # Resource 'NAME' matches table 'NAME' via case-insensitive fallback
    # Both table columns need quote=True since they differ from lowercase
    assert SynchronizationPlanner([resource], [table], CaseFolding.LOWER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
            column_name="Name",
            column_quote=True,
            column_type="VARCHAR",
        ),
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=2,
            column_name="NAME",
            column_quote=True,
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_ambiguous_columns_in_table():
    """Test that ambiguous columns in table (e.g., 'Name' and 'NAME') use exact match first."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # With LOWER folding: resource 'name' folds to 'name', matches table 'name' exactly
    # Table 'NAME' is unmatched, so it gets added with quote=True (differs from lowercase)
    assert SynchronizationPlanner([resource], [table], CaseFolding.LOWER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_name="NAME",
            column_quote=True,
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_ambiguous_columns_mismatch():
    """Test ambiguous columns with different cases between resource and table."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
            ResourceColumn(name="First_Name", quote=False, data_type="VARCHAR", description=""),
            ResourceColumn(name="FIRST_NAME", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="first_name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # With LOWER folding, delete pass processes in reversed order:
    # - Resource 'FIRST_NAME' at index 2 folds to 'first_name', matches table 'first_name' -> keep
    # - Resource 'First_Name' at index 1 folds to 'first_name', but already matched -> delete
    # - Resource 'id' at index 0 matches table 'id' -> keep
    # After delete: ['id', 'FIRST_NAME']
    # Update: 'FIRST_NAME' needs rename to 'first_name'
    assert SynchronizationPlanner([resource], [table], CaseFolding.LOWER).plan().actions == [
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
        ),
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=1,
            column_name="first_name",
            column_quote=False,
            column_type="VARCHAR",
        ),
    ]


def test_synchronization_versioned_model():
    """Test that synchronization planner generates actions with version for versioned models."""
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.customers.v2"),
        name="customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="customers_v2",
        type=ResourceType.MODEL,
        path=Path("models/customers_2.sql"),
        yaml_path=Path("models/_schema.yml"),
        columns=[
            ResourceColumn(name="ID", quote=False, data_type="INTEGER", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=2,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.customers.v2"),
        columns=[
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    actions = SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions

    # Should generate AddResourceColumn with version=2
    assert len(actions) == 1
    assert actions[0] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="customers",
        source_name=None,
        path=Path("models/_schema.yml"),
        version=2,
        column_name="NAME",
        column_quote=False,
        column_type="VARCHAR",
    )


def test_synchronization_no_false_positive_when_quote_is_none():
    """Test that no update is generated when resource column has quote=None and column doesn't need quoting.

    This tests the fix for false positive quote_changed detection.
    When a column in YAML doesn't have explicit quote attribute, dbt returns None.
    If the column doesn't require quoting, None should be treated as equivalent to False.
    """
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            # quote=None simulates a column without explicit quote in YAML
            ResourceColumn(name="ID", quote=None, data_type="INTEGER", description=""),
            ResourceColumn(name="NAME", quote=None, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            # Standard uppercase columns - don't need quoting with UPPER folding
            TableColumn(name="ID", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # With UPPER folding:
    # - ID doesn't need quoting (uppercase matches folded)
    # - NAME doesn't need quoting (uppercase matches folded)
    # - resource columns have quote=None, which should be treated as quote=False
    # - No updates should be generated since name, quote (None==False), and data_type all match
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == []


def test_synchronization_update_when_quote_none_needs_quoting():
    """Test that update IS generated when resource column has quote=None but column DOES need quoting.

    When a column requires quoting (e.g., reserved word) and resource has quote=None,
    an update should be generated to set quote=True.
    """
    resource = Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        source_name=None,
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[
            # quote=None but ORDER is a reserved word and needs quoting
            ResourceColumn(name="ORDER", quote=None, data_type="INTEGER", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
        version=None,
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="ORDER", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
        ],
    )

    # ORDER is a reserved word, so it needs quoting
    # resource has quote=None, table needs quote=True -> update required
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            version=None,
            column_index=0,
            column_name="ORDER",
            column_quote=True,
            column_type="INTEGER",
        ),
    ]
