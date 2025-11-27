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
    assert [] == BootstrapPlanner(no_resources).plan().actions


def test_bootstrap_no_yaml_path(no_yaml_path_resources):
    assert [] == BootstrapPlanner(no_yaml_path_resources).plan().actions


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
    assert [] == RelocationPlanner(no_resources).plan().actions


def test_relocation_no_yaml_path(no_yaml_path_resources):
    assert [] == RelocationPlanner(no_yaml_path_resources).plan().actions


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
    assert [] == RelocationPlanner(actual_yaml_resources).plan().actions


def test_synchronization_no_resources():
    assert [] == SynchronizationPlanner([], [], CaseFolding.UPPER).plan().actions
    assert [] == SynchronizationPlanner([], [], CaseFolding.UPPER).plan().actions


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
        columns=[ResourceColumn(name="id", quote=False, data_type="INTEGER", description="")],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
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
            column_name="ID",
            column_quote=False,
            column_type="NUMBER(38,0)",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
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
            column_name="ID",
            column_quote=False,
            column_type="NUMBER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
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
            ResourceColumn(name="id", quote=False, data_type=None, description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
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
            column_name="id",
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
            ResourceColumn(name="id", quote=False, data_type="integer", description=""),
            ResourceColumn(name="name", quote=False, data_type="VarChar", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
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
            ResourceColumn(name="id", quote=False, data_type="NUMBER", description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=True,
            string_length=False,
        ),
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
            column_name="id",
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
            ResourceColumn(name="id", quote=False, data_type="NUMBER", description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=True,
        ),
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
            column_name="name",
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
            ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
            ResourceColumn(name="LAST NAME", quote=True, data_type="VARCHAR", description=""),
            ResourceColumn(name="name", quote=False, data_type="VARCHAR", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
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
            column_name="LAST NAME",
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

    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="id",
            column_type="INTEGER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="BIRTH_DATE",
            column_quote=False,
            column_type="DATE",
        ),
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="LAST NAME",
        ),
        ReorderResourceColumns(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            columns_order=["id", "BIRTH_DATE", "name"],
        ),
    ]


def test_synchronization_ambiguous_columns_in_resource():
    """Test that ambiguous columns in resource (e.g., 'Name' and 'NAME') fallback to exact match."""
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
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="Name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Should use exact match and produce no actions
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == []


def test_synchronization_ambiguous_columns_in_table():
    """Test that ambiguous columns in table (e.g., 'Name' and 'NAME') fallback to exact match."""
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
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Should use exact match: resource "name" matches table "name", table "NAME" is new
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="NAME",
            column_quote=False,
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
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="first_name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Should use exact match: no exact match for "first_name" in resource, so add it
    # Delete both "First_Name" and "FIRST_NAME"
    # Note: "first_name" needs quoting because it differs from "FIRST_NAME" (UPPER folding)
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="first_name",
            column_quote=True,  # Lowercase needs quoting with UPPER folding
            column_type="VARCHAR",
        ),
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="First_Name",
        ),
        DeleteResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="FIRST_NAME",
        ),
    ]


def test_synchronization_ambiguous_columns_with_reorder():
    """Test that reorder works correctly with ambiguous columns using exact match."""
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
            ResourceColumn(name="Name", quote=False, data_type="VARCHAR", description=""),
            ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=""),
            ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
        ],
        config=ResourceConfig(
            yaml_path_template=None,
            numeric_precision_and_scale=False,
            string_length=False,
        ),
    )

    table = Table(
        resource_id=ResourceID("model.my_pumpkin.stg_customers"),
        columns=[
            TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
            TableColumn(name="Name", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
            TableColumn(name="NAME", dtype="VARCHAR", data_type="VARCHAR", is_numeric=False, is_string=True),
        ],
    )

    # Should use exact match and reorder columns to match table order
    assert SynchronizationPlanner([resource], [table], CaseFolding.UPPER).plan().actions == [
        ReorderResourceColumns(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            columns_order=["id", "Name", "NAME"],
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
        column_name="ID",
        column_quote=False,  # Standard uppercase, no quoting
        column_type="INTEGER",
    )
    assert actions[1] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="CustomerId",
        column_quote=True,  # Mixed case, needs quoting
        column_type="VARCHAR",
    )
    assert actions[2] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
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
        column_name="id",
        column_quote=False,  # Standard lowercase, no quoting
        column_type="INTEGER",
    )
    assert actions[1] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="CustomerId",
        column_quote=True,  # Mixed case, needs quoting
        column_type="VARCHAR",
    )
    assert actions[2] == AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
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
            column_name="FIRST NAME",
            column_quote=True,  # Space in name
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
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
            column_name="ORDER",
            column_quote=True,  # Reserved word
            column_type="INTEGER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="user",
            column_quote=True,  # Reserved word (case-insensitive)
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="Select",
            column_quote=True,  # Reserved word (case-insensitive)
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="Group",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="table",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="DESCRIBE",
            column_quote=True,  # Reserved word
            column_type="VARCHAR",
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
            column_name="ID",
            column_quote=True,  # UNKNOWN - quoted for safety
            column_type="INTEGER",
        ),
        AddResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="CustomerId",
            column_quote=True,  # UNKNOWN - quoted for safety
            column_type="VARCHAR",
        ),
    ]
