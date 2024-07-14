from pathlib import Path

import pytest

from dbt_pumpkin.data import Resource, ResourceColumn, ResourceConfig, ResourceID, ResourceType, Table, TableColumn
from dbt_pumpkin.plan import (
    AddResourceColumn,
    BootstrapResource,
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
    assert set(RelocationPlanner(separate_yaml_resources).plan().actions) == {
        RelocateResource(
            resource_type=ResourceType.SOURCE,
            resource_name="ingested",
            from_path=Path("models/staging/_sources.yml"),
            to_path=Path("models/staging/_ingested.yml"),
        ),
        RelocateResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            from_path=Path("models/staging/_schema.yml"),
            to_path=Path("models/staging/_stg_customers.yml"),
        ),
    }


def test_relocation_yaml_actual_paths(actual_yaml_resources):
    assert [] == RelocationPlanner(actual_yaml_resources).plan().actions


def test_synchronization_no_resources():
    assert [] == SynchronizationPlanner([], []).plan().actions
    assert [] == SynchronizationPlanner([], []).plan().actions


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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
        UpdateResourceColumn(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            column_name="id",
            column_type="INTEGER",
        ),
    ]


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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == [
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
