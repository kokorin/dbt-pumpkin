from pathlib import Path

import pytest

from dbt_pumpkin.data import Model, Resource, ResourceColumn, ResourceConfig, ResourceID, ResourceType, Source, Table, TableColumn
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
        Source(
            unique_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            name="customers",
            source_name="ingested",
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            yaml_path=Path("models/staging/_sources.yml"),
            columns=[],
            config=source_config,
        ),
        Model(
            unique_id=ResourceID("model.my_pumpkin.stg_customers"),
            name="stg_customers",
            database="dev",
            schema="main",
            identifier="stg_customers",
            path=Path("models/staging/stg_customers.sql"),
            yaml_path=Path("models/staging/_schema.yml"),
            columns=[ResourceColumn(name="id", quote=False, data_type=None, description="")],
            config=non_source_config,
        ),
        Model(
            unique_id=ResourceID("model.my_pumpkin.stg_orders"),
            name="stg_orders",
            database="dev",
            schema="main",
            identifier="stg_orders",
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
def actual_yaml_resources() -> list[Resource]:
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
    # Get the stg_orders resource from the fixture
    stg_orders = next(r for r in separate_yaml_resources if r.name == "stg_orders")

    assert set(BootstrapPlanner(separate_yaml_resources).plan().actions) == {
        BootstrapResource(
            resource=stg_orders,
            path=Path("models/staging/_stg_orders.yml"),
        )
    }


def test_bootstrap_yaml_actual_paths(actual_yaml_resources):
    # Get the stg_orders resource from the fixture
    stg_orders = next(r for r in actual_yaml_resources if r.name == "stg_orders")

    assert set(BootstrapPlanner(actual_yaml_resources).plan().actions) == {
        BootstrapResource(
            resource=stg_orders,
            path=Path("models/staging/_schema.yml"),
        )
    }


def test_relocation_no_resources(no_resources):
    assert [] == RelocationPlanner(no_resources).plan().actions


def test_relocation_no_yaml_path(no_yaml_path_resources):
    assert [] == RelocationPlanner(no_yaml_path_resources).plan().actions


def test_relocation_yaml_per_resource(separate_yaml_resources):
    # Get the resources from the fixture
    stg_customers = next(r for r in separate_yaml_resources if r.name == "stg_customers")
    ingested = next(r for r in separate_yaml_resources if hasattr(r, 'source_name') and r.source_name == "ingested")

    assert RelocationPlanner(separate_yaml_resources).plan().actions == [
        RelocateResource(
            resource=stg_customers,
            from_path=Path("models/staging/_schema.yml"),
            to_path=Path("models/staging/_stg_customers.yml"),
        ),
        RelocateResource(
            resource=ingested,
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
    assert [] == SynchronizationPlanner([], []).plan().actions
    assert [] == SynchronizationPlanner([], []).plan().actions


def test_synchronization_only_add():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=None),
        ),
    ]


def test_synchronization_add_numeric_precision_and_scale():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="ID", quote=False, data_type="NUMBER(38,0)", description=None),
        ),
        AddResourceColumn(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="NAME", quote=False, data_type="VARCHAR", description=None),
        ),
    ]


def test_synchronization_add_string_length():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="ID", quote=False, data_type="NUMBER", description=None),
        ),
        AddResourceColumn(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="NAME", quote=False, data_type="character varying(256)", description=None),
        ),
    ]


def test_synchronization_only_update():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
        ),
    ]


def test_synchronization_no_update_when_datatypes_match_ignorecase():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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

    assert SynchronizationPlanner([resource], [table]).plan().actions == []


def test_synchronization_update_numeric_precision_and_scale():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="id", quote=False, data_type="NUMBER(38,0)", description=""),
        ),
    ]


def test_synchronization_update_string_length():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="name", quote=False, data_type="character varying(256)", description=""),
        ),
    ]


def test_synchronization_only_delete():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column_name="LAST NAME",
        ),
    ]


def test_synchronization_all_actions():
    resource = Model(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
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
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="id", quote=False, data_type="INTEGER", description=""),
        ),
        AddResourceColumn(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column=ResourceColumn(name="BIRTH_DATE", quote=False, data_type="DATE", description=None),
        ),
        DeleteResourceColumn(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            column_name="LAST NAME",
        ),
        ReorderResourceColumns(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            columns_order=["id", "BIRTH_DATE", "name"],
        ),
    ]
