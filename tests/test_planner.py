from pathlib import Path

import pytest

from dbt_pumpkin.data import Column, Resource, ResourceConfig, ResourceID, ResourceType
from dbt_pumpkin.plan import BootstrapResource, RelocateResource
from dbt_pumpkin.planner import BootstrapPlanner, RelocationPlanner


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
            columns=[Column(name="id", data_type=None, description="")],
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
            columns=[Column(name="id", data_type=None, description="")],
            config=non_source_config,
        ),
    ]


@pytest.fixture
def no_resources() -> list[Resource]:
    return []


@pytest.fixture
def no_yaml_path_resources() -> list[Resource]:
    return resources_with_config(
        source_config=ResourceConfig(yaml_path_template=None), non_source_config=ResourceConfig(yaml_path_template=None)
    )


@pytest.fixture
def separate_yaml_resources() -> list[Resource]:
    return resources_with_config(
        source_config=ResourceConfig(yaml_path_template="/models/staging/_{name}.yml"),
        non_source_config=ResourceConfig(yaml_path_template="_{name}.yml"),
    )


@pytest.fixture
def actual_yaml_resources() -> [list]:
    return resources_with_config(
        source_config=ResourceConfig(yaml_path_template="/models/staging/_sources.yml"),
        non_source_config=ResourceConfig(yaml_path_template="_schema.yml"),
    )


def test_bootstrap_no_resources(no_resources):
    assert [] == BootstrapPlanner().plan(no_resources).actions


def test_bootstrap_no_yaml_path(no_yaml_path_resources):
    assert [] == BootstrapPlanner().plan(no_yaml_path_resources).actions


def test_bootstrap_yaml_per_resource(separate_yaml_resources):
    assert set(BootstrapPlanner().plan(separate_yaml_resources).actions) == {
        BootstrapResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_stg_orders.yml"),
        )
    }


def test_bootstrap_yaml_actual_paths(actual_yaml_resources):
    assert set(BootstrapPlanner().plan(actual_yaml_resources).actions) == {
        BootstrapResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_schema.yml"),
        )
    }


def test_relocation_no_resources(no_resources):
    assert [] == RelocationPlanner().plan(no_resources).actions


def test_relocation_no_yaml_path(no_yaml_path_resources):
    assert [] == RelocationPlanner().plan(no_yaml_path_resources).actions


def test_relocation_yaml_per_resource(separate_yaml_resources):
    assert set(RelocationPlanner().plan(separate_yaml_resources).actions) == {
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
    assert [] == RelocationPlanner().plan(actual_yaml_resources).actions
