from pathlib import Path

import pytest

from dbt_pumpkin.data import Column, Resource, ResourceConfig, ResourceID, ResourceType
from dbt_pumpkin.plan import InitializeResource, RelocateResource
from dbt_pumpkin.planner import ActionPlanner


def planner_with_config(source_config: ResourceConfig, non_source_config: ResourceConfig):
    return ActionPlanner(
        [
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
    )


@pytest.fixture
def empty_planner():
    return ActionPlanner([])


@pytest.fixture
def no_yaml_path_planner() -> ActionPlanner:
    return planner_with_config(
        source_config=ResourceConfig(yaml_path_template=None), non_source_config=ResourceConfig(yaml_path_template=None)
    )


@pytest.fixture
def yaml_per_resource_planner() -> ActionPlanner:
    return planner_with_config(
        source_config=ResourceConfig(yaml_path_template="/models/staging/_{name}.yml"),
        non_source_config=ResourceConfig(yaml_path_template="_{name}.yml"),
    )


@pytest.fixture
def yaml_actual_paths_planner() -> ActionPlanner:
    return planner_with_config(
        source_config=ResourceConfig(yaml_path_template="/models/staging/_sources.yml"),
        non_source_config=ResourceConfig(yaml_path_template="_schema.yml"),
    )


def test_initialization_no_resources(empty_planner):
    assert [] == empty_planner.plan_initialization().actions


def test_initialization_no_yaml_path(no_yaml_path_planner):
    assert [] == no_yaml_path_planner.plan_initialization().actions


def test_initialization_yaml_per_resource(yaml_per_resource_planner):
    assert set(yaml_per_resource_planner.plan_initialization().actions) == {
        InitializeResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_stg_orders.yml"),
        )
    }


def test_initialization_yaml_actual_paths(yaml_actual_paths_planner):
    assert set(yaml_actual_paths_planner.plan_initialization().actions) == {
        InitializeResource(
            resource_type=ResourceType.MODEL,
            resource_name="stg_orders",
            path=Path("models/staging/_schema.yml"),
        )
    }


def test_relocation_no_resources(empty_planner):
    assert [] == empty_planner.plan_relocation().actions


def test_relocation_no_yaml_path(no_yaml_path_planner):
    assert [] == no_yaml_path_planner.plan_relocation().actions


def test_relocation_yaml_per_resource(yaml_per_resource_planner):
    assert set(yaml_per_resource_planner.plan_relocation().actions) == {
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


def test_relocation_yaml_actual_paths(yaml_actual_paths_planner):
    assert [] == yaml_actual_paths_planner.plan_relocation().actions
