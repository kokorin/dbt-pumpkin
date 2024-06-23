from pathlib import Path

import pytest

from dbt_pumpkin.data import Column, Resource, ResourceConfig, ResourceID, ResourceType
from dbt_pumpkin.plan import AddResource, MoveResource


@pytest.fixture
def files() -> dict[Path, dict]:
    return {
        Path("models/staging/_schema.yml"): {
            "version": 2,
            "models": [
                {
                    "name": "stg_customers",
                    "columns": [
                        {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                        {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                    ],
                },
                {
                    "name": "int_customers",
                    "columns": [
                        {"name": "id"},
                        {
                            "name": "name",
                        },
                    ],
                },
            ],
        }
    }


@pytest.fixture
def stg_customers() -> Resource:
    return Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_customers"),
        name="stg_customers",
        database="dev",
        schema="main",
        identifier="stg_customers",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_customers.sql"),
        yaml_path=Path("models/staging/_schema.yml"),
        columns=[Column(name="id", data_type=None, description="")],
        config=ResourceConfig(),
    )


@pytest.fixture
def stg_orders() -> Resource:
    return Resource(
        unique_id=ResourceID("model.my_pumpkin.stg_orders"),
        name="stg_orders",
        database="dev",
        schema="main",
        identifier="stg_orders",
        type=ResourceType.MODEL,
        path=Path("models/staging/stg_orders.sql"),
        yaml_path=None,
        columns=[
            Column(name="id", data_type=None, description=""),
            Column(name="name", data_type="varchar", description=None),
        ],
        config=ResourceConfig(),
    )


def test_move_resource(stg_customers, files):
    action = MoveResource(
        resource=stg_customers,
        from_path=Path("models/staging/_schema.yml"),
        to_path=Path("models/staging/stg_customers.yml"),
    )

    action.apply(files)

    assert files[Path("models/staging/_schema.yml")] == {
        "version": 2,
        "models": [
            {
                "name": "int_customers",
                "columns": [
                    {"name": "id"},
                    {
                        "name": "name",
                    },
                ],
            }
        ],
    }

    assert files[Path("models/staging/stg_customers.yml")] == {
        "version": 2,
        "models": [
            {
                "name": "stg_customers",
                "columns": [
                    {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                    {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                ],
            }
        ],
    }


def test_add_resource(stg_orders, files):
    action = AddResource(resource=stg_orders, to_path=Path("models/staging/stg_orders.yml"))

    action.apply(files)

    assert files[Path("models/staging/stg_orders.yml")] == {
        "version": 2,
        "models": [
            {
                "name": "stg_orders",
                "columns": [
                    {
                        "name": "id",
                    },
                    {"name": "name", "data_type": "varchar"},
                ],
            }
        ],
    }
