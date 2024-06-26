import copy
from pathlib import Path

import pytest

from dbt_pumpkin.data import ResourceType
from dbt_pumpkin.exception import PumpkinError, ResourceNotFoundError
from dbt_pumpkin.plan import InitializeResource, RelocateResource


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
        },
        Path("models/staging/_sources.yml"): {
            "version": 2,
            "sources": [
                {
                    "name": "ingested",
                    "tables": [
                        {"name": "customers", "columns": []},
                        {"name": "orders", "columns": []},
                    ],
                },
            ],
        },
    }


def test_relocate_resource_to_existing_file(files):
    action = RelocateResource(
        resource_type=ResourceType.SOURCE,
        resource_name="ingested",
        from_path=Path("models/staging/_sources.yml"),
        to_path=Path("models/staging/_schema.yml"),
    )
    expected = copy.deepcopy(files)
    expected[Path("models/staging/_sources.yml")] = {
        "version": 2,
        "sources": [],
    }
    expected[Path("models/staging/_schema.yml")]["sources"] = [
        {
            "name": "ingested",
            "tables": [
                {"name": "customers", "columns": []},
                {"name": "orders", "columns": []},
            ],
        },
    ]

    action.execute(files)

    assert files == expected


def test_relocate_resource_to_new_file(files):
    action = RelocateResource(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        from_path=Path("models/staging/_schema.yml"),
        to_path=Path("models/staging/stg_customers.yml"),
    )
    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
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
            },
            Path("models/staging/stg_customers.yml"): {
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
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_relocate_resource_error(files):
    action = RelocateResource(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        from_path=Path("models/staging/non_existent.yml"),
        to_path=Path("models/staging/stg_customers.yml"),
    )
    with pytest.raises(ResourceNotFoundError):
        action.execute(files)


def test_initialize_model_resource(files):
    action = InitializeResource(
        resource_type=ResourceType.MODEL, resource_name="stg_orders", path=Path("models/staging/stg_orders.yml")
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/stg_orders.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_orders",
                        "columns": [],
                    }
                ],
            }
        }
    )

    action.execute(files)

    assert files == expected


def test_initialize_source_error():
    with pytest.raises(PumpkinError):
        InitializeResource(
            resource_type=ResourceType.SOURCE, resource_name="stg_orders", path=Path("models/staging/_sources.yml")
        )
