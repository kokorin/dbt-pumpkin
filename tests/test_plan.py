import copy
from pathlib import Path

import pytest

from dbt_pumpkin.data import ResourceType
from dbt_pumpkin.exception import PumpkinError, ResourceNotFoundError
from dbt_pumpkin.plan import (
    AddResourceColumn,
    BootstrapResource,
    DeleteEmptyDescriptor,
    DeleteResourceColumn,
    RelocateResource,
    ReorderResourceColumns,
    UpdateResourceColumn,
)


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
                        {"name": "name"},
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


@pytest.mark.parametrize("resource_type", list(ResourceType))
def test_delete_empty_descriptor(resource_type: ResourceType):
    action = DeleteEmptyDescriptor(
        path=Path("models/schema.yml"),
    )

    files = {
        Path("models/schema.yml"): {
            "version": 2,
            resource_type.plural_name: [],
        },
    }
    action.execute(files)
    assert files == {Path("models/schema.yml"): None}

    files = {
        Path("models/schema.yml"): {
            "version": 2,
            resource_type.plural_name: [{"name": "any_name"}],
        },
    }
    expected = copy.deepcopy(files)
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
    action = BootstrapResource(
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
        BootstrapResource(
            resource_type=ResourceType.SOURCE, resource_name="stg_orders", path=Path("models/staging/_sources.yml")
        )


def test_add_resource_column(files):
    action = AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="address",
        column_quote=False,
        column_type="varchar",
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_customers",
                        "columns": [
                            {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                            {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                            {"name": "address", "data_type": "varchar"},
                        ],
                    },
                    {
                        "name": "int_customers",
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_add_resource_column_quoted(files):
    action = AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="address",
        column_quote=True,
        column_type="varchar",
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_customers",
                        "columns": [
                            {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                            {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                            {"name": "address", "quote": True, "data_type": "varchar"},
                        ],
                    },
                    {
                        "name": "int_customers",
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_add_resource_column_error(files):
    action = AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="not_defined",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="any",
        column_quote=False,
        column_type="any",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_add_resource_column_resource_not_found_error(files):
    action = AddResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/__unknown__.yml"),
        column_name="any",
        column_quote=False,
        column_type="any",
    )

    with pytest.raises(ResourceNotFoundError):
        action.execute(files)


def test_add_source_column(files):
    action = AddResourceColumn(
        resource_type=ResourceType.SOURCE,
        resource_name="customers",
        source_name="ingested",
        path=Path("models/staging/_sources.yml"),
        column_name="id",
        column_quote=False,
        column_type="int",
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_sources.yml"): {
                "version": 2,
                "sources": [
                    {
                        "name": "ingested",
                        "tables": [
                            {"name": "customers", "columns": [{"name": "id", "data_type": "int"}]},
                            {"name": "orders", "columns": []},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_add_source_column_unknown_source_name(files):
    action = AddResourceColumn(
        resource_type=ResourceType.SOURCE,
        resource_name="customers",
        source_name="unknown",
        path=Path("models/staging/_sources.yml"),
        column_name="id",
        column_quote=False,
        column_type="int",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_add_source_column_unknown_name(files):
    action = AddResourceColumn(
        resource_type=ResourceType.SOURCE,
        resource_name="unknown",
        source_name="ingested",
        path=Path("models/staging/_sources.yml"),
        column_name="id",
        column_quote=False,
        column_type="int",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_update_resource_column(files):
    action = UpdateResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="id",
        column_type="bigint",
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_customers",
                        "columns": [
                            {"name": "id", "data_type": "bigint", "tests": ["not_null", "unique"]},
                            {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                        ],
                    },
                    {
                        "name": "int_customers",
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_update_resource_column_no_resource_error(files):
    action = UpdateResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="unknown",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="id",
        column_type="bigint",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_update_resource_column_no_column_error(files):
    action = UpdateResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="unknown",
        column_type="variant",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_delete_resource_column(files):
    action = DeleteResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="name",
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_customers",
                        "columns": [
                            {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                        ],
                    },
                    {
                        "name": "int_customers",
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_delete_resource_column_no_resource_error(files):
    action = DeleteResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="unknown",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="name",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_delete_resource_column_no_column_error(files):
    action = DeleteResourceColumn(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        column_name="unknown",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_reorder_resource_columns(files):
    action = ReorderResourceColumns(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        columns_order=["name", "id"],
    )

    (expected := copy.deepcopy(files)).update(
        {
            Path("models/staging/_schema.yml"): {
                "version": 2,
                "models": [
                    {
                        "name": "stg_customers",
                        "columns": [
                            {"name": "name", "data_type": "varchar", "tests": ["not_null"]},
                            {"name": "id", "data_type": "int", "tests": ["not_null", "unique"]},
                        ],
                    },
                    {
                        "name": "int_customers",
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                        ],
                    },
                ],
            },
        }
    )

    action.execute(files)

    assert files == expected


def test_reorder_resource_columns_not_unique_columns_error():
    with pytest.raises(PumpkinError):
        ReorderResourceColumns(
            resource_type=ResourceType.MODEL,
            resource_name="stg_customers",
            source_name=None,
            path=Path("models/staging/_schema.yml"),
            columns_order=["name", "id", "id"],
        )


def test_reorder_resource_columns_unknown_column_error(files):
    action = ReorderResourceColumns(
        resource_type=ResourceType.MODEL,
        resource_name="stg_customers",
        source_name=None,
        path=Path("models/staging/_schema.yml"),
        columns_order=["name", "id", "unknown"],
    )

    with pytest.raises(PumpkinError):
        action.execute(files)
