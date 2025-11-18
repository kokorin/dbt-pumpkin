import copy
from pathlib import Path

import pytest

from dbt_pumpkin.data import Model, Resource, ResourceColumn, ResourceConfig, ResourceID, ResourceType, Seed, Source
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


def make_source(name: str, source_name: str, yaml_path: Path | None = None) -> Source:
    """Helper to create a Source resource for tests."""
    return Source(
        unique_id=ResourceID(f"source.test.{source_name}.{name}"),
        name=name,
        source_name=source_name,
        database="db",
        schema="schema",
        identifier=name,
        yaml_path=yaml_path,
        columns=[],
        config=None,
    )


def make_model(name: str, yaml_path: Path | None = None, version: str | float | None = None) -> Model:
    """Helper to create a Model resource for tests."""
    return Model(
        unique_id=ResourceID(f"model.test.{name}"),
        name=name,
        database="db",
        schema="schema",
        identifier=name,
        path=Path(f"models/{name}.sql"),
        yaml_path=yaml_path,
        columns=[],
        config=None,
        version=version,
    )


def make_seed(name: str, yaml_path: Path | None = None) -> Seed:
    """Helper to create a Seed resource for tests."""
    return Seed(
        unique_id=ResourceID(f"seed.test.{name}"),
        name=name,
        database="db",
        schema="schema",
        identifier=name,
        path=Path(f"seeds/{name}.csv"),
        yaml_path=yaml_path,
        columns=[],
        config=None,
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
    resource = make_source("ingested", "ingested", yaml_path=Path("models/staging/_sources.yml"))
    action = RelocateResource(
        resource=resource,
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
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    action = RelocateResource(
        resource=resource,
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
    resource = make_model("stg_customers", yaml_path=Path("models/staging/non_existent.yml"))
    action = RelocateResource(
        resource=resource,
        from_path=Path("models/staging/non_existent.yml"),
        to_path=Path("models/staging/stg_customers.yml"),
    )
    with pytest.raises(ResourceNotFoundError):
        action.execute(files)


def test_initialize_model_resource(files):
    resource = make_model("stg_orders", yaml_path=Path("models/staging/stg_orders.yml"))
    action = BootstrapResource(
        resource=resource, path=Path("models/staging/stg_orders.yml")
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
    resource = make_source("stg_orders", "ingested", yaml_path=Path("models/staging/_sources.yml"))
    with pytest.raises(PumpkinError):
        BootstrapResource(
            resource=resource, path=Path("models/staging/_sources.yml")
        )


def test_add_resource_column(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="address", quote=False, data_type="varchar", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
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
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="address", quote=True, data_type="varchar", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
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
    resource = make_model("not_defined", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="any", quote=False, data_type="any", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_add_resource_column_resource_not_found_error(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/__unknown__.yml"))
    column = ResourceColumn(name="any", quote=False, data_type="any", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/__unknown__.yml"),
        column=column,
    )

    with pytest.raises(ResourceNotFoundError):
        action.execute(files)


def test_add_source_column(files):
    resource = make_source("customers", "ingested", yaml_path=Path("models/staging/_sources.yml"))
    column = ResourceColumn(name="id", quote=False, data_type="int", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_sources.yml"),
        column=column,
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
    resource = make_source("customers", "unknown", yaml_path=Path("models/staging/_sources.yml"))
    column = ResourceColumn(name="id", quote=False, data_type="int", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_sources.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_add_source_column_unknown_name(files):
    resource = make_source("unknown", "ingested", yaml_path=Path("models/staging/_sources.yml"))
    column = ResourceColumn(name="id", quote=False, data_type="int", description="")
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_sources.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_update_resource_column(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="id", quote=False, data_type="bigint", description="")
    action = UpdateResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
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
    resource = make_model("unknown", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="id", quote=False, data_type="bigint", description="")
    action = UpdateResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_update_resource_column_no_column_error(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    column = ResourceColumn(name="unknown", quote=False, data_type="variant", description="")
    action = UpdateResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_delete_resource_column(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    action = DeleteResourceColumn(
        resource=resource,
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
    resource = make_model("unknown", yaml_path=Path("models/staging/_schema.yml"))
    action = DeleteResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column_name="name",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_delete_resource_column_no_column_error(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    action = DeleteResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column_name="unknown",
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


def test_reorder_resource_columns(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    action = ReorderResourceColumns(
        resource=resource,
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
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    with pytest.raises(PumpkinError):
        ReorderResourceColumns(
            resource=resource,
            path=Path("models/staging/_schema.yml"),
            columns_order=["name", "id", "id"],
        )


def test_reorder_resource_columns_unknown_column_error(files):
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"))
    action = ReorderResourceColumns(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        columns_order=["name", "id", "unknown"],
    )

    with pytest.raises(PumpkinError):
        action.execute(files)


# Versioned models tests


@pytest.fixture
def versioned_files() -> dict[Path, dict]:
    """Files fixture with a versioned model."""
    return {
        Path("models/staging/_schema.yml"): {
            "version": 2,
            "models": [
                {
                    "name": "stg_customers",
                    "latest_version": 2,
                    "versions": [
                        {
                            "v": 1,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                            ],
                        },
                        {
                            "v": 2,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                                {"name": "parent_id", "data_type": "int"},
                            ],
                        },
                    ],
                },
            ],
        },
    }


def test_bootstrap_versioned_model_error():
    """Test that bootstrapping a versioned model raises an error."""
    resource = make_model("stg_orders", yaml_path=Path("models/staging/_schema.yml"), version=1)
    with pytest.raises(PumpkinError, match="Versioned models cannot be bootstrapped"):
        BootstrapResource(resource=resource, path=Path("models/staging/_schema.yml"))


def test_add_column_to_versioned_model(versioned_files):
    """Test adding a column to a specific version of a versioned model."""
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=1)
    column = ResourceColumn(name="name", quote=False, data_type="varchar", description=None)
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    expected = copy.deepcopy(versioned_files)
    expected[Path("models/staging/_schema.yml")]["models"][0]["versions"][0]["columns"].append(
        {"name": "name", "data_type": "varchar"}
    )

    action.execute(versioned_files)
    assert versioned_files == expected


def test_update_column_in_versioned_model(versioned_files):
    """Test updating a column in a specific version of a versioned model."""
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=2)
    column = ResourceColumn(name="parent_id", quote=False, data_type="bigint", description=None)
    action = UpdateResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    expected = copy.deepcopy(versioned_files)
    expected[Path("models/staging/_schema.yml")]["models"][0]["versions"][1]["columns"][1]["data_type"] = "bigint"

    action.execute(versioned_files)
    assert versioned_files == expected


def test_delete_column_from_versioned_model(versioned_files):
    """Test deleting a column from a specific version of a versioned model."""
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=2)
    action = DeleteResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column_name="parent_id",
    )

    expected = copy.deepcopy(versioned_files)
    expected[Path("models/staging/_schema.yml")]["models"][0]["versions"][1]["columns"] = [
        {"name": "id", "data_type": "int"}
    ]

    action.execute(versioned_files)
    assert versioned_files == expected


def test_reorder_columns_in_versioned_model(versioned_files):
    """Test reordering columns in a specific version of a versioned model."""
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=2)
    action = ReorderResourceColumns(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        columns_order=["parent_id", "id"],
    )

    expected = copy.deepcopy(versioned_files)
    expected[Path("models/staging/_schema.yml")]["models"][0]["versions"][1]["columns"] = [
        {"name": "parent_id", "data_type": "int"},
        {"name": "id", "data_type": "int"},
    ]

    action.execute(versioned_files)
    assert versioned_files == expected


def test_versioned_model_version_not_found_error(versioned_files):
    """Test error when trying to modify a non-existent version."""
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=3)
    column = ResourceColumn(name="address", quote=False, data_type="varchar", description=None)
    action = AddResourceColumn(
        resource=resource,
        path=Path("models/staging/_schema.yml"),
        column=column,
    )

    with pytest.raises(PumpkinError, match="Version 3 not found"):
        action.execute(versioned_files)


def test_relocate_versioned_model():
    """Test relocating a versioned model moves the entire model with all versions."""
    files = {
        Path("models/staging/_schema.yml"): {
            "version": 2,
            "models": [
                {
                    "name": "stg_customers",
                    "latest_version": 2,
                    "versions": [
                        {
                            "v": 1,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                            ],
                        },
                        {
                            "v": 2,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                                {"name": "parent_id", "data_type": "int"},
                            ],
                        },
                    ],
                },
                {
                    "name": "stg_orders",
                    "columns": [{"name": "id", "data_type": "int"}],
                },
            ],
        },
    }

    # Relocate the versioned model (using v1 as the resource representative)
    # Note: In real usage, the planner would select one version, but relocation moves the entire model
    resource = make_model("stg_customers", yaml_path=Path("models/staging/_schema.yml"), version=1)
    action = RelocateResource(
        resource=resource,
        from_path=Path("models/staging/_schema.yml"),
        to_path=Path("models/staging/stg_customers.yml"),
    )

    expected = {
        Path("models/staging/_schema.yml"): {
            "version": 2,
            "models": [
                {
                    "name": "stg_orders",
                    "columns": [{"name": "id", "data_type": "int"}],
                },
            ],
        },
        Path("models/staging/stg_customers.yml"): {
            "version": 2,
            "models": [
                {
                    "name": "stg_customers",
                    "latest_version": 2,
                    "versions": [
                        {
                            "v": 1,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                            ],
                        },
                        {
                            "v": 2,
                            "columns": [
                                {"name": "id", "data_type": "int"},
                                {"name": "parent_id", "data_type": "int"},
                            ],
                        },
                    ],
                },
            ],
        },
    }

    action.execute(files)
    assert files == expected
