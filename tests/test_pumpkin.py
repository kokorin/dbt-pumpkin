import pytest
from dbt_pumpkin.pumpkin import Pumpkin
from typing import List
from dbt_pumpkin.dbt_compat import ColumnInfo


def pumpkin(selects: List[str] = None, excludes: List[str] = None) -> Pumpkin:
    return Pumpkin("tests/my_pumpkin", "tests/my_pumpkin", selects, excludes)


# FIXTURES


@pytest.fixture(scope="module")
def pumpkin_all() -> Pumpkin:
    return pumpkin()


@pytest.fixture
def pumpkin_only_sources() -> Pumpkin:
    return pumpkin(selects=["source:*"])


@pytest.fixture
def pumpkin_only_seeds() -> Pumpkin:
    return pumpkin(selects=["resource_type:seed"])


@pytest.fixture
def pumpkin_only_snapshots() -> Pumpkin:
    return pumpkin(selects=["resource_type:snapshot"])


@pytest.fixture
def pumpkin_only_models() -> Pumpkin:
    return pumpkin(selects=["resource_type:model"])


# TESTS


def test_manifest(pumpkin_all):
    assert pumpkin_all.manifest

    assert pumpkin_all.manifest.nodes
    assert pumpkin_all.manifest.sources


def test_selected_resource_ids(pumpkin_all):
    assert pumpkin_all.selected_resource_ids == {
        "seed": {
            "seed.my_pumpkin.seed_customers",
        },
        "source": {
            "source.my_pumpkin.pumpkin.customers",
        },
        "model": {
            "model.my_pumpkin.stg_customers",
        },
        "snapshot": {
            "snapshot.my_pumpkin.customers_snapshot",
        },
    }


def test_selected_resource_ids_only_sources(pumpkin_only_sources: Pumpkin):
    assert pumpkin_only_sources.selected_resource_ids == {
        "source": {
            "source.my_pumpkin.pumpkin.customers",
        }
    }


def test_selected_resource_ids_only_seeds(pumpkin_only_seeds: Pumpkin):
    assert pumpkin_only_seeds.selected_resource_ids == {
        "seed": {
            "seed.my_pumpkin.seed_customers",
        },
    }


def test_selected_resource_ids_only_snapshots(pumpkin_only_snapshots: Pumpkin):
    assert pumpkin_only_snapshots.selected_resource_ids == {
        "snapshot": {
            "snapshot.my_pumpkin.customers_snapshot",
        },
    }


def test_selected_resource_ids_only_models(pumpkin_only_models: Pumpkin):
    assert pumpkin_only_models.selected_resource_ids == {
        "model": {
            "model.my_pumpkin.stg_customers",
        },
    }


def test_selected_resources(pumpkin_all):
    assert {s.unique_id for s in pumpkin_all.selected_resources} == {
        "source.my_pumpkin.pumpkin.customers",
        "model.my_pumpkin.stg_customers",
        "seed.my_pumpkin.seed_customers",
        "snapshot.my_pumpkin.customers_snapshot",
    }


def test_selected_resources_total_count(pumpkin_all):
    assert sum(len(ids) for ids in pumpkin_all.selected_resource_ids.values()) == len(pumpkin_all.selected_resources)


def test_selected_resource_actual_schemas(pumpkin_all):
    actual_schemas = pumpkin_all.selected_resource_actual_schemas
    assert actual_schemas.keys() == {
        "source.my_pumpkin.pumpkin.customers",
        "model.my_pumpkin.stg_customers",
        "seed.my_pumpkin.seed_customers",
        "snapshot.my_pumpkin.customers_snapshot",
    }

    assert actual_schemas["seed.my_pumpkin.seed_customers"] == [
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="character varying(256)"),
    ]
    assert actual_schemas["source.my_pumpkin.pumpkin.customers"] == [
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="character varying(256)"),
    ]
    assert actual_schemas["model.my_pumpkin.stg_customers"] == [
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="character varying(256)"),
    ]
    assert actual_schemas["snapshot.my_pumpkin.customers_snapshot"] == [
        ColumnInfo(name="id", data_type="INTEGER"),
        ColumnInfo(name="name", data_type="character varying(256)"),
        ColumnInfo(name="dbt_scd_id", data_type="character varying(256)"),
        ColumnInfo(name="dbt_updated_at", data_type="TIMESTAMP"),
        ColumnInfo(name="dbt_valid_from", data_type="TIMESTAMP"),
        ColumnInfo(name="dbt_valid_to", data_type="TIMESTAMP"),
    ]
