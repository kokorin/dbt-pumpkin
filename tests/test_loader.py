from __future__ import annotations

from pathlib import Path

import pytest

from dbt_pumpkin.data import Column, Resource, ResourceConfig, ResourceID, ResourceType, Table
from dbt_pumpkin.loader import ResourceLoader


def loader(selects: list[str] | None = None, excludes: list[str] | None = None) -> ResourceLoader:
    return ResourceLoader("tests/my_pumpkin", "tests/my_pumpkin", selects, excludes)


# FIXTURES


@pytest.fixture(scope="module")
def loader_all() -> ResourceLoader:
    return loader()


@pytest.fixture
def loader_only_sources() -> ResourceLoader:
    return loader(selects=["source:*"])


@pytest.fixture
def loader_only_seeds() -> ResourceLoader:
    return loader(selects=["resource_type:seed"])


@pytest.fixture
def loader_only_snapshots() -> ResourceLoader:
    return loader(selects=["resource_type:snapshot"])


@pytest.fixture
def loader_only_models() -> ResourceLoader:
    return loader(selects=["resource_type:model"])


# TESTS


def test_manifest(loader_all):
    assert loader_all.manifest

    assert loader_all.manifest.nodes
    assert loader_all.manifest.sources


def test_selected_resource_ids(loader_all):
    assert loader_all.resource_ids == {
        ResourceType.SEED: {
            ResourceID("seed.my_pumpkin.seed_customers"),
        },
        ResourceType.SOURCE: {
            ResourceID("source.my_pumpkin.pumpkin.customers"),
        },
        ResourceType.MODEL: {
            ResourceID("model.my_pumpkin.stg_customers"),
        },
        ResourceType.SNAPSHOT: {
            ResourceID("snapshot.my_pumpkin.customers_snapshot"),
        },
    }


def test_selected_resource_ids_only_sources(loader_only_sources: ResourceLoader):
    assert loader_only_sources.resource_ids == {
        ResourceType.SOURCE: {
            ResourceID("source.my_pumpkin.pumpkin.customers"),
        }
    }


def test_selected_resource_ids_only_seeds(loader_only_seeds: ResourceLoader):
    assert loader_only_seeds.resource_ids == {
        ResourceType.SEED: {
            ResourceID("seed.my_pumpkin.seed_customers"),
        },
    }


def test_selected_resource_ids_only_snapshots(loader_only_snapshots: ResourceLoader):
    assert loader_only_snapshots.resource_ids == {
        ResourceType.SNAPSHOT: {
            ResourceID("snapshot.my_pumpkin.customers_snapshot"),
        },
    }


def test_selected_resource_ids_only_models(loader_only_models: ResourceLoader):
    assert loader_only_models.resource_ids == {
        ResourceType.MODEL: {
            ResourceID("model.my_pumpkin.stg_customers"),
        },
    }


def test_selected_resources(loader_all):
    assert set(loader_all.resources) == {
        Resource(
            unique_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            name="customers",
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            type=ResourceType.SOURCE,
            yaml_path=Path("models/staging/_sources.yml"),
            columns=[],
            config=ResourceConfig(),
        ),
        Resource(
            unique_id=ResourceID("model.my_pumpkin.stg_customers"),
            name="stg_customers",
            database="dev",
            schema="main",
            identifier="stg_customers",
            type=ResourceType.MODEL,
            yaml_path=Path("models/staging/_schema.yml"),
            columns=[Column(name="id", data_type=None, description="")],
            config=ResourceConfig(),
        ),
        Resource(
            unique_id=ResourceID("seed.my_pumpkin.seed_customers"),
            name="seed_customers",
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            type=ResourceType.SEED,
            yaml_path=None,
            columns=[],
            config=ResourceConfig(),
        ),
        Resource(
            unique_id=ResourceID("snapshot.my_pumpkin.customers_snapshot"),
            name="customers_snapshot",
            database="dev",
            schema="sources_snapshot",
            identifier="customers_snapshot",
            type=ResourceType.SNAPSHOT,
            yaml_path=None,
            columns=[],
            config=ResourceConfig(),
        ),
    }


def test_selected_resources_total_count(loader_all):
    assert sum(len(ids) for ids in loader_all.resource_ids.values()) == len(loader_all.resources)


def test_selected_resource_tables(loader_all):
    assert set(loader_all.resource_tables) == {
        Table(
            resource_id=ResourceID("seed.my_pumpkin.seed_customers"),
            columns=[
                Column(name="id", data_type="INTEGER", description=None),
                Column(name="name", data_type="character varying(256)", description=None),
            ],
        ),
        Table(
            resource_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            columns=[
                Column(name="id", data_type="INTEGER", description=None),
                Column(name="name", data_type="character varying(256)", description=None),
            ],
        ),
        Table(
            resource_id=ResourceID("model.my_pumpkin.stg_customers"),
            columns=[
                Column(name="id", data_type="INTEGER", description=None),
                Column(name="name", data_type="character varying(256)", description=None),
            ],
        ),
        Table(
            resource_id=ResourceID("snapshot.my_pumpkin.customers_snapshot"),
            columns=[
                Column(name="id", data_type="INTEGER", description=None),
                Column(name="name", data_type="character varying(256)", description=None),
                Column(name="dbt_scd_id", data_type="character varying(256)", description=None),
                Column(name="dbt_updated_at", data_type="TIMESTAMP", description=None),
                Column(name="dbt_valid_from", data_type="TIMESTAMP", description=None),
                Column(name="dbt_valid_to", data_type="TIMESTAMP", description=None),
            ],
        ),
    }


def test_selected_resource_paths(loader_all):
    resource_paths = loader_all.resource_yaml_paths
    assert resource_paths
    assert resource_paths == {
        ResourceID("model.my_pumpkin.stg_customers"): Path("models/staging/_schema.yml"),
        ResourceID("source.my_pumpkin.pumpkin.customers"): Path("models/staging/_sources.yml"),
    }
