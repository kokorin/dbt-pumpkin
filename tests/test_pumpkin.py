import pytest
from dbt_pumpkin.pumpkin import Pumpkin
from typing import List


def pumpkin(selects: List[str] = None, excludes: List[str] = None) -> Pumpkin:
    return Pumpkin("tests/my_pumpkin", "tests/my_pumpkin", selects, excludes)


# FIXTURES


@pytest.fixture(scope="module")
def pumpkin_all() -> Pumpkin:
    return pumpkin()


@pytest.fixture(scope="module")
def pumpkin_only_sources() -> Pumpkin:
    return pumpkin(selects=["source:*"])


@pytest.fixture(scope="module")
def pumpkin_only_seeds() -> Pumpkin:
    return pumpkin(selects=["resource_type:seed"])


@pytest.fixture(scope="module")
def pumpkin_only_snapshots() -> Pumpkin:
    return pumpkin(selects=["resource_type:snapshot"])


@pytest.fixture(scope="module")
def pumpkin_only_models() -> Pumpkin:
    return pumpkin(selects=["resource_type:model"])


# TESTS


def test_manifest(pumpkin_all):
    assert pumpkin_all.manifest

    assert pumpkin_all.manifest.nodes
    assert pumpkin_all.manifest.sources


def test_selected_unique_ids(pumpkin_all):
    assert pumpkin_all.selected_unique_ids == {
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


def test_selected_unique_ids_only_sources(pumpkin_only_sources: Pumpkin):
    assert pumpkin_only_sources.selected_unique_ids == {
        "source": {
            "source.my_pumpkin.pumpkin.customers",
        }
    }


def test_selected_unique_ids_only_seeds(pumpkin_only_seeds: Pumpkin):
    assert pumpkin_only_seeds.selected_unique_ids == {
        "seed": {
            "seed.my_pumpkin.seed_customers",
        },
    }


def test_selected_unique_ids_only_snapshots(pumpkin_only_snapshots: Pumpkin):
    assert pumpkin_only_snapshots.selected_unique_ids == {
        "snapshot": {
            "snapshot.my_pumpkin.customers_snapshot",
        },
    }


def test_selected_unique_ids_only_models(pumpkin_only_models: Pumpkin):
    assert pumpkin_only_models.selected_unique_ids == {
        "model": {
            "model.my_pumpkin.stg_customers",
        },
    }


def test_selected_sources(pumpkin_all):
    assert {s.unique_id for s in pumpkin_all.selected_sources} == {
        "source.my_pumpkin.pumpkin.customers",
    }


def test_selected_models(pumpkin_all):
    assert {s.unique_id for s in pumpkin_all.selected_models} == {
        "model.my_pumpkin.stg_customers",
    }


def test_selected_seeds(pumpkin_all):
    assert {s.unique_id for s in pumpkin_all.selected_seeds} == {"seed.my_pumpkin.seed_customers"}


def test_selected_snapshots(pumpkin_all):
    assert {s.unique_id for s in pumpkin_all.selected_snapshots} == {
        "snapshot.my_pumpkin.customers_snapshot",
    }


def test_selected_sources_and_nodes_total_count(pumpkin_all):
    assert len(pumpkin_all.selected_unique_ids) == (
        len(pumpkin_all.selected_sources)
        + len(pumpkin_all.selected_seeds)
        + len(pumpkin_all.selected_models)
        + len(pumpkin_all.selected_snapshots)
    )
