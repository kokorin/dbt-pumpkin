import pytest
from _pytest.monkeypatch import MonkeyPatch

from dbt_pumpkin.dbt_compat import prepare_monkey_patches
from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams

from .mock import mock_project


@pytest.fixture(scope="module")
def dbt_project_path() -> str:
    path = mock_project(
        files={
            "dbt_project.yml": """\
                    name: test_pumpkin
                    version: "0.1.0"
                    profile: test_pumpkin
                """,
            "models/customers.sql": "select 1 as id, null as test_dbt_compat",
        },
        build=True,
    )
    return str(path)


def new_loader(path: str) -> ResourceLoader:
    return ResourceLoader(
        project_params=ProjectParams(path, path),
        resource_params=ResourceParams(),
    )


def test_load_manifest(monkeypatch: MonkeyPatch, dbt_project_path):
    not_patched = new_loader(dbt_project_path).load_manifest()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader(dbt_project_path).load_manifest()

    assert patched
    assert not_patched.sources.keys() == patched.sources.keys()
    assert not_patched.nodes.keys() == patched.nodes.keys()


def test_resource_ids(monkeypatch: MonkeyPatch, dbt_project_path):
    not_patched = new_loader(dbt_project_path).list_all_resource_ids()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader(dbt_project_path).list_all_resource_ids()

    assert patched
    assert not_patched == patched


def test_resource_tables(monkeypatch: MonkeyPatch, dbt_project_path):
    not_patched = new_loader(dbt_project_path).lookup_tables()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader(dbt_project_path).lookup_tables()

    assert patched
    assert set(not_patched) == set(patched)
