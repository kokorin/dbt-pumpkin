from _pytest.monkeypatch import MonkeyPatch

from dbt_pumpkin.dbt_compat import prepare_monkey_patches
from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams

from .mock_project import Project, mock_project


def new_loader(*, build=False) -> ResourceLoader:
    path = mock_project(
        project=Project(
            project_yml={
                "name": "test_pumpkin",
                "version": "0.1.0",
                "profile": "test_pumpkin",
            },
            project_files={"models/customers.sql": "select 1 as id, null as test_dbt_compat"},
        ),
        build=build,
    )

    return ResourceLoader(
        project_params=ProjectParams(str(path), str(path)),
        resource_params=ResourceParams(),
    )


def test_load_manifest(monkeypatch: MonkeyPatch):
    not_patched = new_loader().load_manifest()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader().load_manifest()

    assert patched
    assert not_patched.sources.keys() == patched.sources.keys()
    assert not_patched.nodes.keys() == patched.nodes.keys()


def test_resource_ids(monkeypatch: MonkeyPatch):
    not_patched = new_loader().select_resource_ids()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader().select_resource_ids()

    assert patched
    assert not_patched == patched


def test_resource_tables(monkeypatch: MonkeyPatch):
    not_patched = new_loader(build=True).lookup_tables()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader(build=True).lookup_tables()

    assert patched
    assert set(not_patched) == set(patched)
