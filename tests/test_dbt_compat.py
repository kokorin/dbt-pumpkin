from _pytest.monkeypatch import MonkeyPatch

from dbt_pumpkin.dbt_compat import prepare_monkey_patches
from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams


def new_loader() -> ResourceLoader:
    return ResourceLoader(
        project_params=ProjectParams(
            "tests/my_pumpkin",
            "tests/my_pumpkin",
        ),
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
    not_patched = new_loader().lookup_tables()
    assert not_patched

    with monkeypatch.context() as m:
        for patch in prepare_monkey_patches():
            m.setattr(patch.obj, patch.name, patch.value)

        patched = new_loader().lookup_tables()

    assert patched
    assert set(not_patched) == set(patched)
