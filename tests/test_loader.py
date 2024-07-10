from __future__ import annotations

import os
import shutil
import textwrap
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

import pytest
from dbt_pumpkin.dbt_compat import dbtRunner
from ruamel.yaml import YAML

from dbt_pumpkin.data import Resource, ResourceColumn, ResourceConfig, ResourceID, ResourceType, Table, TableColumn
from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams


def loader(selects: list[str] | None = None, excludes: list[str] | None = None) -> ResourceLoader:
    return ResourceLoader(
        project_params=ProjectParams(
            "tests/my_pumpkin",
            "tests/my_pumpkin",
        ),
        resource_params=ResourceParams(select=selects, exclude=excludes),
    )


# FIXTURES


@pytest.fixture
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


yaml = YAML(typ="safe")


@dataclass
class Project:
    project_yml: dict[str, Any]
    project_files: dict[str, Any]
    profiles_yml: dict[str, Any] | None = None
    local_packages: list[Project] | None = None


def fake_dbt_project_loader(project: Project) -> ResourceLoader:
    project_dir = Path(mkdtemp(prefix="test_pumpkin_"))

    default_profiles = {
        "test_pumpkin": {
            "target": "test",
            "outputs": {
                "test": {
                    # Comment to stop formatting in 1 line
                    "type": "duckdb",
                    "path": f"{project_dir}/dev.duckdb",
                    "threads": 1,
                }
            },
        }
    }

    def create_project(root: Path, project: Project):
        project_yaml = {**{"packages-install-path": str(root / "dbt_packages")}, **project.project_yml.copy()}
        yaml.dump(project_yaml, root / "dbt_project.yml")

        for path_str, content in project.project_files.items():
            path = root / path_str
            path.parent.mkdir(exist_ok=True)
            path.write_text(content, encoding="utf-8")

        if project.local_packages:
            packages_yml = {}

            for package in project.local_packages:
                package_name = package.project_yml["name"]
                package_root = root / "sub_packages" / package_name
                package_root.mkdir(parents=True, exist_ok=True)

                create_project(package_root, package)

                packages_yml.setdefault("packages", []).append({"local": str(package_root)})

            yaml.dump(packages_yml, root / "packages.yml")
            # DBT 1.5 can't install local deps on Windows, we just copy packages
            # Besides that DBT 1.8 and earlier changes CWD when executing `dbt deps`
            # # https://github.com/dbt-labs/dbt-core/issues/8997
            # so copying file tree is the easiest fix

            shutil.copytree(root / "sub_packages", root / "dbt_packages")

    create_project(project_dir, project)

    yaml.dump(default_profiles, project_dir / "profiles.yml")

    return ResourceLoader(
        project_params=ProjectParams(project_dir=project_dir, profiles_dir=project_dir),
        resource_params=ResourceParams(),
    )


@pytest.fixture
def loader_multiple_roots():
    return fake_dbt_project_loader(
        Project(
            project_yml={
                "name": "test_pumpkin",
                "version": "0.1.0",
                "profile": "test_pumpkin",
                "model-paths": ["models", "models_{{ var('absent_var', 'extra') }}"],
                "seed-paths": ["seeds", "seeds_{{ var('absent_var', 'extra') }}"],
                "snapshot-paths": ["snapshots", "snapshots_{{ var('absent_var', 'extra') }}"],
            },
            project_files={
                "models/customers.sql": "select 1 as id",
                "models/customers.yml": textwrap.dedent("""\
                version: 2
                models:
                  - name: customers
            """),
                "models_extra/extra_customers.sql": "select 1 as id",
                "models_extra/extra_customers.yml": textwrap.dedent("""\
                version: 2
                models:
                  - name: extra_customers
            """),
                "seeds/seed_customers.csv": textwrap.dedent("""\
                id,name
                42,John
            """),
                "seeds/seed_customers.yml": textwrap.dedent("""\
                version: 2
                seeds:
                  - name: seed_customers
            """),
                "seeds_extra/seed_extra_customers.csv": textwrap.dedent("""\
                id,name
                42,John
            """),
                "seeds_extra/seed_extra_customers.yml": textwrap.dedent("""\
                version: 2
                seeds:
                  - name: seed_extra_customers
            """),
                "models/sources.yml": textwrap.dedent("""\
                version: 2
                sources:
                  - name: pumpkin
                    schema: main_sources
                    tables:
                      - name: customers
                        identifier: seed_customers
            """),
                "models_extra/sources.yml": textwrap.dedent("""\
                version: 2
                sources:
                  - name: extra_pumpkin
                    schema: main_sources
                    tables:
                      - name: customers
                        identifier: seed_customers
            """),
                "snapshots/customers_snapshot.sql": textwrap.dedent("""\
                {% snapshot customers_snapshot %}
                    {{ config(unique_key='id', target_schema='snapshots', strategy='check', check_cols='all') }}
                    select * from {{ source('pumpkin', 'customers') }}
                {% endsnapshot %}
            """),
                "snapshots/customers_snapshot.yml": textwrap.dedent("""\
                version: 2
                snapshots:
                  - name: customers_snapshot
            """),
                "snapshots_extra/extra_customers_snapshot.sql": textwrap.dedent("""\
                {% snapshot extra_customers_snapshot %}
                    {{ config(unique_key='id', target_schema='extra_snapshots', strategy='check', check_cols='all') }}
                    select * from {{ source('extra_pumpkin', 'customers') }}
                {% endsnapshot %}
            """),
                "snapshots_extra/extra_customers_snapshot.yml": textwrap.dedent("""\
                version: 2
                snapshots:
                  - name: extra_customers_snapshot
            """),
            },
        )
    )


@pytest.fixture
def loader_configured_paths():
    return fake_dbt_project_loader(
        Project(
            project_yml={
                "name": "test_pumpkin",
                "version": "0.1.0",
                "profile": "test_pumpkin",
                "seeds": {"test_pumpkin": {"+dbt-pumpkin-path": "_seeds.yml"}},
                "models": {"test_pumpkin": {"+dbt-pumpkin-path": "_models.yml"}},
                "snapshots": {"test_pumpkin": {"+dbt-pumpkin-path": "_snapshots.yml"}},
                "sources": {"test_pumpkin": {"+dbt-pumpkin-path": "_sources.yml"}},
            },
            project_files={
                "models/customers.sql": "select 1 as id",
                "seeds/seed_customers.csv": textwrap.dedent("""\
                 id,name
                 42,John
             """),
                "models/sources.yml": textwrap.dedent("""\
                 version: 2
                 sources:
                   - name: pumpkin
                     schema: main_sources
                     tables:
                       - name: customers
                       - name: orders
             """),
                "snapshots/customers_snapshot.sql": textwrap.dedent("""\
                 {% snapshot customers_snapshot %}
                     {{ config(unique_key='id', target_schema='snapshots', strategy='check', check_cols='all') }}
                     select * from {{ source('pumpkin', 'customers') }}
                 {% endsnapshot %}
             """),
            },
        )
    )


@pytest.fixture
def loader_with_deps():
    return fake_dbt_project_loader(
        Project(
            project_yml={
                "name": "test_pumpkin",
                "version": "0.1.0",
                "profile": "test_pumpkin",
            },
            project_files={
                "models/customers.sql": "select 1 as id",
            },
            local_packages=[
                Project(
                    project_yml={
                        "name": "extra",
                        "version": "0.1.0",
                        "profile": "test_pumpkin",
                    },
                    project_files={
                        "models/extra_customers.sql": "select 1 as id",
                    },
                )
            ],
        )
    )


# TESTS


def test_manifest(loader_all):
    manifest = loader_all.load_manifest()
    assert manifest

    assert manifest.nodes
    assert manifest.sources


def test_selected_resource_ids(loader_all):
    assert loader_all.select_resource_ids() == {
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
    assert loader_only_sources.select_resource_ids() == {
        ResourceType.SOURCE: {
            ResourceID("source.my_pumpkin.pumpkin.customers"),
        }
    }


def test_selected_resource_ids_only_seeds(loader_only_seeds: ResourceLoader):
    assert loader_only_seeds.select_resource_ids() == {
        ResourceType.SEED: {
            ResourceID("seed.my_pumpkin.seed_customers"),
        },
    }


def test_selected_resource_ids_only_snapshots(loader_only_snapshots: ResourceLoader):
    assert loader_only_snapshots.select_resource_ids() == {
        ResourceType.SNAPSHOT: {
            ResourceID("snapshot.my_pumpkin.customers_snapshot"),
        },
    }


def test_selected_resource_ids_only_models(loader_only_models: ResourceLoader):
    assert loader_only_models.select_resource_ids() == {
        ResourceType.MODEL: {
            ResourceID("model.my_pumpkin.stg_customers"),
        },
    }


def test_selected_resources_non_project_resources_excluded(loader_with_deps):
    assert loader_with_deps.select_resource_ids() == {
        ResourceType.MODEL: {
            ResourceID("model.test_pumpkin.customers"),
        },
    }


def test_selected_resources(loader_all):
    def sort_order(res: Resource):
        return str(res.unique_id)

    assert loader_all.select_resources().sort(key=sort_order) == [
        Resource(
            unique_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            name="customers",
            source_name="pumpkin",
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            type=ResourceType.SOURCE,
            path=None,
            yaml_path=Path("models/staging/_sources.yml"),
            columns=[],
            config=ResourceConfig(yaml_path_template=None),
        ),
        Resource(
            unique_id=ResourceID("model.my_pumpkin.stg_customers"),
            name="stg_customers",
            source_name=None,
            database="dev",
            schema="main",
            identifier="stg_customers",
            type=ResourceType.MODEL,
            path=Path("models/staging/stg_customers.sql"),
            yaml_path=Path("models/staging/_schema.yml"),
            columns=[ResourceColumn(name="id", quote=False, data_type=None, description="")],
            config=ResourceConfig(yaml_path_template="_schema.yml"),
        ),
        Resource(
            unique_id=ResourceID("seed.my_pumpkin.seed_customers"),
            name="seed_customers",
            source_name=None,
            database="dev",
            schema="main_sources",
            identifier="seed_customers",
            type=ResourceType.SEED,
            path=Path("seeds/sources/seed_customers.csv"),
            yaml_path=None,
            columns=[],
            config=ResourceConfig(yaml_path_template=None),
        ),
        Resource(
            unique_id=ResourceID("snapshot.my_pumpkin.customers_snapshot"),
            name="customers_snapshot",
            source_name=None,
            database="dev",
            schema="sources_snapshot",
            identifier="customers_snapshot",
            type=ResourceType.SNAPSHOT,
            path=Path("snapshots/sources/customers_snapshot.sql"),
            yaml_path=None,
            columns=[],
            config=ResourceConfig(yaml_path_template=None),
        ),
    ].sort(key=sort_order)


def test_selected_resource_paths_multiroot(loader_multiple_roots):
    assert {r.unique_id: r.path for r in loader_multiple_roots.select_resources()} == {
        ResourceID("model.test_pumpkin.customers"): Path("models/customers.sql"),
        ResourceID("model.test_pumpkin.extra_customers"): Path("models_extra/extra_customers.sql"),
        ResourceID("seed.test_pumpkin.seed_customers"): Path("seeds/seed_customers.csv"),
        ResourceID("seed.test_pumpkin.seed_extra_customers"): Path("seeds_extra/seed_extra_customers.csv"),
        ResourceID("snapshot.test_pumpkin.extra_customers_snapshot"): Path(
            "snapshots_extra/extra_customers_snapshot.sql"
        ),
        ResourceID("snapshot.test_pumpkin.customers_snapshot"): Path("snapshots/customers_snapshot.sql"),
        ResourceID("source.test_pumpkin.pumpkin.customers"): None,
        ResourceID("source.test_pumpkin.extra_pumpkin.customers"): None,
    }


def test_selected_resource_yaml_paths_multiroot(loader_multiple_roots):
    assert {r.unique_id: r.yaml_path for r in loader_multiple_roots.select_resources()} == {
        ResourceID("model.test_pumpkin.customers"): Path("models/customers.yml"),
        ResourceID("model.test_pumpkin.extra_customers"): Path("models_extra/extra_customers.yml"),
        ResourceID("seed.test_pumpkin.seed_customers"): Path("seeds/seed_customers.yml"),
        ResourceID("seed.test_pumpkin.seed_extra_customers"): Path("seeds_extra/seed_extra_customers.yml"),
        ResourceID("snapshot.test_pumpkin.extra_customers_snapshot"): Path(
            "snapshots_extra/extra_customers_snapshot.yml"
        ),
        ResourceID("snapshot.test_pumpkin.customers_snapshot"): Path("snapshots/customers_snapshot.yml"),
        ResourceID("source.test_pumpkin.pumpkin.customers"): Path("models/sources.yml"),
        ResourceID("source.test_pumpkin.extra_pumpkin.customers"): Path("models_extra/sources.yml"),
    }


def test_selected_resource_config(loader_configured_paths):
    assert {r.unique_id: r.config for r in loader_configured_paths.select_resources()} == {
        ResourceID(unique_id="source.test_pumpkin.pumpkin.customers"): ResourceConfig(
            yaml_path_template="_sources.yml"
        ),
        ResourceID(unique_id="source.test_pumpkin.pumpkin.orders"): ResourceConfig(yaml_path_template="_sources.yml"),
        ResourceID(unique_id="seed.test_pumpkin.seed_customers"): ResourceConfig(yaml_path_template="_seeds.yml"),
        ResourceID(unique_id="snapshot.test_pumpkin.customers_snapshot"): ResourceConfig(
            yaml_path_template="_snapshots.yml"
        ),
        ResourceID(unique_id="model.test_pumpkin.customers"): ResourceConfig(yaml_path_template="_models.yml"),
    }


def test_selected_resources_total_count(loader_all):
    assert sum(len(ids) for ids in loader_all.select_resource_ids().values()) == len(loader_all.select_resources())


def test_selected_resource_tables(loader_all):
    assert set(loader_all.lookup_tables()) == {
        Table(
            resource_id=ResourceID("seed.my_pumpkin.seed_customers"),
            columns=[
                TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
                TableColumn(
                    name="name", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
                ),
            ],
        ),
        Table(
            resource_id=ResourceID("source.my_pumpkin.pumpkin.customers"),
            columns=[
                TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
                TableColumn(
                    name="name", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
                ),
            ],
        ),
        Table(
            resource_id=ResourceID("model.my_pumpkin.stg_customers"),
            columns=[
                TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
                TableColumn(
                    name="name", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
                ),
            ],
        ),
        Table(
            resource_id=ResourceID("snapshot.my_pumpkin.customers_snapshot"),
            columns=[
                TableColumn(name="id", dtype="INTEGER", data_type="INTEGER", is_numeric=False, is_string=False),
                TableColumn(
                    name="name", dtype="VARCHAR", data_type="character varying(256)", is_numeric=False, is_string=True
                ),
                TableColumn(
                    name="dbt_scd_id",
                    dtype="VARCHAR",
                    data_type="character varying(256)",
                    is_numeric=False,
                    is_string=True,
                ),
                TableColumn(
                    name="dbt_updated_at", dtype="TIMESTAMP", data_type="TIMESTAMP", is_numeric=False, is_string=False
                ),
                TableColumn(
                    name="dbt_valid_from", dtype="TIMESTAMP", data_type="TIMESTAMP", is_numeric=False, is_string=False
                ),
                TableColumn(
                    name="dbt_valid_to", dtype="TIMESTAMP", data_type="TIMESTAMP", is_numeric=False, is_string=False
                ),
            ],
        ),
    }


def test_selected_resource_tables_no_actual_tables(loader_configured_paths):
    assert [] == loader_configured_paths.lookup_tables()
