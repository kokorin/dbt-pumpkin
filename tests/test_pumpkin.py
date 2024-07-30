import textwrap
from pathlib import Path

import pytest

from dbt_pumpkin.params import ProjectParams, ResourceParams
from dbt_pumpkin.pumpkin import Pumpkin

from .mock import mock_project


@pytest.fixture(scope="module")
def project_path() -> Path:
    return mock_project(
        files={
            "dbt_project.yml": """\
                name: test_pumpkin
                version: "0.1.0"
                profile: test_pumpkin
                seeds:
                  test_pumpkin:
                    +dbt-pumpkin-path: _seeds.yml
                models:
                  test_pumpkin:
                    +dbt-pumpkin-path: _models.yml
                snapshots:
                  test_pumpkin:
                    +dbt-pumpkin-path: _snapshots.yml
                sources:
                  test_pumpkin:
                    +dbt-pumpkin-path: /models/_sources.yml
            """,
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
                     select 1 as id, 'test' as name
                 {% endsnapshot %}
             """),
        },
        build=True
    )


def test_bootstrap(project_path):
    pumpkin = Pumpkin(
        project_params=ProjectParams(project_dir=str(project_path), profiles_dir=str(project_path)),
        resource_params=ResourceParams(),
    )

    pumpkin.bootstrap(dry_run=False)


def test_relocate(project_path):
    pumpkin = Pumpkin(
        project_params=ProjectParams(project_dir=str(project_path), profiles_dir=str(project_path)),
        resource_params=ResourceParams(),
    )

    pumpkin.relocate(dry_run=False)


def test_synchronize(project_path):
    pumpkin = Pumpkin(
        project_params=ProjectParams(project_dir=str(project_path), profiles_dir=str(project_path)),
        resource_params=ResourceParams(),
    )

    pumpkin.synchronize(dry_run=False)
