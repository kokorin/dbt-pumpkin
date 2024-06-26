from pathlib import Path

import pytest

from dbt_pumpkin.exception import NotRootRelativePathError
from dbt_pumpkin.resolver import PathResolver


@pytest.fixture
def resolver() -> PathResolver:
    return PathResolver()


def test_resolve_fixed_path(resolver):
    assert Path("models/_my_schema.yml") == resolver.resolve(
        path_template="_my_schema.yml", resource_name="my_model", resource_path=Path("models/my_model.sql")
    )

    assert Path("models/staging/_my_schema.yml") == resolver.resolve(
        path_template="_my_schema.yml", resource_name="my_model", resource_path=Path("models/staging/my_model.sql")
    )

    assert Path("models/staging/schemas/_my_schema.yml") == resolver.resolve(
        path_template="schemas/_my_schema.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )

    assert Path("models/_my_schema.yml") == resolver.resolve(
        path_template="/models/_my_schema.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )


def test_resolve_templated_path(resolver):
    assert Path("models/_my_model.yml") == resolver.resolve(
        path_template="_{name}.yml", resource_name="my_model", resource_path=Path("models/my_model.sql")
    )

    assert Path("models/_my_model.yml") == resolver.resolve(
        path_template="/models/_{name}.yml", resource_name="my_model", resource_path=Path("models/staging/my_model.sql")
    )

    assert Path("models/staging/../_my_model.yml") == resolver.resolve(
        path_template="../_{name}.yml", resource_name="my_model", resource_path=Path("models/staging/my_model.sql")
    )

    assert Path("models/staging/_staging.yml") == resolver.resolve(
        path_template="_{parent}.yml", resource_name="my_model", resource_path=Path("models/staging/my_model.sql")
    )

    assert Path("models/staging/_staging_my_model.yml") == resolver.resolve(
        path_template="_{parent}_{name}.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )

    assert Path("models/_staging.yml") == resolver.resolve(
        path_template="/models/_{parent}.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )

    assert Path("models/_staging/_my_schema.yml") == resolver.resolve(
        path_template="/models/_{parent}/_my_schema.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )

    assert Path("models/_staging/my_model.yml") == resolver.resolve(
        path_template="/models/_{parent}/{name}.yml",
        resource_name="my_model",
        resource_path=Path("models/staging/my_model.sql"),
    )


def test_resolve_templated_path_without_resource_path(resolver):
    assert Path("models/_my_source.yml") == resolver.resolve(
        path_template="/models/_{name}.yml", resource_name="my_source", resource_path=None
    )

    with pytest.raises(NotRootRelativePathError):
        resolver.resolve(path_template="_{name}.yml", resource_name="my_source", resource_path=None)

    with pytest.raises(NotRootRelativePathError):
        resolver.resolve(path_template="{parent}_{name}.yml", resource_name="my_source", resource_path=None)
