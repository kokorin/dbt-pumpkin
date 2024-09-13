import pytest

from dbt_pumpkin.data import ResourceID, ResourceType, YamlFormat
from dbt_pumpkin.exception import PropertyRequiredError


@pytest.mark.parametrize(
    argnames=["unique_id", "name"],
    argvalues=[
        ("model.my_pumpkin.stg_customers", "stg_customers"),
        ("seed.my_pumpkin.seed_customers", "seed_customers"),
        ("snapshot.my_pumpkin.customers_snapshot", "customers_snapshot"),
    ],
)
def test_resoruce_id_name(unique_id: str, name: str):
    assert ResourceID(unique_id).name == name


def test_resource_type_plural_names():
    assert ResourceType.MODEL.plural_name == "models"
    assert ResourceType.SEED.plural_name == "seeds"
    assert ResourceType.SOURCE.plural_name == "sources"
    assert ResourceType.SNAPSHOT.plural_name == "snapshots"


def test_yaml_format_both_indent_and_offset_required():
    with pytest.raises(PropertyRequiredError):
        YamlFormat(indent=2)

    with pytest.raises(PropertyRequiredError):
        YamlFormat(offset=2)

    assert YamlFormat(indent=2, offset=2)


def test_yaml_format_empty():
    YamlFormat()
