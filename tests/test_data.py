import pytest

from dbt_pumpkin.data import ResourceID, ResourceType


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
