import pytest
from dbt_pumpkin.canon import LowercaseCanon, UppercaseCanon
from dbt_pumpkin.exception import NamingCanonError


def test_uppercase_canon():
    canon = UppercaseCanon()
    assert canon.canonize("NAME") == "NAME"
    assert canon.canonize("name") == "NAME"
    assert canon.canonize("Name") == "NAME"
    assert canon.canonize("NaMe") == "NAME"
    assert canon.canonize("_name") == "_NAME"
    assert canon.canonize("_na_me") == "_NA_ME"
    assert canon.canonize("_nam3") == "_NAM3"
    assert canon.canonize("nAm3") == "NAM3"


def test_lowercase_canon():
    canon = LowercaseCanon()
    assert canon.canonize("name") == "name"
    assert canon.canonize("NAME") == "name"
    assert canon.canonize("Name") == "name"
    assert canon.canonize("NaMe") == "name"
    assert canon.canonize("_name") == "_name"
    assert canon.canonize("_na_me") == "_na_me"
    assert canon.canonize("_nam3") == "_nam3"
    assert canon.canonize("nAm3") == "nam3"


@pytest.mark.parametrize(
    argnames="name",
    argvalues=[
        ("na me"),
        ("_na me"),
        ("Na me"),
        ("_Na me"),
        ("3name"),
    ],
)
def test_non_canonizeable(name):
    with pytest.raises(NamingCanonError):
        UppercaseCanon().canonize(name)

    with pytest.raises(NamingCanonError):
        LowercaseCanon().canonize(name)
