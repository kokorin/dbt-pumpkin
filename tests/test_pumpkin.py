from dbt_pumpkin.pumpkin import parse_manifest

def test_parse_manifest():
    manifest = parse_manifest('tests/my_pumpkin', 'tests/my_pumpkin')
    assert manifest

    assert manifest.nodes
    assert manifest.sources
    