import textwrap
from pathlib import Path

import yaml

from dbt_pumpkin.storage import DiskStorage


def test_load_yaml(tmp_path):
    (tmp_path / "schema.yml").write_text(
        textwrap.dedent("""\
        version: 2
        models:
            - name: my_model
    """)
    )

    files = DiskStorage(tmp_path, read_only=False).load_yaml({Path("schema.yml"), Path("absent.yml")})
    assert files == {Path("schema.yml"): {"version": 2, "models": [{"name": "my_model"}]}}


def test_save_yaml(tmp_path):
    DiskStorage(tmp_path, read_only=False).save_yaml(
        {Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}}
    )

    actual = yaml.safe_load((tmp_path / "schema.yml").read_text())
    assert actual == {"version": 2, "models": [{"name": "my_other_model"}]}


def test_save_yaml_read_only(tmp_path):
    DiskStorage(tmp_path, read_only=True).save_yaml(
        {Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}}
    )

    assert not (tmp_path / "schema.yml").exists()
