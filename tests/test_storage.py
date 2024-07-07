import textwrap
from pathlib import Path

import yaml

from dbt_pumpkin.storage import DiskStorage


def test_load_yaml(tmp_path: Path):
    (tmp_path / "schema.yml").write_text(
        textwrap.dedent("""\
        version: 2
        models:
            - name: my_model
    """)
    )

    files = DiskStorage(tmp_path, read_only=False).load_yaml({Path("schema.yml"), Path("absent.yml")})
    assert files == {Path("schema.yml"): {"version": 2, "models": [{"name": "my_model"}]}}


def test_save_yaml(tmp_path: Path):
    DiskStorage(tmp_path, read_only=False).save_yaml(
        {Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}}
    )

    actual = yaml.safe_load((tmp_path / "schema.yml").read_text())
    assert actual == {"version": 2, "models": [{"name": "my_other_model"}]}


def test_save_yaml_read_only(tmp_path: Path):
    DiskStorage(tmp_path, read_only=True).save_yaml(
        {Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}}
    )

    assert not (tmp_path / "schema.yml").exists()


def test_roundtrip(tmp_path: Path):
    content = textwrap.dedent("""\
        version: 2
        models:
        # TODO rename it!
        - name: my_model
          description: my very first model
          columns:
          - name: id
            data_type: short # or is it int actually?
          - name: name
    """)

    (tmp_path / "my_model.yml").write_text(content)

    storage = DiskStorage(tmp_path, read_only=False)
    files = storage.load_yaml({Path("my_model.yml")})
    storage.save_yaml(files)

    actual = (tmp_path / "my_model.yml").read_text()

    # Ruamel adds newlines after comments
    # It should be fine as we don't re-write a file if there is nothing to change in it
    expected = textwrap.dedent("""\
        version: 2
        models:
        # TODO rename it!

        - name: my_model
          description: my very first model
          columns:
          - name: id
            data_type: short # or is it int actually?

          - name: name
    """)

    assert expected == actual
