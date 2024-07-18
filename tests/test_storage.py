import platform
import textwrap
from pathlib import Path

import yaml

from dbt_pumpkin.data import YamlFormat
from dbt_pumpkin.storage import DiskStorage


def test_load_yaml(tmp_path: Path):
    (tmp_path / "schema.yml").write_text(
        textwrap.dedent("""\
        version: 2
        models:
            - name: my_model
    """)
    )

    storage = DiskStorage(tmp_path, yaml_format=None, read_only=False)
    files = storage.load_yaml({Path("schema.yml"), Path("absent.yml")})
    assert files == {Path("schema.yml"): {"version": 2, "models": [{"name": "my_model"}]}}


def test_save_yaml(tmp_path: Path):
    storage = DiskStorage(tmp_path, yaml_format=None, read_only=False)

    storage.save_yaml({Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}})

    actual = yaml.safe_load((tmp_path / "schema.yml").read_text())
    assert actual == {"version": 2, "models": [{"name": "my_other_model"}]}


def test_save_yaml_default_format(tmp_path: Path):
    storage = DiskStorage(tmp_path, yaml_format=None, read_only=False)

    storage.save_yaml(
        {
            Path("schema.yml"): {
                "version": 2,
                "models": [
                    {
                        # prevent one-line formatting ------------
                        "name": "my_other_model",
                        "columns": [{"name": "id"}],
                    }
                ],
            }
        }
    )

    actual = (tmp_path / "schema.yml").read_text()
    expected = textwrap.dedent("""\
        version: 2
        models:
        - name: my_other_model
          columns:
          - name: id
    """)
    assert actual == expected


def test_save_yaml_format(tmp_path: Path):
    yaml_format = YamlFormat(indent=2, offset=2)
    storage = DiskStorage(tmp_path, yaml_format, read_only=False)

    storage.save_yaml(
        {
            Path("schema.yml"): {
                "version": 2,
                "models": [
                    {
                        # prevent one-line formatting ------------
                        "name": "my_other_model",
                        "columns": [{"name": "id"}],
                    }
                ],
            }
        }
    )

    actual = (tmp_path / "schema.yml").read_text()
    expected = textwrap.dedent("""\
        version: 2
        models:
          - name: my_other_model
            columns:
              - name: id
    """)
    assert actual == expected


def test_save_yaml_read_only(tmp_path: Path):
    storage = DiskStorage(tmp_path, yaml_format=None, read_only=True)
    storage.save_yaml({Path("schema.yml"): {"version": 2, "models": [{"name": "my_other_model"}]}})

    assert not (tmp_path / "schema.yml").exists()


def test_roundtrip_preserve_comments(tmp_path: Path):
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

    storage = DiskStorage(tmp_path, yaml_format=None, read_only=False)
    files = storage.load_yaml({Path("my_model.yml")})
    storage.save_yaml(files)

    actual = (tmp_path / "my_model.yml").read_text()

    # Ruamel adds newlines after comments on Windows, but not on Ubuntu
    # It should be fine as we don't re-write a file if there is nothing to change in it
    if platform.system().lower() == "windows":
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
    else:
        expected = content

    assert expected == actual


def test_roundtrip_preserve_quotes(tmp_path: Path):
    content = textwrap.dedent("""\
        version: 2
        models:
        - name: "my_model"
          description: "my very first model"
          columns:
          - name: "id"
            data_type: short
    """)

    (tmp_path / "my_model.yml").write_text(content)

    storage = DiskStorage(tmp_path, yaml_format=None, read_only=False)
    files = storage.load_yaml({Path("my_model.yml")})

    yaml = files[Path("my_model.yml")]
    assert len([m for m in yaml["models"] if m["name"] == "my_model"]) == 1
    storage.save_yaml(files)

    actual = (tmp_path / "my_model.yml").read_text()

    assert content == actual
