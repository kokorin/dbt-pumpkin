from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp

from ruamel.yaml import YAML


@dataclass
class Project:
    project_yml: dict[str, any]
    project_files: dict[str, any]
    profiles_yml: dict[str, any] | None = None
    local_packages: list[Project] | None = None


_yaml = YAML(typ="safe")


def _do_create_project(root: Path, project: Project):
    project_yaml = {"packages-install-path": str(root / "dbt_packages"), **project.project_yml.copy()}
    _yaml.dump(project_yaml, root / "dbt_project.yml")

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

            _do_create_project(package_root, package)

            packages_yml.setdefault("packages", []).append({"local": str(package_root)})

        _yaml.dump(packages_yml, root / "packages.yml")
        # DBT 1.5 can't install local deps on Windows, we just copy packages
        # Besides that DBT 1.8 and earlier changes CWD when executing `dbt deps`
        # # https://github.com/dbt-labs/dbt-core/issues/8997
        # so copying file tree is the easiest fix

        shutil.copytree(root / "sub_packages", root / "dbt_packages")


def mock_project(project: Project) -> Path:
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

    _do_create_project(project_dir, project)

    _yaml.dump(default_profiles, project_dir / "profiles.yml")

    return project_dir
