from __future__ import annotations

import shutil
import textwrap
from pathlib import Path
from tempfile import mkdtemp

import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult


def mock_project(files: dict[str, str], local_packages: dict[str, dict[str, str]] = None, build=False) -> Path:
    project_dir = Path(mkdtemp(prefix="test_pumpkin_"))

    project_yml = yaml.safe_load(textwrap.dedent(files.pop("dbt_project.yml")))

    profiles_yml = {
        project_yml["profile"]: {
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

    if local_packages:
        packages_install_dir = project_dir / "dbt_packages"
        project_yml["packages-install-path"] = str(packages_install_dir)

        packages_dir = project_dir / "local_packages"

        packages_yml = {"packages": (packages := [])}

        for package_name, package_files in local_packages.items():
            for path_str, content in package_files.items():
                package_dir = packages_dir / package_name
                package_dir.mkdir(parents=True, exist_ok=True)

                (package_dir / path_str).write_text(content)

                packages.append({"local": str(package_dir)})

        (project_dir / "packages.yml").write_text(yaml.dump(packages_yml))

        # DBT 1.5 can't install local deps on Windows, we just copy packages
        # Besides that DBT 1.8 and earlier changes CWD when executing `dbt deps`
        # # https://github.com/dbt-labs/dbt-core/issues/8997
        # so copying file tree is the easiest fix
        shutil.copytree(packages_dir, packages_install_dir)

    (project_dir / "dbt_project.yml").write_text(yaml.safe_dump(project_yml))
    (project_dir / "profiles.yml").write_text(yaml.safe_dump(profiles_yml))

    for path_str, content in files.items():
        path = project_dir / path_str
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    if build:
        args = ["build", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]
        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            msg = f"Mock project build failed. Exception: {res.exception}"
            raise Exception(msg)  # noqa: TRY002

    return project_dir
