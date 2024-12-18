# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "click",
# ]
# ///
import pathlib
import shutil
import textwrap
from pathlib import Path

import click


@click.command
@click.option('--project-dir', envvar='DBT_PROJECT_DIR', default='.dbt_project',
              type=click.Path(path_type=pathlib.Path, resolve_path=True))
@click.option('--profiles-dir', envvar='DBT_PROFILES_DIR', default='.dbt_project',
              type=click.Path(path_type=pathlib.Path, resolve_path=True))
@click.option('--keep', is_flag=True, default=False, show_default=True, help="Don't prune project dir")
@click.argument("models", default=10, type=int)
def cli(project_dir: Path, profiles_dir: Path, keep: bool, models: int):
    """
    Generates simple DBT project which can be used for manual testing of dbt-pumpkin output
    """

    print(f"Will generate DBT project at {project_dir}")

    if not keep and project_dir.exists():
        shutil.rmtree(project_dir)

    project_dir.mkdir(parents=True, exist_ok=True)

    (project_dir / "dbt_project.yml").write_text(textwrap.dedent("""\
        name: my_pumpkin
        version: 1.0.0
        profile: test_pumpkin
        models:
          my_pumpkin:
            +dbt-pumpkin-path: "_schema/{name}.yml"
    """))

    print(f"Will generate DBT profiles at {profiles_dir}")

    (profiles_dir / "profiles.yml").write_text(textwrap.dedent(f"""\
        test_pumpkin:
          target: test
          outputs:
            test:
              # Comment to stop formatting in 1 line
              type: duckdb
              path: {project_dir}/test.duckdb
              threads: 8
    """))

    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    for i in range(1, models + 1):
        model_path = models_dir / f"model_{i}.sql"
        model_path.write_text(textwrap.dedent("""\
            select 1 as id
        """))

    print("Generated")


def main():
    cli()


if __name__ == "__main__":
    main()
