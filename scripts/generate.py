# /// script
# requires-python = ">=3.9"
# ///

from pathlib import Path

project_dir:Path = Path(__file__,  "../../.dbt_project").resolve()

print(f"Will generate DBT project at {project_dir}")

project_dir.mkdir(parents=True, exist_ok=True)

(project_dir / "dbt_project.yml").write_text("""\
name: my_pumpkin
version: 1.0.0
profile: test_pumpkin
models:
  my_pumpkin:
    +dbt-pumpkin-path: "_schema/{name}.yml"
""")

(project_dir / "profiles.yml").write_text(f"""\
test_pumpkin:
  target: test
  outputs:
    test:
      # Comment to stop formatting in 1 line
      type: duckdb
      path: {project_dir}/test.duckdb
      threads: 8
""")

models_dir = project_dir / "models"
models_dir.mkdir(parents=True, exist_ok=True)

for i in range(1, 1000):
    model_path = models_dir / f"model_{i}.sql"
    model_path.write_text("""\
        select 1 as id
    """)

print("Generated")
