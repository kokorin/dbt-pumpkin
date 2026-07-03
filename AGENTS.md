# AGENTS.md

## Project

`dbt-pumpkin` is a CLI tool for managing dbt projects (bootstrap, relocate, synchronize YAML schema files).

- Language: Python 3.9+
- Build/test: [Hatch](https://hatch.pypa.io/)
- Entry point: `dbt_pumpkin/cli.py`

## Architecture

Execution pipeline: `ResourceLoader` → `*Planner` → `Plan` (list of `Action`s) → `DiskStorage`

- `loader.py` — invokes dbt (via `dbtRunner`) to list resources and look up tables; reads YAML files
- `planner.py` — compares loaded resources against desired state, produces `Action` list
- `plan.py` — `Action` subclasses modify in-memory `dict[Path, dict]`; `Storage` flushes to disk
- `storage.py` — reads/writes YAML files; `--dry-run` skips the write step
- `dbt_compat.py` — monkey-patches dbt internals to suppress output; version-specific, touch carefully
- `resolver.py` — resolves `dbt-pumpkin-path` templates to actual file paths

All configuration is set in `dbt_project.yml`. Three properties are available:

- **`+dbt-pumpkin-path`** — path template for the YAML schema file of a resource. Supports `{name}` and `{parent}` placeholders. Root-relative paths start with `/`. Required for bootstrap/relocate; sources only support root-relative paths.
- **`+dbt-pumpkin-types`** — controls type verbosity during synchronize: `numeric-precision-and-scale` and `string-length` flags.
- **`vars: dbt-pumpkin: yaml_format`** — controls YAML output formatting (`indent`, `offset`, `preserve_quotes`, `max_width`).

## Development

```sh
# Run tests (default env: Python 3.12, dbt 1.10)
hatch run pytest

# Run full matrix (Python 3.10–3.12 × dbt 1.7–1.10)
hatch test

# Lint and format checks
hatch check fmt
hatch check code

# Auto-fix lint and format issues
hatch check fmt --fix
hatch check code --fix
```

## Guidelines

- Do not modify `pyproject.toml` matrix entries without understanding the dbt/Python compatibility constraints.
- Integration tests (`test_loader.py`, `test_dbt_compat.py`, `test_pumpkin.py`) spin up real dbt projects in temp dirs via `tests/mock.py` — do not mock dbt internals.
- Keep YAML handling through `ruamel-yaml` to preserve formatting and comments.
