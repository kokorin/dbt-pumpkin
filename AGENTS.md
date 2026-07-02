# AGENTS.md

## Project

`dbt-pumpkin` is a CLI tool for managing dbt projects (bootstrap, relocate, synchronize YAML schema files).

- Language: Python 3.9+
- Build/test: [Hatch](https://hatch.pypa.io/)
- Entry point: `dbt_pumpkin/cli.py`

## Development

```sh
# Run tests (default env: Python 3.12, dbt 1.10)
hatch run pytest

# Run full matrix
hatch test

# Run a specific test file
hatch run pytest tests/path/to/test_file.py
```

## Guidelines

- Do not modify `pyproject.toml` matrix entries without understanding the dbt/Python compatibility constraints.
- Integration tests (`test_loader.py`, `test_dbt_compat.py`, `test_pumpkin.py`) spin up real dbt projects in temp dirs via `tests/mock.py` — do not mock dbt internals.
- Keep YAML handling through `ruamel-yaml` to preserve formatting and comments.