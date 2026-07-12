# DBT-PUMPKIN

[![PyPI Version](https://img.shields.io/pypi/v/dbt-pumpkin)](https://pypi.org/project/dbt-pumpkin/)

[![codecov](https://codecov.io/github/kokorin/dbt-pumpkin/graph/badge.svg?token=EKGRIWEIMZ)](https://codecov.io/github/kokorin/dbt-pumpkin)

`dbt-pumpkin` is a command-line tool which helps to manage [DBT](https://docs.getdbt.com/docs/introduction) projects.

Inspired by [dbt-osmosis](https://z3z1ma.github.io/dbt-osmosis/)

## Usage

All commands accept `--project-dir`, `--profiles-dir`, `--target`, `--profile`, `--select`, `--exclude`, `--dry-run`
and `--debug` options (same semantics as `dbt`).

### Bootstrap

Creates missing YAML schema files for resources that have [`dbt-pumpkin-path`](#dbt-pumpkin-path) configured.
Generated files will be empty — use [synchronize](#synchronize) to populate columns.

```sh
dbt-pumpkin bootstrap [OPTIONS]
```

### Relocate

Moves YAML schema files to the location defined by [`dbt-pumpkin-path`](#dbt-pumpkin-path). Can split or merge YAML
files depending on configuration.

```sh
dbt-pumpkin relocate [OPTIONS]
```

### Synchronize

Updates YAML schema files to match actual table/view columns in the database — same columns, same types, same order.
Columns absent from the database are removed; descriptions, tests and other properties are preserved.

```sh
dbt-pumpkin synchronize [OPTIONS]
```

## Configuration

`dbt-pumpkin` properties can be set either as top-level config properties (using the `+` prefix) or via the `meta` config property:

```yaml
models:
  "<YOUR_PROJECT_NAME>":
    +meta:
      dbt-pumpkin-path: _schema.yml
      dbt-pumpkin-types:
        numeric-precision-and-scale: true
        string-length: true
```

### `dbt-pumpkin-path`

`dbt-pumpkin-path` sets a path to the YAML schema file for a resource. Paths are relative to the resource file (SQL,
CSV or PY), or root-relative (starting with `/`, relative to the dbt project root).

The path is a **template** supporting `{name}` (resource name) and `{parent}` (parent folder name) placeholders.

```yaml
models:
  "<YOUR_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema.yml        # single shared file next to each model
    staging:
      +dbt-pumpkin-path: _{name}.yml      # one file per model
sources:
  "<YOUR_PROJECT_NAME>":
    # sources have no SQL file, so root-relative path is required
    +dbt-pumpkin-path: /models/staging/_source_{name}.yml
```

### `dbt-pumpkin-types`

`dbt-pumpkin-types` controls type verbosity during synchronize:

- `numeric-precision-and-scale` — include precision and scale for numeric types (e.g. `NUMBER(38,0)`)
- `string-length` — include length for string types (e.g. `character varying(256)`)
- `max-string-length` — when used with `string-length: true`, caps the length at this value

```yaml
models:
  "<YOUR_PROJECT_NAME>":
    +dbt-pumpkin-types:
      numeric-precision-and-scale: true
      string-length: true
      max-string-length: 256
```

Applies to models, seeds, snapshots and sources.

### YAML Format

You can configure how `dbt-pumpkin` formats YAML files. For that it's required to add specific DBT variable to your
project:

```yaml
vars:
  dbt-pumpkin:
    yaml_format:
      # indent of properties in a map, default 2
      indent: 2
      # offset of items in a list, default 0
      offset: 2
      # whether to preserve original quotes, default false
      preserve_quotes: true
      # maximum line width, default 80
      max_width: 120
```

## Development

```sh
# First install Hatch globally
pip install hatch

# Configure Hatch to create venvs in project
hatch config set dirs.env.virtual .hatch

# test in one venv
hatch test
# test across different python & dbt versions
hatch test --all

# sometimes working DBT project is required to verify user experience
hatch run scripts/generate.py --help
# to generate DBT project with 100 models
hatch run scripts/generate.py 100
hatch run dbt build

# to validate dbt-pumpkin output visually (on test project generated above)
hatch run dbt-pumpkin bootstrap --dry-run
hatch run dbt-pumpkin synchronize
```

## Troubleshooting

Clean envs and caches:

```sh
hatch env prune
pip cache purge
# UV is used by default for hatch-test environments
uv cache clean
```
