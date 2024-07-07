# DBT-PUMPKIN

`dbt-pumpkin` is a command-line tool which helps to manage [DBT](https://docs.getdbt.com/docs/introduction) projects.

Inspired by [dbt-osmosis](https://z3z1ma.github.io/dbt-osmosis/)

## Usage

### Botstrap DBT Resources

`dbt-pumpkin` allows to create DBT YAML schema files for Seeds, Models and Snapshots. For that one has to add
[dbt-pumpking-path](#dbt-pumpkin-path) configuration property for a Resource. Then `dbt-pumpkin` will be able to
understand where YAML files should be located.

By default `dbt-pumpkin` will analyze all project's resources and create **absent** YAML schema definition files. You
can use `--select` and `--exclude` options to select/exclude a subset of resources. These arguments work exactly the
same way as they do with `dbt` command (`dbt list` is used under the hood).

*Note*: YAML files created by this command will be almost empty. Another command should be used to add
columns: [synchronize](#synchronize-dbt-resources)

```sh
dbt-pumpkin bootstrap --help
Usage: dbt-pumpkin bootstrap [OPTIONS]

  Bootstraps project by adding missing YAML definitions

Options:
  --project-dir TEXT
  --profiles-dir TEXT
  -t, --target TEXT
  --profile TEXT
  -s, --select TEXT
  --exclude TEXT
  --dry-run
  --debug
  --help               Show this message and exit.
```

### Relocate DBT Resources

`dbt-pumpkin` also allows to move DBT YAML schema files for Sources, Seeds, Models and Snapshots. As
with [bootstrap](#botstrap-dbt-resources) command [dbt-pumpking-path](#dbt-pumpkin-path) configuration property is
required for a Resource. Then `dbt-pumpkin` will be able to understand where YAML files should be located.

*Note*: `dbt-pumking` can split YAML schema file (if it contains several Resources) into separate YAML file, or,
alternatively, it can merge several YAML files into one. This is all configured by `dbt-pumpking-path`.

```sh
dbt-pumpkin relocate --help
Usage: dbt-pumpkin relocate [OPTIONS]

  Relocates YAML definitions according to dbt-pumpkin-path configuration

Options:
  --project-dir TEXT
  --profiles-dir TEXT
  -t, --target TEXT
  --profile TEXT
  -s, --select TEXT
  --exclude TEXT
  --dry-run
  --debug
  --help               Show this message and exit.
```

### Synchronize DBT Resources

Last, but not least, feature is `synchronize`. It analyzes schema of actual tables and views in your database and
updates YAML schema files to have the same columns (and column types) in the same order as they are in a table/view
corresponding to a Resource.

Pay attention, that columns declared in YAML but absent in the database will be deleted. But you are using version
control system (and most probably it's git), right? So you can always revert changes done by `dbt-pumpkin` if, for
example, you forgot to run/build a Resource and its table/view is outdated.

*Note* `dbt-pumpkin` leaves intact any descriptions, tests and any other properties set in YAML file.

```sh
dbt-pumpkin synchronize --help
Usage: dbt-pumpkin synchronize [OPTIONS]

  Synchronizes YAML definitions with actual tables in DB

Options:
  --project-dir TEXT
  --profiles-dir TEXT
  -t, --target TEXT
  --profile TEXT
  -s, --select TEXT
  --exclude TEXT
  --dry-run
  --debug
  --help               Show this message and exit.
```

## Configuration

### `dbt-pumpkin-path`

`dbt-pumpkin-path` is the only configuration property supported for now.

It sets a path to YAML schema file of a resource. The path can relative to resource (SQL, CSV or PY) or root-relative.
Root-relative paths set path relative to DBT project root directory and start with `/` symbol.

#### Examples

Configure `dbt-pumpkin` to bootstrap resources at (or relocate resources to) `_schema.yml` file located **next to**
SQL, CSV or PY file.

```yaml
models:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema.yml
seeds:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema.yml
snapshots:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema.yml
```

You your Model is called `my_model` and is defined at `models/path/to/my_model.sql`, then `dbt-pumpkin` will consider
`models/path/to/_schema.yml` as the path were the model should be bootstrapped at or relocated to.

Of course, as with any other DBT configuration property, you can re-define `dbt-pumpkin-path` on resources you want:

```yaml
models:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema.yml
    intermediate:
      int_my_model:
        +dbt-pumpkin-path: int_my_model.yml
```

`dbt-pumpkin-path` actually defines **template** path and supports `{name}` and `{parent}` values. `{name}` gets
replaced with Resource's name and `{parent}` - with folder's name where resource SQL, CSV or PY file is located.

```yaml
models:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _{name}.yml
```

With the above configuration, every resource will be relocated to (bootstrapped at) separate YAML file, so `my_model`
will be defined at `_my_model.yml`.

In case you have too many resourced defined in a directory, you may specify subdirectory to hold YAML files:

```yaml
models:
  "<YOU_PROJECT_NAME>":
    +dbt-pumpkin-path: _schema/{name}.yml
```

Source in DBT have no other files except YAML. It means that it's not possible to use relative path
in `dbt-pumpkin-path` to configure source YAML file location. The only option is to use root-relative paths:


```yaml
sources:
  "<YOU_PROJECT_NAME>":
    # root-relative path must start with /
    +dbt-pumpkin-path: /models/staging/_source_{name}.yml
```

## Development

```sh
# First install Hatch globally
pip install hatch

# Configure Hatch to create venvs in project
hatch config set dirs.env.virtual .hatch

# Before testing we need to build DBT project
hatch run dbt clean
hatch run dbt seed
hatch run dbt build

# test in one venv
hatch test
# test across different python & dbt versions
hatch test --all
```

## Troubleshooting

Clean envs and caches:

```sh
hatch env prune
pip cache purge
# UV is used by default for hatch-test environments
uv cache clean
```
