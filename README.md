# DBT-PUMPKIN

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
# test accross different python & dbt versions
hatch test --all
```
