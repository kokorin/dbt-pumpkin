# DBT-PUMPKIN

## Development

```sh
# First install Hatch globally
pip install hatch

# Configure Hatch to create venvs in project
hatch config set dirs.env.virtual .hatch

# test in one venv
hatch test
# test accross different python & dbt versions
hatch test --all
```
