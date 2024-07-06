import logging

import click

from dbt_pumpkin.dbt_compat import hijack_dbt_logs
from dbt_pumpkin.params import ProjectParams, ResourceParams
from dbt_pumpkin.pumpkin import Pumpkin


class P:
    project_dir = click.option("--project-dir")
    profiles_dir = click.option("--profiles-dir")
    target = click.option("--target", "-t")
    profile = click.option("--profile")
    select = click.option("--select", "-s", multiple=True)
    exclude = click.option("--exclude", multiple=True)
    dry_run = click.option("--dry-run", is_flag=True, default=False)
    debug = click.option("--debug", is_flag=True, default=False)


def set_up_logging(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level)
    hijack_dbt_logs()


@click.group
def cli():
    pass


@cli.command
@P.project_dir
@P.profiles_dir
@P.target
@P.profile
@P.select
@P.exclude
@P.dry_run
@P.debug
def bootstrap(project_dir, profiles_dir, target, profile, select, exclude, dry_run, debug):
    """
    Bootstraps project by adding missing YAML definitions
    """
    set_up_logging(debug)

    project_params = ProjectParams(project_dir=project_dir, profiles_dir=profiles_dir, target=target, profile=profile)
    resource_params = ResourceParams(select=select, exclude=exclude)
    pumpkin = Pumpkin(project_params, resource_params)
    pumpkin.bootstrap(dry_run=dry_run)


@cli.command
@P.project_dir
@P.profiles_dir
@P.target
@P.profile
@P.select
@P.exclude
@P.dry_run
@P.debug
def relocate(project_dir, profiles_dir, target, profile, select, exclude, dry_run, debug):
    """
    Relocates YAML definitions according to dbt-pumpkin-path configuration
    """
    set_up_logging(debug)

    project_params = ProjectParams(project_dir=project_dir, profiles_dir=profiles_dir, target=target, profile=profile)
    resource_params = ResourceParams(select=select, exclude=exclude)
    pumpkin = Pumpkin(project_params, resource_params)
    pumpkin.relocate(dry_run=dry_run)

@cli.command
@P.project_dir
@P.profiles_dir
@P.target
@P.profile
@P.select
@P.exclude
@P.dry_run
@P.debug
def synchronize(project_dir, profiles_dir, target, profile, select, exclude, dry_run, debug):
    """
    Synchronizes YAML definitions with actual tables in DB
    """
    set_up_logging(debug)

    project_params = ProjectParams(project_dir=project_dir, profiles_dir=profiles_dir, target=target, profile=profile)
    resource_params = ResourceParams(select=select, exclude=exclude)
    pumpkin = Pumpkin(project_params, resource_params)
    pumpkin.synchronize(dry_run=dry_run)


def main():
    cli()


if __name__ == "__main__":
    main()
