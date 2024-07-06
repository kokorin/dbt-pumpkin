import logging

from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams
from dbt_pumpkin.planner import BootstrapPlanner, RelocationPlanner, SynchronizationPlanner
from dbt_pumpkin.storage import DiskStorage

logger = logging.getLogger(__name__)


class Pumpkin:
    def __init__(self, project_params: ProjectParams, resource_params: ResourceParams) -> None:
        self.project_params = project_params
        self.resource_params = resource_params

    def bootstrap(self, *, dry_run: bool):
        loader = ResourceLoader(self.project_params, self.resource_params)

        logger.info("Loading resource")
        resources = loader.select_resources()

        planner = BootstrapPlanner(resources)
        plan = planner.plan()

        storage = DiskStorage(loader.locate_project_dir(), read_only=dry_run)
        logger.info("Executing actions")
        plan.execute(storage)

    def relocate(self, *, dry_run: bool):
        loader = ResourceLoader(self.project_params, self.resource_params)

        logger.info("Loading resource")
        resources = loader.select_resources()

        planner = RelocationPlanner(resources)
        plan = planner.plan()

        storage = DiskStorage(loader.locate_project_dir(), read_only=dry_run)
        logger.info("Executing actions")
        plan.execute(storage)

    def synchronize(self, *, dry_run: bool):
        loader = ResourceLoader(self.project_params, self.resource_params)

        logger.info("Loading resource")
        resources = loader.select_resources()

        logger.info("Looking up tables")
        tables = loader.lookup_tables()

        planner = SynchronizationPlanner(resources, tables)
        plan = planner.plan()

        storage = DiskStorage(loader.locate_project_dir(), read_only=dry_run)
        logger.info("Executing actions")
        plan.execute(storage)
