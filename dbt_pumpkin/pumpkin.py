import logging

from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams
from dbt_pumpkin.planner import ActionPlanner, BootstrapPlanner, RelocationPlanner
from dbt_pumpkin.storage import DiskStorage

logger = logging.getLogger(__name__)


class Pumpkin:
    def __init__(self, project_params: ProjectParams, resource_params: ResourceParams) -> None:
        self.project_params = project_params
        self.resource_params = resource_params

    def _plan_and_execute(self, planner: ActionPlanner, *, dry_run: bool):
        logger.info("Loading resource")
        loader = ResourceLoader(self.project_params, self.resource_params)

        logger.info("Planning actions")
        plan = planner.plan(loader.resources)

        storage = DiskStorage(loader.locate_project_dir(), read_only=dry_run)
        logger.info("Executing actions")
        plan.execute(storage)

    def bootstrap(self, *, dry_run: bool):
        self._plan_and_execute(BootstrapPlanner(), dry_run=dry_run)

    def relocate(self, *, dry_run: bool):
        self._plan_and_execute(RelocationPlanner(), dry_run=dry_run)
