import logging
from typing import Callable

from dbt_pumpkin.loader import ResourceLoader
from dbt_pumpkin.params import ProjectParams, ResourceParams
from dbt_pumpkin.plan import ExecutionMode
from dbt_pumpkin.planner import ActionPlanner, BootstrapPlanner, RelocationPlanner, SynchronizationPlanner
from dbt_pumpkin.storage import DiskStorage

logger = logging.getLogger(__name__)


class Pumpkin:
    def __init__(self, project_params: ProjectParams, resource_params: ResourceParams) -> None:
        self.project_params = project_params
        self.resource_params = resource_params

    def _execute(self, create_planner: Callable[[ResourceLoader], ActionPlanner], *, dry_run: bool):
        loader = ResourceLoader(self.project_params, self.resource_params)

        logger.debug("Creating action planner")
        planner = create_planner(loader)
        plan = planner.plan()

        storage = DiskStorage(loader.locate_project_dir(), loader.detect_yaml_format())
        mode = ExecutionMode.DRY_RUN if dry_run else ExecutionMode.RUN

        logger.info("Plan execution mode: %s", mode)
        plan.execute(storage, mode)

    def bootstrap(self, *, dry_run: bool):
        def create_planner(loader: ResourceLoader) -> ActionPlanner:
            resources = loader.select_resources()
            return BootstrapPlanner(resources)

        self._execute(create_planner, dry_run=dry_run)

    def relocate(self, *, dry_run: bool):
        def create_planner(loader: ResourceLoader) -> ActionPlanner:
            resources = loader.select_resources()
            return RelocationPlanner(resources)

        self._execute(create_planner, dry_run=dry_run)

    def synchronize(self, *, dry_run: bool):
        def create_planner(loader: ResourceLoader) -> ActionPlanner:
            resources = loader.select_resources()
            tables = loader.lookup_tables()
            case_folding = loader.detect_case_folding()
            return SynchronizationPlanner(resources, tables, case_folding)

        self._execute(create_planner, dry_run=dry_run)
