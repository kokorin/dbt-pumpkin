from pathlib import Path

from dbt_pumpkin.data import Resource
from dbt_pumpkin.plan import Plan


class RelocationPlanner:
    def __init__(self, project_dir: Path, resources: list[Resource]):
        self.project_dir = project_dir
        self.resources = resources

    def plan_relocations(self) -> Plan:
        pass
