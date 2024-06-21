from dbt_pumpkin.data import Resource

class Planner:
    def __init__(self,declared_resources:list[Resource], actual_resources:list[Resource]) -> None:
        self.declared_resources = declared_resources
        self.actual_resources = actual_resources

    def plan_changes(self):
        pass
