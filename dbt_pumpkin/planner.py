from abc import ABC, abstractmethod

from dbt_pumpkin.data import Resource, ResourceType
from dbt_pumpkin.exception import PumpkinError
from dbt_pumpkin.plan import Action, BootstrapResource, Plan, RelocateResource
from dbt_pumpkin.resolver import PathResolver


class ActionPlanner(ABC):
    @abstractmethod
    def plan(self, resources: list[Resource]) -> Plan:
        raise NotImplementedError


class BootstrapPlanner(ActionPlanner):
    def plan(self, resources: list[Resource]) -> Plan:
        actions: list[Action] = []
        path_resolver = PathResolver()

        for resource in resources:
            if resource.type == ResourceType.SOURCE:
                # sources can be initialized only manually
                continue

            if resource.yaml_path:
                # Resource already initialized
                continue

            if not resource.config or not resource.config.yaml_path_template:
                # TODO: warning
                continue

            yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            actions.append(BootstrapResource(resource.type, resource.name, yaml_path))

        return Plan(actions)


class RelocationPlanner(ActionPlanner):
    def plan(self, resources: list[Resource]) -> Plan:
        actions: list[Action] = []
        path_resolver = PathResolver()

        sources: dict[str, list[Resource]] = {}

        for resource in resources:
            if resource.type == ResourceType.SOURCE:
                # sources with the same source_name must be defined in one file
                sources.setdefault(resource.source_name, []).append(resource)
                continue

            if not resource.yaml_path:
                # No definition found, nothing to relocate
                continue

            if not resource.config or not resource.config.yaml_path_template:
                # TODO: warning
                continue

            to_yaml_path = path_resolver.resolve(resource.config.yaml_path_template, resource.name, resource.path)
            if resource.yaml_path != to_yaml_path:
                actions.append(RelocateResource(resource.type, resource.name, resource.yaml_path, to_yaml_path))

        for source_name, source_tables in sources.items():
            # make sure all source's resources have exactly the same configuration
            configs = {r.config for r in source_tables}
            if len(configs) > 1:
                # TODO: warning
                msg = f"Sources in {source_name} have different configurations: {configs}"
                raise PumpkinError(msg)

            config = configs.pop()

            if not config or not config.yaml_path_template:
                # TODO: warning
                continue

            yaml_path = source_tables[0].yaml_path
            to_yaml_path = path_resolver.resolve(config.yaml_path_template, source_name, resource_path=None)

            if yaml_path != to_yaml_path:
                actions.append(RelocateResource(ResourceType.SOURCE, source_name, yaml_path, to_yaml_path))

        return Plan(actions)
