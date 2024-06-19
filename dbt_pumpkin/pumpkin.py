import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition, ModelNode, SnapshotNode, SeedNode
from typing import List, Set, Dict
from functools import cached_property


class Pumpkin:

    def __init__(
        self, project_dir: str = None, profiles_dir: str = None, selects: List[str] = None, excludes: List[str] = None
    ) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.selects = selects or []
        self.excludes = excludes or []

    @cached_property
    def manifest(self) -> Manifest:
        args = ["parse"]
        if self.project_dir:
            args += ["--project-dir", self.project_dir]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]

        res: dbtRunnerResult = dbtRunner().invoke(args)

        if not res.success:
            raise res.exception

        return res.result

    @cached_property
    def selected_unique_ids(self) -> Dict[str, Set[str]]:
        """
        Returns a dictionary mapping resource type to a set of resource identifiers
        """
        args = ["list"]
        if self.project_dir:
            args += ["--project-dir", self.project_dir]
        if self.profiles_dir:
            args += ["--profiles-dir", self.profiles_dir]
        for select in self.selects:
            args += ["--select", select]
        for exclude in self.excludes:
            args += ["--exclude", exclude]
        args += ["--output", "json"]

        res: dbtRunnerResult = dbtRunner(self.manifest).invoke(args)

        if not res.success:
            raise res.exception

        result: Dict[str, Set[str]] = {}
        for raw_resource in res.result:
            resource = json.loads(raw_resource)
            resource_type = resource["resource_type"]
            if resource_type in {"seed", "model", "snapshot", "source"}:
                result.setdefault(resource_type, set()).add(resource["unique_id"])

        return result

    @cached_property
    def selected_sources(self) -> List[SourceDefinition]:
        return [self.manifest.sources[id] for id in self.selected_unique_ids.get("source", [])]

    @cached_property
    def selected_snapshots(self) -> List[SnapshotNode]:
        return [self.manifest.nodes[id] for id in self.selected_unique_ids.get("snapshot", [])]

    @cached_property
    def selected_seeds(self) -> List[SeedNode]:
        return [self.manifest.nodes[id] for id in self.selected_unique_ids.get("seed", [])]

    @cached_property
    def selected_models(self) -> List[ModelNode]:
        return [self.manifest.nodes[id] for id in self.selected_unique_ids.get("model", [])]
