from __future__ import annotations

from dataclasses import dataclass

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.cli.resolvers import default_project_dir
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SeedNode, SnapshotNode, SourceDefinition

try:
    from dbt_common.events.base_types import EventLevel, EventMsg
except ImportError:
    from dbt.events.base_types import EventLevel, EventMsg

try:
    from dbt.artifacts.resources.v1.components import ColumnInfo
except ImportError:
    from dbt.contracts.graph.nodes import ColumnInfo

try:
    from dbt_common.events.functions import fire_event
except ImportError:
    from dbt.events.functions import fire_event


@dataclass
class MonkeyPatch:
    obj: object
    name: str
    value: any


def prepare_monkey_patches() -> list[MonkeyPatch]:
    result: list[MonkeyPatch] = []

    # PATCH EventManager to not add DBT internal loggers

    def event_manager_add_logger(self, *args) -> None:  # noqa: ARG001
        pass

    event_manager_obj: object
    try:
        import dbt_common.events.event_manager

        event_manager_obj = dbt_common.events.event_manager.EventManager
    except ImportError:
        import dbt.events.eventmgr

        event_manager_obj = dbt.events.eventmgr.EventManager

    result.append(MonkeyPatch(event_manager_obj, "add_logger", event_manager_add_logger))

    # PATH List task not to print results using print method

    import dbt.events.types
    import dbt.task.list

    def list_task_output_results(self, results):
        """
        Original method uses print() method and hence isn't extensible
        """

        for result in results:
            self.node_results.append(result)
            fire_event(dbt.task.list.ListCmdOut(msg=result), level=EventLevel.DEBUG)
        return self.node_results

    result.append(MonkeyPatch(dbt.task.list.ListTask, "output_results", list_task_output_results))

    return result


def hijack_dbt_logs():
    for patch in prepare_monkey_patches():
        patch.obj.__setattr__(patch.name, patch.value)


__all__ = [
    "dbtRunner",
    "dbtRunnerResult",
    "default_project_dir",
    "Manifest",
    "SourceDefinition",
    "ModelNode",
    "SnapshotNode",
    "SeedNode",
    "EventMsg",
    "ColumnInfo",
    "hijack_dbt_logs",
]
