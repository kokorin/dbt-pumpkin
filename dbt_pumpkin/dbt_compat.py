from __future__ import annotations

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


def hijack_dbt_logs():
    try:
        from dbt_common.events.event_manager import EventManager
        from dbt_common.events.functions import fire_event
    except ImportError:
        from dbt.events.eventmgr import EventManager
        from dbt.events.functions import fire_event

    from dbt.events.types import ListCmdOut
    from dbt.task.list import ListTask

    def event_manager_add_logger(self, *args) -> None:  # noqa: ARG001
        pass

    def list_task_output_results(self, results):
        """
        Original method uses print() method and hence isn't extensible
        """

        for result in results:
            self.node_results.append(result)
            fire_event(ListCmdOut(msg=result), level=EventLevel.DEBUG)
        return self.node_results

    EventManager.add_logger = event_manager_add_logger
    ListTask.output_results = list_task_output_results


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
