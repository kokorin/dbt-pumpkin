from __future__ import annotations

from logging import Logger, getLevelName, getLogger
from uuid import uuid4

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.cli.resolvers import default_project_dir
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SeedNode, SnapshotNode, SourceDefinition

try:
    from dbt_common.events.base_types import EventMsg
except ImportError:
    from dbt.events.base_types import EventMsg

try:
    from dbt.artifacts.resources.v1.components import ColumnInfo
except ImportError:
    from dbt.contracts.graph.nodes import ColumnInfo


def hijack_dbt_logs(logger: Logger | None = None):
    logger = logger or getLogger(__name__)

    try:
        from dbt_common.events.base_types import msg_from_base_event
    except ImportError:
        from dbt.events.base_types import msg_from_base_event

    class HijackingEventManager:
        def __init__(self):
            self.loggers = []
            self.callbacks = []
            self.invocation_id = str(uuid4())

        def add_logger(self, *args) -> None:
            pass

        def add_callback(self, callback) -> None:
            self.callbacks.append(callback)

        def fire_event(self, e, level=None) -> None:
            msg = msg_from_base_event(e, level=level)

            for callback in self.callbacks:
                callback(msg)

            level = getLevelName(msg.info.level.upper())
            logger.log(level, msg.info.msg)

        def flush(self) -> None:
            pass

    try:
        import dbt_common.events.event_manager
        import dbt_common.events.event_manager_client

        dbt_common.events.event_manager.EventManager = HijackingEventManager
        dbt_common.events.event_manager_client._EVENT_MANAGER = HijackingEventManager()  # noqa: SLF001
    except ImportError:
        import dbt.events.eventmgr
        import dbt.events.functions

        dbt.events.eventmgr.EventManager = HijackingEventManager
        dbt.events.functions.EVENT_MANAGER = HijackingEventManager()


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
