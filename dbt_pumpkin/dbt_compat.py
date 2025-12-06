from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class MonkeyPatch:
    obj: object
    name: str
    value: any


def _get_dbt_patches() -> Sequence[MonkeyPatch]:
    try:
        from dbt.version import get_installed_version

        dbt_version = get_installed_version().major + "." + get_installed_version().minor
    except ImportError:
        dbt_version = "detection_failed"
    ###
    # Patches DBT 1.5 - 1.8 to suppress console output for `dbt list`.
    # Original output_results method uses print()
    ###
    list_task_obj = None

    if dbt_version == "1.8":
        import dbt.task.list
        from dbt_common.events.base_types import EventLevel
        from dbt_common.events.functions import fire_event

        list_task_obj = dbt.task.list.ListTask
    elif dbt_version in {"1.5", "1.6", "1.7"}:
        import dbt.task.list
        from dbt.events.base_types import EventLevel
        from dbt.events.functions import fire_event

        list_task_obj = dbt.task.list.ListTask

    def list_task_output_results(self, results):
        for result in results:
            self.node_results.append(result)
            fire_event(dbt.task.list.ListCmdOut(msg=result), level=EventLevel.DEBUG)
        return self.node_results

    if list_task_obj:
        yield MonkeyPatch(list_task_obj, "output_results", list_task_output_results)

    ###
    # Patches DBT 1.5 - 1.10 EventManager to not add DBT internal loggers
    ###
    def event_manager_add_logger(_self, *_args) -> None:
        pass

    event_manager_obj = None
    if dbt_version in {"1.8", "1.9", "1.10"}:
        import dbt_common.events.event_manager

        event_manager_obj = dbt_common.events.event_manager.EventManager
    elif dbt_version in {"1.5", "1.6", "1.7"}:
        import dbt.events.eventmgr

        event_manager_obj = dbt.events.eventmgr.EventManager

    if event_manager_obj:
        yield MonkeyPatch(event_manager_obj, "add_logger", event_manager_add_logger)


def suppress_dbt_cli_output():
    for patch in _get_dbt_patches():
        setattr(patch.obj, patch.name, patch.value)
