"""Load the real Airflow DAG objects and dump how every task is wired.

    python -m toolkit.pages._quality_dag <out.json>

Spawned by toolkit/pages/quality.py in its own process (with a throwaway
AIRFLOW_HOME) so Airflow's import-time setup never touches the parent build.
Values are read from the instantiated operators, i.e. after default_args are
applied -- the same objects the scheduler would run.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any

from toolkit.common import REPO_ROOT


def _qualified(func: Any) -> str | None:
    if func is None:
        return None
    if isinstance(func, (list, tuple)):
        names = [_qualified(f) for f in func]
        return ", ".join(n for n in names if n)
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None) or repr(func)
    return f"{module}.{qualname}" if module else qualname


def _dataset_uris(items: Any) -> list[str]:
    return [str(getattr(item, "uri", item)) for item in (items or [])]


def run(out_path: Path) -> None:
    from airflow.models.dag import DAG

    dags: list[dict[str, Any]] = []
    for file in sorted((REPO_ROOT / "dags").glob("*.py")):
        module = importlib.import_module(f"dags.{file.stem}")
        for value in vars(module).values():
            if not isinstance(value, DAG):
                continue
            tasks = []
            for task in value.tasks:
                retry_delay = getattr(task, "retry_delay", None)
                tasks.append(
                    {
                        "task_id": task.task_id,
                        "operator": type(task).__name__,
                        "callable": _qualified(getattr(task, "python_callable", None)),
                        "retries": task.retries,
                        "retry_delay_s": (
                            retry_delay.total_seconds()
                            if retry_delay is not None
                            else None
                        ),
                        "trigger_rule": (
                            str(getattr(task.trigger_rule, "value", None))
                            if hasattr(task.trigger_rule, "value")
                            else str(task.trigger_rule)
                        ),
                        "on_failure_callback": _qualified(task.on_failure_callback),
                        "upstream": sorted(task.upstream_task_ids),
                        "downstream": sorted(task.downstream_task_ids),
                        "inlets": _dataset_uris(task.inlets),
                        "outlets": _dataset_uris(task.outlets),
                    }
                )
            dags.append(
                {
                    "dag_id": value.dag_id,
                    "file": f"dags/{file.name}",
                    "schedule": str(getattr(value, "schedule_interval", None)),
                    "tasks": tasks,
                }
            )
    import airflow

    out_path.write_text(
        json.dumps({"airflow_version": airflow.__version__, "dags": dags}),
        encoding="utf-8",
    )


if __name__ == "__main__":
    run(Path(sys.argv[1]))
