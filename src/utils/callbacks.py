"""Airflow task callbacks shared across DAGs."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def log_quality_gate_failure(context: dict[str, Any]) -> None:
    """on_failure_callback for the quality_gate task.

    No external notifier (Slack/SMTP) is wired up yet, so this makes the
    failure impossible to miss in the Airflow UI/logs: an ERROR-level banner
    with the run id, task, and the DataQualityError's per-expectation
    summary already built by src.pipelines.quality.run_quality_gate.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")
    logger.error(
        "\n%s\nQUALITY GATE FAILED: %s.%s (run %s)\n%s\n%s",
        "=" * 70,
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
        exception,
        "=" * 70,
    )
