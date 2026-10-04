"""ENV-based executor selection, mirroring config/spark_config.py."""

from __future__ import annotations

import os

from src.governance_agent.executors.base import QueryExecutor
from src.governance_agent.executors.databricks_executor import DatabricksExecutor
from src.governance_agent.executors.duckdb_executor import DuckDBExecutor


def get_executor(env: str | None = None) -> QueryExecutor:
    raw_env = env if env is not None else os.getenv("EXECUTION_ENV", "local")
    resolved_env = raw_env.lower().strip()
    if resolved_env == "local":
        return DuckDBExecutor()
    if resolved_env == "databricks":
        return DatabricksExecutor()
    raise ValueError(
        f"Invalid EXECUTION_ENV={resolved_env!r}. Supported values are 'local' or 'databricks'."
    )
