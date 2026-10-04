"""Stub executor for EXECUTION_ENV=databricks.
not implemented yet , probably won't be implemented in the near future either.

"""

from __future__ import annotations

from src.governance_agent.executors.base import QueryExecutor, QueryResult


class DatabricksExecutor(QueryExecutor):
    def execute(self, sql: str) -> QueryResult:
        raise NotImplementedError(
            "DatabricksExecutor is a stub -- Databricks execution is deferred."
        )

    def close(self) -> None:
        pass
