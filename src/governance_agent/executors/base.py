"""QueryExecutor -> finally executes the validated query.
factory pattern is used here cause it's cool and makes me look like a professional  .
DUCK DB IS IMPLENTED
Databricks is just a placeholder.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    duration_ms: float


class QueryExecutor(ABC):
    @abstractmethod
    def execute(self, sql: str) -> QueryResult:
        """Execute a single, already-validated SELECT statement."""

    @abstractmethod
    def close(self) -> None:
        """Release any held connection/resources."""
