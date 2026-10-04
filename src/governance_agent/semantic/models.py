"""Pydantic models for the semantic layer: metrics, dimensions, and joins.

These are hand-authored in semantic/*.yml by a developer, not produced by
the LLM -- the LLM only ever *selects* a metric/dimension name that already
exists here (src/governance_agent/compiler.py). That's what keeps SQL
generation deterministic.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from src.governance_agent.exceptions import UnknownDimensionError, UnknownMetricError

JoinType = Literal["inner", "left"]


class Metric(BaseModel):
    """An aggregate expression over a single base table."""

    name: str
    description: str
    table: str
    expression: str
    time_column: str | None = None


class Dimension(BaseModel):
    """A groupable/filterable column or expression on a single table."""

    name: str
    description: str
    table: str
    column: str


class Join(BaseModel):
    """A join between a metric's base table and a dimension's table."""

    left_table: str
    left_key: str
    right_table: str
    right_key: str
    join_type: JoinType = "left"


class SemanticCatalog(BaseModel):
    """The full loaded semantic layer, with name-based lookups."""

    metrics: dict[str, Metric]
    dimensions: dict[str, Dimension]
    joins: list[Join]

    def get_metric(self, name: str) -> Metric:
        try:
            return self.metrics[name]
        except KeyError:
            raise UnknownMetricError(name) from None

    def get_dimension(self, name: str) -> Dimension:
        try:
            return self.dimensions[name]
        except KeyError:
            raise UnknownDimensionError(name) from None

    def find_join(self, left_table: str, right_table: str) -> Join | None:
        """Return the join connecting two tables, in either direction."""
        for join in self.joins:
            if join.left_table == left_table and join.right_table == right_table:
                return join
            if join.left_table == right_table and join.right_table == left_table:
                return join
        return None
