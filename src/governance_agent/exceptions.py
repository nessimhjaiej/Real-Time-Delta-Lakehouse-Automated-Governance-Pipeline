"""Errors shared across the governance agent's stages."""

from __future__ import annotations


class GovernanceAgentError(Exception):
    """Base class for all governance-agent errors."""


class UnknownMetricError(GovernanceAgentError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Unknown metric: {name!r}")
        self.name = name


class UnknownDimensionError(GovernanceAgentError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Unknown dimension: {name!r}")
        self.name = name


class DuplicateMetricError(GovernanceAgentError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Duplicate metric name across semantic/metrics/*.yml: {name!r}"
        )
        self.name = name


class DuplicateDimensionError(GovernanceAgentError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Duplicate dimension name across semantic/dimensions/*.yml: {name!r}"
        )
        self.name = name


class NoJoinPathError(GovernanceAgentError):
    def __init__(self, left_table: str, right_table: str) -> None:
        super().__init__(
            f"No join defined between {left_table!r} and {right_table!r} in semantic/joins.yml"
        )
        self.left_table = left_table
        self.right_table = right_table


class InvalidFilterError(GovernanceAgentError):
    """Raised when a QueryIntent filter is malformed (e.g. IN with a non-list value)."""


class UnknownRoleError(GovernanceAgentError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Unknown role: {name!r}")
        self.name = name
