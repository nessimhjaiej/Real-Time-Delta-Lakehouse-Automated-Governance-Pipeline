"""Typed LangGraph state for the ask-a-question flow.

Every node returns only the keys it adds/changes; LangGraph merges them
into this state as the graph runs.
"""

from __future__ import annotations

from typing import Literal, TypedDict

from src.governance_agent.compiler import QueryIntent
from src.governance_agent.executors.base import QueryResult
from src.governance_agent.rag.retriever import RetrievedChunk
from src.governance_agent.validator import ValidationResult

Verdict = Literal["allowed", "blocked"]


class AgentState(TypedDict, total=False):
    # input
    question: str
    role: str
    user: str

    # interpret
    search_query: str

    # retrieve
    retrieved_metrics: list[RetrievedChunk]

    # compile
    structured_intent: QueryIntent | None
    compiled_sql: str | None

    # validate
    validation_result: ValidationResult | None

    # execute
    query_result: QueryResult | None

    # narrate / blocked
    narration: str | None
    citations: list[RetrievedChunk]
    verdict: Verdict
    block_reason: str | None
