"""LangGraph flow: interpret -> retrieve -> compile -> validate -> execute
-> narrate, with conditional edges to a blocked node (design decision #7).

Model wiring is intentionally split in two:
  - ``llm``: used plainly for interpret (query rewriting) and narrate (short
    text from already-computed rows). Just .invoke(...) and read .content.
  - ``structured_llm``: used in compile to pick a QueryIntent (or decline
    with CannotAnswer). Just .invoke(...) and read .tool_calls -- binding
    (``bind_intent_tools(llm)``) happens once, outside the nodes, when the
    caller constructs the model. That keeps node code identical whether
    ``structured_llm`` is a real bound ChatOpenAI or a test double that
    returns a canned AIMessage with .tool_calls already set -- no model needs
    to actually implement .bind_tools() for tests to work (langchain's fake
    chat models don't).

The model never produces the numbers: execute() runs the compiled,
validated SQL, and narrate only describes query_result's rows.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage
from pydantic import ValidationError

from src.governance_agent.agent.state import AgentState
from src.governance_agent.compiler import CannotAnswer, QueryIntent, compile_query
from src.governance_agent.exceptions import GovernanceAgentError
from src.governance_agent.executors.base import QueryExecutor
from src.governance_agent.policy import RolePolicy
from src.governance_agent.rag.retriever import (
    RetrievedChunk,
    discover_metric,
    explain_block,
)
from src.governance_agent.semantic.models import SemanticCatalog
from src.governance_agent.validator import validate_query

MetricRetriever = Callable[[str], list[RetrievedChunk]]
CitationRetriever = Callable[[str], list[RetrievedChunk]]


def bind_intent_tools(llm: BaseChatModel):
    """Bind the two tools the compile step may call; one call is required.

    Returns the bound runnable (left unannotated like the node factories).
    """
    return llm.bind_tools([QueryIntent, CannotAnswer], tool_choice="required")


def _interpret_node(llm: BaseChatModel):
    def node(state: AgentState) -> dict[str, Any]:
        response = llm.invoke(
            [
                HumanMessage(
                    "Rewrite this question as a short search query for finding "
                    "the business metric it's asking about. Return only the "
                    f"rewritten query, nothing else.\n\nQuestion: {state['question']}"
                )
            ]
        )
        content = str(response.content).strip()
        return {"search_query": content or state["question"]}

    return node


def _retrieve_node(
    metric_retriever: MetricRetriever,
):
    def node(state: AgentState) -> dict[str, Any]:
        return {"retrieved_metrics": metric_retriever(state["search_query"])}

    return node


def _compile_node(structured_llm: BaseChatModel, catalog: SemanticCatalog):
    def node(state: AgentState) -> dict[str, Any]:
        candidates = state.get("retrieved_metrics", [])
        candidate_text = (
            "\n".join(f"- {c.text}" for c in candidates)
            if candidates
            else "(no candidates found)"
        )
        dimension_text = "\n".join(
            f"- {d.name}: {d.description}" for d in catalog.dimensions.values()
        )
        prompt = (
            f"Question: {state['question']}\n\n"
            f"Candidate metrics (pick one of these by name):\n{candidate_text}\n\n"
            f"Available dimensions (pick zero or more by name):\n{dimension_text}\n\n"
            "For ranking questions (top, best, highest, lowest, most, least) set "
            "order_by to the metric name (direction desc for top/highest, asc for "
            "lowest) and set limit to N; order_by may also name a chosen dimension.\n\n"
            "If the question needs something no candidate metric or dimension "
            "provides (for example a customer's name, email or IP, an individual "
            "customer, or a metric that isn't listed), do NOT pick the closest "
            "metric: call the CannotAnswer tool and say what is missing. "
            "Otherwise call the QueryIntent tool with your choice."
        )
        response = structured_llm.invoke([HumanMessage(prompt)])

        if not response.tool_calls:
            return {
                "verdict": "blocked",
                "block_reason": "Could not interpret the question into a known metric.",
            }

        tool_call = response.tool_calls[0]
        if tool_call["name"] == CannotAnswer.__name__:
            reason = str(tool_call["args"].get("reason", "")).strip()
            return {
                "verdict": "blocked",
                "block_reason": (
                    "This question cannot be answered with the governed metrics "
                    f"and dimensions: {reason}".rstrip(": ")
                ),
            }

        try:
            intent = QueryIntent(**tool_call["args"])
            compiled = compile_query(intent, catalog)
            sql = compiled.sql(dialect="duckdb")
        except (GovernanceAgentError, ValidationError) as error:
            return {
                "verdict": "blocked",
                "block_reason": f"Could not compile a valid query: {error}",
            }

        return {"structured_intent": intent, "compiled_sql": sql}

    return node


def _route_after_compile(state: AgentState) -> str:
    return "blocked" if state.get("verdict") == "blocked" else "validate"


def _validate_node(policy: RolePolicy):
    def node(state: AgentState) -> dict[str, Any]:
        sql = state["compiled_sql"]
        assert sql is not None  # only reached when compile succeeded
        result = validate_query(sql, policy)
        if not result.allowed:
            return {
                "validation_result": result,
                "verdict": "blocked",
                "block_reason": result.reason,
            }
        return {"validation_result": result}

    return node


def _route_after_validate(state: AgentState) -> str:
    return "blocked" if state.get("verdict") == "blocked" else "execute"


def _execute_node(executor: QueryExecutor):
    def node(state: AgentState) -> dict[str, Any]:
        sql = state["compiled_sql"]
        assert sql is not None  # only reached when validation passed
        result = executor.execute(sql)
        return {"query_result": result}

    return node


def _narrate_node(llm: BaseChatModel):
    def node(state: AgentState) -> dict[str, Any]:
        query_result = state["query_result"]
        assert query_result is not None
        rows_preview = [
            dict(zip(query_result.columns, row, strict=True))
            for row in query_result.rows[:20]
        ]
        prompt = (
            f"Question: {state['question']}\n\n"
            f"Result columns: {query_result.columns}\n"
            f"Result rows: {rows_preview}\n\n"
            "Write a short (1-3 sentence) plain-language explanation of this "
            "result. The numbers above are already final and correct -- refer "
            "to them, don't recompute or restate different ones."
        )
        response = llm.invoke([HumanMessage(prompt)])
        return {"narration": str(response.content), "verdict": "allowed"}

    return node


def _blocked_node(
    citation_retriever: CitationRetriever,
):
    def node(state: AgentState) -> dict[str, Any]:
        reason = state.get("block_reason") or "This request could not be completed."
        citations = citation_retriever(reason)
        return {"citations": citations, "verdict": "blocked", "block_reason": reason}

    return node


def build_graph(
    llm: BaseChatModel,
    structured_llm: BaseChatModel,
    catalog: SemanticCatalog,
    policy: RolePolicy,
    executor: QueryExecutor,
    metric_retriever: MetricRetriever = discover_metric,
    citation_retriever: CitationRetriever = explain_block,
):
    """Compile the LangGraph agent. Returns a LangGraph CompiledStateGraph
    (left untyped here since langgraph's own return type is internal)."""
    from langgraph.graph import END, StateGraph

    builder = StateGraph(AgentState)
    builder.add_node("interpret", _interpret_node(llm))
    builder.add_node("retrieve", _retrieve_node(metric_retriever))
    builder.add_node("compile", _compile_node(structured_llm, catalog))
    builder.add_node("validate", _validate_node(policy))
    builder.add_node("execute", _execute_node(executor))
    builder.add_node("narrate", _narrate_node(llm))
    builder.add_node("blocked", _blocked_node(citation_retriever))

    builder.set_entry_point("interpret")
    builder.add_edge("interpret", "retrieve")
    builder.add_edge("retrieve", "compile")
    builder.add_conditional_edges(
        "compile", _route_after_compile, {"blocked": "blocked", "validate": "validate"}
    )
    builder.add_conditional_edges(
        "validate", _route_after_validate, {"blocked": "blocked", "execute": "execute"}
    )
    builder.add_edge("execute", "narrate")
    builder.add_edge("narrate", END)
    builder.add_edge("blocked", END)

    return builder.compile()
