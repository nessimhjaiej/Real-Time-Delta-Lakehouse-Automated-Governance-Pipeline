"""FastAPI app: POST /ask, GET /health, GET /metrics.

Role arrives via a header (no real auth yet -- design decision #9). A
blocked verdict returns 403, not 200, so clients can tell "here's your
answer" apart from "this was refused" without inspecting the body.

Heavy resources (the embedding model via retrieval, the DuckDB connection,
the LLM client) are each built lazily and cached once per process via
get_agent_graph() / the module-level singletons below -- importing this
module does not download or connect to anything. Tests replace
get_agent_graph with a fake entirely (see tests/unit/test_api.py) so they
never construct a real ChatOpenAI or touch the network.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from functools import lru_cache

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from langchain_openai import ChatOpenAI

from src.governance_agent.agent.graph import build_graph
from src.governance_agent.api.schemas import (
    AskRequest,
    AskResponse,
    HealthResponse,
    MetricInfo,
    MetricsResponse,
)
from src.governance_agent.audit import AuditLog, AuditRecord
from src.governance_agent.compiler import QueryIntent
from src.governance_agent.exceptions import UnknownRoleError
from src.governance_agent.executors.factory import get_executor
from src.governance_agent.policy import load_access_policy
from src.governance_agent.semantic.loader import load_semantic_catalog

load_dotenv()

app = FastAPI(title="Realtime Governance Engine")

DEFAULT_ROLE = "analyst"
DEFAULT_USER = "anonymous"


@lru_cache(maxsize=1)
def _catalog():
    return load_semantic_catalog()


@lru_cache(maxsize=1)
def _access_policy():
    return load_access_policy()


@lru_cache(maxsize=1)
def _executor():
    return get_executor()


@lru_cache(maxsize=1)
def _audit_log() -> AuditLog:
    return AuditLog()


@lru_cache(maxsize=1)
def _llm() -> ChatOpenAI:
    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    return ChatOpenAI(model=model)


@lru_cache(maxsize=1)
def _structured_llm():
    return _llm().bind_tools([QueryIntent], tool_choice="QueryIntent")


@lru_cache(maxsize=8)
def get_agent_graph(role: str):
    """Build (and cache) the compiled agent graph for one role. Tests
    monkeypatch this name directly to inject fakes for every dependency at
    once, rather than overriding each of llm/executor/retrievers."""
    policy = _access_policy().get_role(role)
    return build_graph(_llm(), _structured_llm(), _catalog(), policy, _executor())


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/metrics", response_model=MetricsResponse)
def metrics() -> MetricsResponse:
    catalog = _catalog()
    return MetricsResponse(
        metrics=[
            MetricInfo(name=m.name, description=m.description, table=m.table)
            for m in catalog.metrics.values()
        ],
        dimensions=[d.name for d in catalog.dimensions.values()],
    )


@app.post("/ask", response_model=AskResponse)
def ask(
    request: AskRequest,
    x_role: str = Header(default=DEFAULT_ROLE, alias="X-Role"),
    x_user: str = Header(default=DEFAULT_USER, alias="X-User"),
) -> AskResponse:
    request_id = str(uuid.uuid4())
    start = time.perf_counter()

    try:
        graph = get_agent_graph(x_role)
    except UnknownRoleError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    result = graph.invoke(
        {"question": request.question, "role": x_role, "user": x_user}
    )
    latency_ms = (time.perf_counter() - start) * 1000

    structured_intent = result.get("structured_intent")
    query_result = result.get("query_result")
    citations = result.get("citations") or result.get("retrieved_metrics") or []

    _audit_log().record(
        AuditRecord(
            timestamp=datetime.now(timezone.utc),
            user=x_user,
            role=x_role,
            question=request.question,
            retrieved_chunk_ids=[c.source for c in citations],
            structured_intent=(
                structured_intent.model_dump() if structured_intent else None
            ),
            compiled_sql=result.get("compiled_sql"),
            verdict=result["verdict"],
            block_reason=result.get("block_reason"),
            row_count=query_result.row_count if query_result else None,
            latency_ms=latency_ms,
        )
    )

    response = AskResponse(
        request_id=request_id,
        verdict=result["verdict"],
        answer=result.get("narration"),
        columns=query_result.columns if query_result else [],
        rows=query_result.rows if query_result else [],
        sql=result.get("compiled_sql"),
        metric_used=structured_intent.metric if structured_intent else None,
        citations=[c.source for c in citations],
        block_reason=result.get("block_reason"),
    )

    if result["verdict"] == "blocked":
        raise HTTPException(status_code=403, detail=response.model_dump())

    return response
