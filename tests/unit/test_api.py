"""FastAPI tests. get_agent_graph is monkeypatched to a fake factory for
every test, so no real ChatOpenAI, DuckDB/MinIO, or Qdrant is ever touched
-- these need no network access and no docker services running.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage

from src.governance_agent.agent.graph import build_graph
from src.governance_agent.api import main
from src.governance_agent.exceptions import UnknownRoleError
from src.governance_agent.executors.base import QueryExecutor, QueryResult
from src.governance_agent.policy import RolePolicy
from src.governance_agent.rag.retriever import RetrievedChunk
from src.governance_agent.semantic.models import (
    Dimension,
    Join,
    Metric,
    SemanticCatalog,
)


class FakeExecutor(QueryExecutor):
    def execute(self, sql: str) -> QueryResult:
        return QueryResult(
            columns=["country", "total_revenue"],
            rows=[["France", 1000.0]],
            row_count=1,
            duration_ms=1.0,
        )

    def close(self) -> None:
        pass


def _catalog() -> SemanticCatalog:
    return SemanticCatalog(
        metrics={
            "total_revenue": Metric(
                name="total_revenue",
                description="Total revenue.",
                table="fact_orders",
                expression="SUM(total_amount)",
            )
        },
        dimensions={
            "customer_country": Dimension(
                name="customer_country",
                description="Country.",
                table="dim_customers",
                column="country",
            )
        },
        joins=[
            Join(
                left_table="fact_orders",
                left_key="customer_id",
                right_table="dim_customers",
                right_key="customer_id",
            )
        ],
    )


def _policy(max_rows: int = 1000) -> RolePolicy:
    return RolePolicy(
        description="test",
        allowed_tables=["fact_orders", "dim_customers"],
        allowed_columns={
            "fact_orders": ["customer_id", "total_amount"],
            "dim_customers": ["customer_id", "country"],
        },
        pii_columns={"fact_orders": ["customer_id"], "dim_customers": ["customer_id"]},
        max_rows=max_rows,
    )


def _fake_graph(structured_args: dict, max_rows: int = 1000):
    plain_llm = FakeMessagesListChatModel(
        responses=[
            AIMessage(content="revenue by country"),
            AIMessage(content="Revenue was strongest in France."),
        ]
    )
    structured_llm = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "QueryIntent", "args": structured_args, "id": "1"}
                ],
            )
        ]
    )
    metric_chunk = RetrievedChunk(
        text="total_revenue: Total revenue.",
        source="metric:total_revenue",
        section=None,
        doc_type="metric",
    )
    return build_graph(
        plain_llm,
        structured_llm,
        _catalog(),
        _policy(max_rows),
        FakeExecutor(),
        metric_retriever=lambda q: [metric_chunk],
        citation_retriever=lambda r: [],
    )


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    # isolate the audit DB per test so tests don't share state via the real
    # on-disk audit/audit.db
    from src.governance_agent.audit import AuditLog

    audit_log = AuditLog(tmp_path / "audit.db")
    monkeypatch.setattr(main, "_audit_log", lambda: audit_log)
    monkeypatch.setattr(
        main,
        "get_agent_graph",
        lambda role: _fake_graph(
            {"metric": "total_revenue", "dimensions": ["customer_country"]}
        ),
    )
    return TestClient(main.app)


def test_health() -> None:
    client = TestClient(main.app)
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_metrics_lists_the_real_catalog() -> None:
    client = TestClient(main.app)
    response = client.get("/metrics")

    assert response.status_code == 200
    body = response.json()
    names = {m["name"] for m in body["metrics"]}
    assert "total_revenue" in names
    assert "customer_country" in body["dimensions"]


def test_ask_happy_path_returns_200_with_rows_and_answer(client: TestClient) -> None:
    response = client.post(
        "/ask",
        json={"question": "revenue by country"},
        headers={"X-Role": "analyst", "X-User": "nessim"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["verdict"] == "allowed"
    assert body["answer"] == "Revenue was strongest in France."
    assert body["rows"] == [["France", 1000.0]]
    assert body["metric_used"] == "total_revenue"
    assert "request_id" in body


def test_ask_defaults_role_and_user_when_headers_are_missing(
    client: TestClient,
) -> None:
    response = client.post("/ask", json={"question": "revenue by country"})

    assert response.status_code == 200


def test_ask_blocked_returns_403_with_reason_and_citations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from src.governance_agent.audit import AuditLog

    audit_log = AuditLog(tmp_path / "audit.db")
    monkeypatch.setattr(main, "_audit_log", lambda: audit_log)
    monkeypatch.setattr(
        main,
        "get_agent_graph",
        lambda role: _fake_graph({"metric": "total_revenue", "limit": 999999}),
    )
    client = TestClient(main.app)

    response = client.post(
        "/ask", json={"question": "give me everything"}, headers={"X-Role": "analyst"}
    )

    assert response.status_code == 403
    body = response.json()["detail"]
    assert body["verdict"] == "blocked"
    assert "max_rows" in body["block_reason"]
    assert body["answer"] is None


def test_ask_unknown_role_returns_400(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_unknown_role(role: str):
        raise UnknownRoleError(role)

    monkeypatch.setattr(main, "get_agent_graph", raise_unknown_role)
    client = TestClient(main.app)

    response = client.post(
        "/ask", json={"question": "x"}, headers={"X-Role": "nonexistent_role"}
    )

    assert response.status_code == 400
    assert "nonexistent_role" in response.json()["detail"]


def test_ask_writes_one_audit_record_per_request(client: TestClient) -> None:
    from src.governance_agent.audit import AuditLog

    client.post(
        "/ask",
        json={"question": "q1"},
        headers={"X-Role": "analyst", "X-User": "nessim"},
    )
    client.post(
        "/ask",
        json={"question": "q2"},
        headers={"X-Role": "analyst", "X-User": "nessim"},
    )

    audit_log: AuditLog = main._audit_log()
    records = audit_log.all_records()
    assert len(records) == 2
    assert [r.question for r in records] == ["q1", "q2"]
    assert all(r.verdict == "allowed" for r in records)


def test_multiple_sequential_requests_do_not_crash_on_sqlite_threading(
    client: TestClient,
) -> None:
    # Regression test: FastAPI's sync route handlers can run on different
    # threadpool threads per request; the audit log must handle that.
    for i in range(5):
        response = client.post(
            "/ask", json={"question": f"q{i}"}, headers={"X-Role": "analyst"}
        )
        assert response.status_code == 200
