"""LangGraph agent tests. Every external dependency is faked: the LLM
(two FakeMessagesListChatModel instances, per design decision #6's "mock
the LLM"), the executor (FakeExecutor below, no MinIO), and retrieval (a
plain function returning canned RetrievedChunks, no Qdrant). These tests
need no network access and no docker services running.
"""

from __future__ import annotations

from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage

from src.governance_agent.agent.graph import build_graph
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
    """Records whether/how it was called, so blocked-path tests can assert
    execution never happened."""

    def __init__(self, result: QueryResult | None = None) -> None:
        self.result = result or QueryResult(
            columns=["country", "total_revenue"],
            rows=[["France", 1000.0], ["Germany", 500.0]],
            row_count=2,
            duration_ms=1.0,
        )
        self.calls: list[str] = []

    def execute(self, sql: str) -> QueryResult:
        self.calls.append(sql)
        return self.result

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


def _plain_llm(
    interpret_text: str = "revenue by country",
    narrate_text: str = "Revenue was higher in France.",
) -> FakeMessagesListChatModel:
    return FakeMessagesListChatModel(
        responses=[AIMessage(content=interpret_text), AIMessage(content=narrate_text)]
    )


def _structured_llm(tool_call_args: dict | None) -> FakeMessagesListChatModel:
    if tool_call_args is None:
        return FakeMessagesListChatModel(
            responses=[AIMessage(content="I can't help with that.")]
        )
    return FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "QueryIntent", "args": tool_call_args, "id": "call_1"}
                ],
            )
        ]
    )


def _fake_metric_retriever(chunks: list[RetrievedChunk]):
    return lambda query: chunks


def _fake_citation_retriever(chunks: list[RetrievedChunk]):
    return lambda reason: chunks


METRIC_CHUNK = RetrievedChunk(
    text="total_revenue: Total revenue.",
    source="metric:total_revenue",
    section=None,
    doc_type="metric",
)
POLICY_CHUNK = RetrievedChunk(
    text="no PII in results",
    source="policy:pii-handling#rule",
    section="rule",
    doc_type="policy",
)


# --- happy path ------------------------------------------------------


def test_happy_path_runs_every_node_and_returns_allowed() -> None:
    executor = FakeExecutor()
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=_structured_llm(
            {"metric": "total_revenue", "dimensions": ["customer_country"]}
        ),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([]),
    )

    result = graph.invoke(
        {
            "question": "what's our revenue by country",
            "role": "analyst",
            "user": "nessim",
        }
    )

    assert result["verdict"] == "allowed"
    assert result["search_query"] == "revenue by country"
    assert result["retrieved_metrics"] == [METRIC_CHUNK]
    assert result["structured_intent"].metric == "total_revenue"
    assert "fact_orders" in result["compiled_sql"]
    assert result["query_result"].row_count == 2
    assert result["narration"] == "Revenue was higher in France."
    assert executor.calls == [
        result["compiled_sql"]
    ]  # executed exactly the compiled SQL


def test_happy_path_falls_back_to_raw_question_if_interpret_returns_blank() -> None:
    blank_llm = FakeMessagesListChatModel(
        responses=[AIMessage(content="   "), AIMessage(content="narration")]
    )
    executor = FakeExecutor()
    graph = build_graph(
        llm=blank_llm,
        structured_llm=_structured_llm({"metric": "total_revenue"}),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([]),
    )

    result = graph.invoke(
        {"question": "raw question text", "role": "analyst", "user": "nessim"}
    )

    assert result["search_query"] == "raw question text"


# --- blocked: the model declines via CannotAnswer --------------------------


def test_cannot_answer_blocks_without_executing_and_cites_a_policy() -> None:
    executor = FakeExecutor()
    decline = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "CannotAnswer",
                        "args": {"reason": "no customer name dimension exists"},
                        "id": "call_1",
                    }
                ],
            )
        ]
    )
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=decline,
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([POLICY_CHUNK]),
    )

    result = graph.invoke(
        {"question": "name of our best customer", "role": "analyst", "user": "n"}
    )

    assert result["verdict"] == "blocked"
    assert "no customer name dimension exists" in result["block_reason"]
    assert result["citations"] == [POLICY_CHUNK]
    assert executor.calls == []
    assert "compiled_sql" not in result


# --- blocked: validator rejects the compiled SQL -------------------------


def test_validator_block_skips_execution_and_cites_a_policy() -> None:
    executor = FakeExecutor()
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=_structured_llm({"metric": "total_revenue", "limit": 999999}),
        catalog=_catalog(),
        policy=_policy(max_rows=1000),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([POLICY_CHUNK]),
    )

    result = graph.invoke(
        {"question": "give me everything", "role": "analyst", "user": "nessim"}
    )

    assert result["verdict"] == "blocked"
    assert "max_rows" in result["block_reason"]
    assert result["citations"] == [POLICY_CHUNK]
    assert executor.calls == []  # never executed
    assert "narration" not in result or result.get("narration") is None


# --- blocked: compile fails (hallucinated/unknown metric) ----------------


def test_unknown_metric_blocks_before_validation_or_execution() -> None:
    executor = FakeExecutor()
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=_structured_llm({"metric": "revenue_that_does_not_exist"}),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([]),
        citation_retriever=_fake_citation_retriever([POLICY_CHUNK]),
    )

    result = graph.invoke(
        {"question": "what's the zzz metric", "role": "analyst", "user": "nessim"}
    )

    assert result["verdict"] == "blocked"
    assert "Unknown metric" in result["block_reason"]
    assert executor.calls == []


def test_unknown_dimension_blocks_before_validation_or_execution() -> None:
    executor = FakeExecutor()
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=_structured_llm(
            {"metric": "total_revenue", "dimensions": ["not_a_real_dimension"]}
        ),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([]),
    )

    result = graph.invoke(
        {"question": "revenue by zzz", "role": "analyst", "user": "nessim"}
    )

    assert result["verdict"] == "blocked"
    assert "Unknown dimension" in result["block_reason"]
    assert executor.calls == []


# --- blocked: the model declines / doesn't call the tool -----------------


def test_no_tool_call_blocks_with_a_generic_reason() -> None:
    executor = FakeExecutor()
    graph = build_graph(
        llm=_plain_llm(),
        structured_llm=_structured_llm(None),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([]),
        citation_retriever=_fake_citation_retriever([]),
    )

    result = graph.invoke(
        {"question": "asdkjfh nonsense", "role": "analyst", "user": "nessim"}
    )

    assert result["verdict"] == "blocked"
    assert "known metric" in result["block_reason"]
    assert executor.calls == []


# --- the model never produces the numbers --------------------------------


def test_narration_prompt_is_built_from_real_query_result_not_invented(
    monkeypatch: object,
) -> None:
    captured_prompts: list[str] = []

    class CapturingLLM(FakeMessagesListChatModel):
        def invoke(self, messages, *args, **kwargs):  # type: ignore[override]
            captured_prompts.append(str(messages[-1].content))
            return super().invoke(messages, *args, **kwargs)

    llm = CapturingLLM(
        responses=[AIMessage(content="search"), AIMessage(content="done")]
    )
    executor = FakeExecutor(
        result=QueryResult(columns=["x"], rows=[[42]], row_count=1, duration_ms=1.0)
    )
    graph = build_graph(
        llm=llm,
        structured_llm=_structured_llm({"metric": "total_revenue"}),
        catalog=_catalog(),
        policy=_policy(),
        executor=executor,
        metric_retriever=_fake_metric_retriever([METRIC_CHUNK]),
        citation_retriever=_fake_citation_retriever([]),
    )

    graph.invoke({"question": "q", "role": "analyst", "user": "nessim"})

    narrate_prompt = captured_prompts[-1]
    assert "42" in narrate_prompt  # the real row made it into the prompt
    assert "x" in narrate_prompt
