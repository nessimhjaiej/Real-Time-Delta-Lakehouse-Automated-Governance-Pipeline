"""Retriever tests. The filter/mapping logic is tested against a fake
vectorstore (no network, no Qdrant) -- only the one integration test at the
bottom touches real Qdrant, and it's skipped automatically when Qdrant
isn't reachable (same pattern as test_ingest.py / test_executors.py).
"""

from __future__ import annotations

import socket

import pytest
from langchain_core.documents import Document

from src.governance_agent.rag.retriever import (
    RetrievedChunk,
    discover_metric,
    explain_block,
)


def _qdrant_is_up(
    host: str = "localhost", port: int = 6333, timeout: float = 0.5
) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


requires_qdrant = pytest.mark.skipif(
    not _qdrant_is_up(), reason="Qdrant is not running on localhost:6333"
)


class FakeStore:
    """Stands in for QdrantVectorStore: records the call it received and
    returns canned Documents, so the retriever's filter-building and
    result-mapping logic can be tested without any real vector search."""

    def __init__(self, documents: list[Document]) -> None:
        self._documents = documents
        self.last_query: str | None = None
        self.last_k: int | None = None
        self.last_filter = None

    def similarity_search(
        self, query: str, k: int, filter: object = None
    ) -> list[Document]:
        self.last_query = query
        self.last_k = k
        self.last_filter = filter
        return self._documents[:k]


def _doc(text: str, source: str, doc_type: str, section: str | None = None) -> Document:
    return Document(
        page_content=text,
        metadata={
            "source": source,
            "section": section,
            "doc_type": doc_type,
            "allowed_roles": ["analyst"],
        },
    )


def test_discover_metric_filters_to_metric_doc_type() -> None:
    store = FakeStore([_doc("total revenue...", "metric:total_revenue", "metric")])

    results = discover_metric("how much revenue", store=store)

    assert store.last_query == "how much revenue"
    filter_dict = store.last_filter.model_dump()
    assert filter_dict["must"][0]["match"]["any"] == ["metric"]
    assert results == [
        RetrievedChunk(
            text="total revenue...",
            source="metric:total_revenue",
            section=None,
            doc_type="metric",
        )
    ]


def test_discover_metric_respects_k() -> None:
    store = FakeStore(
        [
            _doc("a", "metric:a", "metric"),
            _doc("b", "metric:b", "metric"),
            _doc("c", "metric:c", "metric"),
        ]
    )

    results = discover_metric("question", store=store, k=2)

    assert store.last_k == 2
    assert len(results) == 2


def test_explain_block_filters_to_policy_doc_type() -> None:
    store = FakeStore(
        [_doc("pii rule text", "policy:pii-handling#rule", "policy", section="rule")]
    )

    results = explain_block("PII column not allowed", store=store)

    filter_dict = store.last_filter.model_dump()
    assert filter_dict["must"][0]["match"]["any"] == ["policy"]
    assert results[0].section == "rule"
    assert results[0].doc_type == "policy"


def test_explain_block_never_includes_metric_doc_type_in_its_filter() -> None:
    store = FakeStore([])

    explain_block("any reason", store=store)

    filter_dict = store.last_filter.model_dump()
    assert "metric" not in filter_dict["must"][0]["match"]["any"]


def test_empty_results_return_empty_list_not_an_error() -> None:
    store = FakeStore([])

    assert discover_metric("nonsense question", store=store) == []
    assert explain_block("nonsense reason", store=store) == []


# --- live integration (skipped if Qdrant isn't up) -----------------------


@requires_qdrant
def test_discover_metric_ranks_the_right_metric_first_against_real_data() -> None:
    from src.governance_agent.rag.ingest import ingest_all

    ingest_all()

    results = discover_metric("how much total money did we make")

    assert results
    assert results[0].source == "metric:total_revenue"


@requires_qdrant
def test_explain_block_cites_the_right_policy_against_real_data() -> None:
    from src.governance_agent.rag.ingest import ingest_all

    ingest_all()

    results = explain_block("PII column not allowed in SELECT: dim_customers.email")

    # The right *document*, not one exact rule: which PII rule ranks first
    # shifts with the embedding model and with edits to the placeholder docs.
    assert results
    assert results[0].source.startswith("policy:pii-handling#")
