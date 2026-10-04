"""
TOP K cosine similarity is the only thing used here (no reranking options)
"""

from __future__ import annotations

from dataclasses import dataclass

from langchain_qdrant import QdrantVectorStore
from qdrant_client.http.models import FieldCondition, Filter, MatchAny

from src.governance_agent.rag.ingest import get_vectorstore

DEFAULT_METRIC_K = 3
DEFAULT_CITATION_K = 2


@dataclass
class RetrievedChunk:
    text: str
    source: str
    section: str | None
    doc_type: str


def _doc_type_filter(doc_types: list[str]) -> Filter:
    return Filter(
        must=[FieldCondition(key="metadata.doc_type", match=MatchAny(any=doc_types))]
    )


def _search(
    store: QdrantVectorStore, query: str, doc_types: list[str], k: int
) -> list[RetrievedChunk]:
    results = store.similarity_search(query, k=k, filter=_doc_type_filter(doc_types))
    return [
        RetrievedChunk(
            text=doc.page_content,
            source=doc.metadata["source"],
            section=doc.metadata.get("section"),
            doc_type=doc.metadata["doc_type"],
        )
        for doc in results
    ]


def discover_metric(
    question: str, store: QdrantVectorStore | None = None, k: int = DEFAULT_METRIC_K
) -> list[RetrievedChunk]:
    """RAG job (a): which metric(s) match this question?"""
    store = store or get_vectorstore()
    return _search(store, question, ["metric"], k)


def explain_block(
    reason: str, store: QdrantVectorStore | None = None, k: int = DEFAULT_CITATION_K
) -> list[RetrievedChunk]:
    """RAG job (b): retrieve policy text to cite for a block decision."""
    store = store or get_vectorstore()
    return _search(store, reason, ["policy"], k)
