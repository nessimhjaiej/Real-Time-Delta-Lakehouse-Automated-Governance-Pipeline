"""Chunk-building tests. These never touch Qdrant or the embedding model --
only ingest_all() does, and its one integration test is skipped
automatically when Qdrant isn't reachable (same pattern as
test_executors.py's MinIO skip).
"""

from __future__ import annotations

import socket
import uuid
from pathlib import Path
from typing import Any

import pytest
from langchain_core.documents import Document

from src.governance_agent.rag.ingest import (
    CHUNK_SIZE,
    COLLECTION_NAME,
    EMBEDDING_DIM,
    Chunk,
    build_all_chunks,
    build_metric_chunks,
    build_policy_chunks,
    get_qdrant_client,
    get_vectorstore,
    ingest_all,
    reset_collection,
)
from src.governance_agent.semantic.models import Metric, SemanticCatalog


def _minimal_catalog() -> SemanticCatalog:
    return SemanticCatalog(
        metrics={
            "revenue": Metric(
                name="revenue",
                description="Total revenue.",
                table="fact_orders",
                expression="SUM(total_amount)",
            )
        },
        dimensions={},
        joins=[],
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


# --- Chunk -----------------------------------------------------------


def test_point_id_is_deterministic_for_the_same_source_and_index() -> None:
    chunk_a = Chunk(
        text="x",
        source="metric:revenue",
        section=None,
        doc_type="metric",
        chunk_index=0,
    )
    chunk_b = Chunk(
        text="different text",
        source="metric:revenue",
        section=None,
        doc_type="metric",
        chunk_index=0,
    )

    assert (
        chunk_a.point_id == chunk_b.point_id
    )  # same source+index -> same id regardless of text


def test_point_id_differs_by_chunk_index() -> None:
    chunk_a = Chunk(
        text="x", source="policy:a#b", section="b", doc_type="policy", chunk_index=0
    )
    chunk_b = Chunk(
        text="x", source="policy:a#b", section="b", doc_type="policy", chunk_index=1
    )

    assert chunk_a.point_id != chunk_b.point_id


def test_to_document_carries_all_payload_fields() -> None:
    chunk = Chunk(
        text="body",
        source="metric:revenue",
        section="s",
        doc_type="metric",
        allowed_roles=["analyst"],
    )

    doc = chunk.to_document()

    assert doc.page_content == "body"
    assert doc.metadata == {
        "source": "metric:revenue",
        "section": "s",
        "doc_type": "metric",
        "allowed_roles": ["analyst"],
    }


# --- build_metric_chunks ----------------------------------------------


def test_build_metric_chunks_one_per_metric() -> None:
    chunks = build_metric_chunks(_minimal_catalog())

    assert len(chunks) == 1
    assert chunks[0].source == "metric:revenue"
    assert chunks[0].doc_type == "metric"
    assert "Total revenue." in chunks[0].text


def test_build_metric_chunks_against_the_real_catalog() -> None:
    chunks = build_metric_chunks()

    sources = {c.source for c in chunks}
    assert sources == {
        "metric:total_revenue",
        "metric:order_count",
        "metric:order_line_count",
        "metric:total_quantity_sold",
        "metric:average_order_value",
    }


# --- build_policy_chunks ------------------------------------------------


def test_build_policy_chunks_splits_on_rule_headings(tmp_path: Path) -> None:
    (tmp_path / "sample.md").write_text(
        "# Sample Policy\n\n"
        "> placeholder banner, not a rule\n\n"
        "## Rule: first rule\n\nBody of the first rule.\n\n"
        "## Rule: second rule\n\nBody of the second rule.\n",
        encoding="utf-8",
    )

    chunks = build_policy_chunks(tmp_path)

    assert [c.section for c in chunks] == ["first rule", "second rule"]
    assert chunks[0].source == "policy:sample#first-rule"
    assert "Body of the first rule." in chunks[0].text
    assert all(c.doc_type == "policy" for c in chunks)


def test_build_policy_chunks_drops_content_before_the_first_heading(
    tmp_path: Path,
) -> None:
    (tmp_path / "sample.md").write_text(
        "This banner text is not a rule and must not become a chunk.\n\n"
        "## Rule: only rule\n\nActual rule body.\n",
        encoding="utf-8",
    )

    chunks = build_policy_chunks(tmp_path)

    assert len(chunks) == 1
    assert "banner" not in chunks[0].text


def test_build_policy_chunks_against_the_real_docs() -> None:
    chunks = build_policy_chunks()

    # No exact count: policies/docs/*.md are placeholders meant to be edited.
    assert chunks
    assert all(c.doc_type == "policy" for c in chunks)
    assert len({c.point_id for c in chunks}) == len(chunks)  # all unique


def test_long_section_is_split_with_overlap(tmp_path: Path) -> None:
    long_body = "word " * (CHUNK_SIZE // 4)  # comfortably exceeds CHUNK_SIZE
    (tmp_path / "long.md").write_text(
        f"## Rule: long rule\n\n{long_body}\n", encoding="utf-8"
    )

    chunks = build_policy_chunks(tmp_path)

    assert len(chunks) > 1
    assert all(c.section == "long rule" for c in chunks)
    assert [c.chunk_index for c in chunks] == list(range(len(chunks)))
    assert len({c.point_id for c in chunks}) == len(chunks)  # still all unique


# --- build_all_chunks / ingest_all ---------------------------------------


def test_build_all_chunks_combines_metrics_and_policies() -> None:
    chunks = build_all_chunks(_minimal_catalog())

    doc_types = {c.doc_type for c in chunks}
    assert "metric" in doc_types
    assert "policy" in doc_types


@requires_qdrant
def test_ingest_all_upserts_into_qdrant_without_duplicating() -> None:
    first_count = ingest_all()
    second_count = (
        ingest_all()
    )  # re-ingesting the same content must upsert, not duplicate

    assert first_count == second_count
    assert first_count > 0


# --- overwrite ---------------------------------------------------------------


class _FakeQdrantClient:
    """Records the collection calls reset_collection makes, in order."""

    def __init__(self, exists: bool) -> None:
        self.exists = exists
        self.calls: list[tuple[str, str, Any]] = []

    def collection_exists(self, name: str) -> bool:
        return self.exists

    def delete_collection(self, name: str) -> None:
        self.calls.append(("delete", name, None))
        self.exists = False

    def create_collection(self, name: str, vectors_config: Any) -> None:
        self.calls.append(("create", name, vectors_config.size))


def test_reset_collection_drops_an_existing_collection_then_recreates_it() -> None:
    client = _FakeQdrantClient(exists=True)

    reset_collection(client, "c")

    assert client.calls == [("delete", "c", None), ("create", "c", EMBEDDING_DIM)]


def test_reset_collection_just_creates_when_nothing_exists_yet() -> None:
    client = _FakeQdrantClient(exists=False)

    reset_collection(client, "c")

    assert client.calls == [("create", "c", EMBEDDING_DIM)]


def test_ingest_all_overwrite_rejects_a_prebuilt_store() -> None:
    # A prebuilt store has already validated the old collection, so the flag
    # can't be honored -- fail loudly rather than silently ignore it.
    with pytest.raises(ValueError, match="overwrite"):
        ingest_all(store=object(), overwrite=True)  # type: ignore[arg-type]


@requires_qdrant
def test_ingest_all_overwrite_removes_chunks_that_are_no_longer_in_the_files() -> None:
    ingest_all()
    stale_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "policy:deleted-doc#removed-rule:0"))
    get_vectorstore().add_documents(
        [
            Document(
                page_content="a rule that no longer exists in any file",
                metadata={
                    "source": "policy:deleted-doc#removed-rule",
                    "section": "removed rule",
                    "doc_type": "policy",
                    "allowed_roles": ["analyst"],
                },
            )
        ],
        ids=[stale_id],
    )
    client = get_qdrant_client()
    assert client.retrieve(COLLECTION_NAME, ids=[stale_id])  # upsert keeps it

    count = ingest_all(overwrite=True)

    assert not client.retrieve(COLLECTION_NAME, ids=[stale_id])
    assert client.count(COLLECTION_NAME).count == count
