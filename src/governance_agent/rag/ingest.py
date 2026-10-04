"""Chunk per metric and per policy rule -- then embed and upsert into Qdrant.

Chunk-building has no dependency on Qdrant or the embedding model, so it's unit-testable without network access. Only
ingest_all() touches external services .
the embedding model is OpenAI text-embedding-3-small

Search is 100% semantic"""

from __future__ import annotations

import argparse
import re
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

from src.governance_agent.semantic.loader import load_semantic_catalog
from src.governance_agent.semantic.models import SemanticCatalog

PROJECT_ROOT = Path(__file__).resolve().parents[3]
POLICIES_DOCS_DIR = PROJECT_ROOT / "policies" / "docs"

EMBEDDING_MODEL_NAME = "text-embedding-3-small"
EMBEDDING_DIM = 1536
COLLECTION_NAME = "governance_agent"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
DEFAULT_ALLOWED_ROLES = ["analyst"]

DocType = str  # "metric" | "policy"


@dataclass
class Chunk:
    text: str
    source: str
    section: str | None
    doc_type: DocType
    chunk_index: int = 0
    allowed_roles: list[str] = field(
        default_factory=lambda: list(DEFAULT_ALLOWED_ROLES)
    )

    @property
    def point_id(self) -> str:
        """Deterministic uuid5 of source + chunk index -- re-ingesting the
        same source upserts in place instead of creating a duplicate."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{self.source}:{self.chunk_index}"))

    def to_document(self) -> Document:
        return Document(
            page_content=self.text,
            metadata={
                "source": self.source,
                "section": self.section,
                "doc_type": self.doc_type,
                "allowed_roles": self.allowed_roles,
            },
        )


@lru_cache(maxsize=1)
def get_embeddings() -> OpenAIEmbeddings:
    """Lazy singleton. The API key comes from OPENAI_API_KEY (loaded from .env
    here so the ingest CLI and pytest work the same as the API, which already
    loads it)."""
    load_dotenv()
    return OpenAIEmbeddings(model=EMBEDDING_MODEL_NAME)


@lru_cache(maxsize=1)
def get_text_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "section"


def _split_by_heading(
    text: str, heading_pattern: re.Pattern[str]
) -> list[tuple[str, str]]:
    """Split text into (section_title, section_body) pairs at each heading
    match. Content before the first heading (titles, placeholder banners)
    is dropped -- it isn't a citable rule."""
    matches = list(heading_pattern.finditer(text))
    sections = []
    for i, match in enumerate(matches):
        title = match.group(1).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body:
            sections.append((title, body))
    return sections


def _chunk_section(
    title: str, body: str, source_prefix: str, doc_type: DocType
) -> list[Chunk]:
    """One Chunk per section, further split by the overlap splitter only if
    the section's text exceeds CHUNK_SIZE."""
    full_text = f"{title}: {body}"
    if len(full_text) <= CHUNK_SIZE:
        pieces = [full_text]
    else:
        pieces = get_text_splitter().split_text(full_text)
    return [
        Chunk(
            text=piece,
            source=source_prefix,
            section=title,
            doc_type=doc_type,
            chunk_index=i,
        )
        for i, piece in enumerate(pieces)
    ]


def build_metric_chunks(catalog: SemanticCatalog | None = None) -> list[Chunk]:
    catalog = catalog or load_semantic_catalog()
    chunks = []
    for metric in catalog.metrics.values():
        text = (
            f"{metric.name}: {metric.description} "
            f"(table: {metric.table}, expression: {metric.expression})"
        )
        chunks.append(
            Chunk(
                text=text,
                source=f"metric:{metric.name}",
                section=None,
                doc_type="metric",
            )
        )
    return chunks


_RULE_HEADING = re.compile(r"^##\s*Rule:\s*(.+)$", re.MULTILINE)


def build_policy_chunks(docs_dir: Path = POLICIES_DOCS_DIR) -> list[Chunk]:
    chunks = []
    for path in sorted(docs_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        for title, body in _split_by_heading(text, _RULE_HEADING):
            source = f"policy:{path.stem}#{_slugify(title)}"
            chunks.extend(_chunk_section(title, body, source, "policy"))
    return chunks


def build_all_chunks(catalog: SemanticCatalog | None = None) -> list[Chunk]:
    return [
        *build_metric_chunks(catalog),
        *build_policy_chunks(),
    ]


def get_qdrant_client(url: str = "http://localhost:6333") -> QdrantClient:
    return QdrantClient(url=url)


def ensure_collection(client: QdrantClient, name: str = COLLECTION_NAME) -> None:
    if not client.collection_exists(name):
        client.create_collection(
            name,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
        )


def reset_collection(client: QdrantClient, name: str = COLLECTION_NAME) -> None:
    """Drop the collection if it exists and recreate it empty at EMBEDDING_DIM."""
    if client.collection_exists(name):
        client.delete_collection(name)
    ensure_collection(client, name)


def get_vectorstore(
    client: QdrantClient | None = None,
    collection_name: str = COLLECTION_NAME,
    overwrite: bool = False,
) -> QdrantVectorStore:
    client = client or get_qdrant_client()
    # Embeddings first: a missing OPENAI_API_KEY should fail before overwrite
    # drops anything.
    embeddings = get_embeddings()
    if overwrite:
        # Must happen before QdrantVectorStore(...) below: its constructor
        # validates the existing collection's vector size against the
        # embedding model, so a stale collection (e.g. 384-dim from the old
        # model) would raise before it could be reset.
        reset_collection(client, collection_name)
    else:
        ensure_collection(client, collection_name)
    return QdrantVectorStore(
        client=client, collection_name=collection_name, embedding=embeddings
    )


def ingest_all(store: QdrantVectorStore | None = None, overwrite: bool = False) -> int:
    """Build every chunk and write it to Qdrant. Returns the chunk count.

    Default is upsert: chunks go in under deterministic ids, so an edited
    rule updates in place -- but chunks for rules you've since deleted or
    renamed stay behind. overwrite=True drops and rebuilds the collection
    first, so it exactly matches the current files."""
    if store is not None and overwrite:
        raise ValueError(
            "overwrite=True needs ingest_all to build its own store; don't pass one."
        )
    # Chunks first: a malformed YAML/markdown file should fail before
    # overwrite wipes anything.
    chunks = build_all_chunks()
    store = store or get_vectorstore(overwrite=overwrite)
    if not chunks:
        return 0
    store.add_documents(
        [c.to_document() for c in chunks],
        ids=[c.point_id for c in chunks],
    )
    return len(chunks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Embed metrics and policy rules into Qdrant."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Drop and rebuild the collection instead of upserting, so Qdrant "
            "exactly matches the current files (removes chunks for deleted or "
            "renamed rules, and fixes a collection built with another "
            "embedding model)."
        ),
    )
    args = parser.parse_args()
    count = ingest_all(overwrite=args.overwrite)
    action = "rebuilt with" if args.overwrite else "upserted into"
    print(f"{count} chunks {action} Qdrant collection {COLLECTION_NAME!r}.")
