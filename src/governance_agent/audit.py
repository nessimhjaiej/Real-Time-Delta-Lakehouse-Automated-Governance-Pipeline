"""Append-only audit log for every /ask request.

SQLite: no new container to satisfy "add no heavy services beyond Qdrant",
append-only by convention (this module only ever INSERTs), and queryable
via plain SQL for free once there's enough data to care about (block rate,
latency percentiles) -- see the Stage 1 report for the full justification
against a flat JSONL file or a new Postgres container.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AUDIT_DB_PATH = PROJECT_ROOT / "audit" / "audit.db"

Verdict = Literal["allowed", "blocked"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    user TEXT NOT NULL,
    role TEXT NOT NULL,
    question TEXT NOT NULL,
    retrieved_chunk_ids TEXT NOT NULL,
    structured_intent TEXT,
    compiled_sql TEXT,
    verdict TEXT NOT NULL,
    block_reason TEXT,
    row_count INTEGER,
    latency_ms REAL NOT NULL
)
"""

_COLUMNS = (
    "timestamp, user, role, question, retrieved_chunk_ids, structured_intent, "
    "compiled_sql, verdict, block_reason, row_count, latency_ms"
)


class AuditRecord(BaseModel):
    timestamp: datetime
    user: str
    role: str
    question: str
    retrieved_chunk_ids: list[str] = []
    structured_intent: dict[str, Any] | None = None
    compiled_sql: str | None = None
    verdict: Verdict
    block_reason: str | None = None
    row_count: int | None = None
    latency_ms: float


class AuditLog:
    """INSERT-only wrapper around a SQLite audit_log table.

    Safe to share one instance across FastAPI's threadpool: sync route
    handlers can each land on a different worker thread, so the connection
    is opened with check_same_thread=False, and writes are serialized with
    a lock (sqlite3 connections aren't safe for concurrent use from
    multiple threads without one)."""

    def __init__(self, db_path: Path = DEFAULT_AUDIT_DB_PATH) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def record(self, entry: AuditRecord) -> int:
        with self._lock:
            cursor = self._conn.execute(
                f"INSERT INTO audit_log ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entry.timestamp.isoformat(),
                    entry.user,
                    entry.role,
                    entry.question,
                    json.dumps(entry.retrieved_chunk_ids),
                    (
                        json.dumps(entry.structured_intent)
                        if entry.structured_intent is not None
                        else None
                    ),
                    entry.compiled_sql,
                    entry.verdict,
                    entry.block_reason,
                    entry.row_count,
                    entry.latency_ms,
                ),
            )
            self._conn.commit()
            row_id = cursor.lastrowid
            assert row_id is not None
            return row_id

    def all_records(self) -> list[AuditRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_COLUMNS} FROM audit_log ORDER BY id"
            ).fetchall()
        return [
            AuditRecord(
                timestamp=row[0],
                user=row[1],
                role=row[2],
                question=row[3],
                retrieved_chunk_ids=json.loads(row[4]),
                structured_intent=json.loads(row[5]) if row[5] is not None else None,
                compiled_sql=row[6],
                verdict=row[7],
                block_reason=row[8],
                row_count=row[9],
                latency_ms=row[10],
            )
            for row in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
