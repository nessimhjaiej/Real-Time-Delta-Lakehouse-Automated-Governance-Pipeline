"""Runs validated SQL against the Gold Delta tables in MinIO, via DuckDB.

Verified during Stage 4 against the live stack: DuckDB's ``delta`` +
``httpfs`` extensions read Delta tables directly off MinIO (S3-compatible,
path-style access) with a ``delta_scan('s3://...')`` call -- no
``deltalake``/delta-rs fallback needed, unlike what Stage 1 left open.
Gold tables are registered as views once at connect time, so compiled SQL
(which references bare table names like "fact_orders") runs unmodified.
"""

from __future__ import annotations

import os
import time

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from src.governance_agent.executors.base import QueryExecutor, QueryResult

GOLD_TABLES = ["fact_orders", "dim_customers", "dim_products"]


def _parse_single_select(sql: str) -> exp.Select:
    """Defense in depth: execute() should only ever receive SQL that already
    passed validator.validate_query, but this guards direct misuse too --
    the same spirit as compiler.py re-parsing instead of trusting input."""
    try:
        statements = [s for s in sqlglot.parse(sql, dialect="duckdb") if s is not None]
    except SqlglotError as error:
        raise ValueError(f"SQL failed to parse: {error}") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("DuckDBExecutor only executes a single SELECT statement")
    return statements[0]


class DuckDBExecutor(QueryExecutor):
    def __init__(
        self,
        minio_endpoint: str | None = None,
        minio_access_key: str | None = None,
        minio_secret_key: str | None = None,
        bucket: str = "lakehouse",
    ) -> None:
        endpoint = (
            minio_endpoint
            if minio_endpoint is not None
            else os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        )
        endpoint = endpoint.removeprefix("http://").removeprefix("https://")
        access_key = (
            minio_access_key
            if minio_access_key is not None
            else os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        )
        secret_key = (
            minio_secret_key
            if minio_secret_key is not None
            else os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
        )

        self._conn = duckdb.connect()
        self._conn.execute("INSTALL httpfs")
        self._conn.execute("LOAD httpfs")
        self._conn.execute("INSTALL delta")
        self._conn.execute("LOAD delta")
        self._conn.execute(
            "CREATE SECRET minio_secret "
            "(TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', USE_SSL false)",
            [access_key, secret_key, endpoint],
        )
        for table in GOLD_TABLES:
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM delta_scan('s3://{bucket}/gold/{table}')"
            )

    def execute(self, sql: str) -> QueryResult:
        statement = _parse_single_select(sql)
        start = time.perf_counter()
        result = self._conn.execute(statement.sql(dialect="duckdb"))
        rows = result.fetchall()
        duration_ms = (time.perf_counter() - start) * 1000
        columns = [d[0] for d in result.description]
        return QueryResult(
            columns=columns,
            rows=[list(row) for row in rows],
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    def close(self) -> None:
        self._conn.close()
