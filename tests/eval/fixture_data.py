"""Small, hand-computable fixture dataset for the eval harness.

Deliberately NOT the real Gold tables, which can change every time the
pipeline re-runs -- every expected_answer in questions.yml is computed by
hand against exactly this data, so eval results stay meaningful and
reproducible run over run, independent of MinIO/Spark state.

Schema matches the real Gold tables exactly (src/pipelines/gold.py):
fact_orders(order_line_id, invoice_id, customer_id, product_id, quantity,
unit_price, total_amount, timestamp), dim_customers(customer_id, full_name,
email, ip_address, country, first_sign_up), dim_products(product_id,
category, description, price, rating_avg, rating_count, title).

Hand-computed totals (verify by re-adding FACT_ORDERS if this ever
changes):
  total_revenue overall      = 200+50+100+150+100+100 = 700.0
  total_revenue France (C1,C3) = 200+50+150+100       = 500.0
  total_revenue Germany (C2)   = 100+100               = 200.0
  total_revenue electronics (P1) = 200+100+100         = 400.0
  total_revenue clothing (P2)    = 50+150+100          = 300.0
  order_count / order_line_count = 6 (every invoice has exactly one line)
  total_quantity_sold         = 2+1+1+3+2+1 = 10
  average_order_value         = 700 / 6 = 116.6666...
"""

from __future__ import annotations

import time

import duckdb

from src.governance_agent.executors.base import QueryExecutor, QueryResult
from src.governance_agent.executors.duckdb_executor import _parse_single_select

# order_line_id, invoice_id, customer_id, product_id, quantity, unit_price, total_amount, timestamp
FACT_ORDERS = [
    ("C1_INV-1_P1", "INV-1", "C1", "P1", 2, 100.0, 200.0, "2026-01-05"),
    ("C1_INV-2_P2", "INV-2", "C1", "P2", 1, 50.0, 50.0, "2026-01-10"),
    ("C2_INV-3_P1", "INV-3", "C2", "P1", 1, 100.0, 100.0, "2026-02-01"),
    ("C3_INV-4_P2", "INV-4", "C3", "P2", 3, 50.0, 150.0, "2026-02-15"),
    ("C2_INV-5_P2", "INV-5", "C2", "P2", 2, 50.0, 100.0, "2026-03-01"),
    ("C3_INV-6_P1", "INV-6", "C3", "P1", 1, 100.0, 100.0, "2026-03-10"),
]

# customer_id, full_name, email, ip_address, country, first_sign_up
DIM_CUSTOMERS = [
    ("C1", "Fixture One", "fixture1@example.com", "10.0.0.1", "France", "2025-01-01"),
    ("C2", "Fixture Two", "fixture2@example.com", "10.0.0.2", "Germany", "2025-01-01"),
    ("C3", "Fixture Three", "fixture3@example.com", "10.0.0.3", "France", "2025-01-01"),
]

# product_id, category, description, price, rating_avg, rating_count, title
DIM_PRODUCTS = [
    ("P1", "electronics", "fixture product", 100.0, 4.5, 10, "Fixture Electronics"),
    ("P2", "clothing", "fixture product", 50.0, 4.0, 5, "Fixture Clothing"),
]


def build_fixture_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE fact_orders (order_line_id VARCHAR, invoice_id VARCHAR, "
        "customer_id VARCHAR, product_id VARCHAR, quantity INTEGER, "
        "unit_price DOUBLE, total_amount DOUBLE, timestamp TIMESTAMP)"
    )
    con.executemany(
        "INSERT INTO fact_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", FACT_ORDERS
    )

    con.execute(
        "CREATE TABLE dim_customers (customer_id VARCHAR, full_name VARCHAR, "
        "email VARCHAR, ip_address VARCHAR, country VARCHAR, first_sign_up DATE)"
    )
    con.executemany(
        "INSERT INTO dim_customers VALUES (?, ?, ?, ?, ?, ?)", DIM_CUSTOMERS
    )

    con.execute(
        "CREATE TABLE dim_products (product_id VARCHAR, category VARCHAR, "
        "description VARCHAR, price DOUBLE, rating_avg DOUBLE, "
        "rating_count INTEGER, title VARCHAR)"
    )
    con.executemany(
        "INSERT INTO dim_products VALUES (?, ?, ?, ?, ?, ?, ?)", DIM_PRODUCTS
    )

    return con


class FixtureExecutor(QueryExecutor):
    """QueryExecutor over the small in-memory fixture above, for the eval
    harness only -- not Qdrant/MinIO-backed, so expected_answer in
    questions.yml stays stable regardless of the real Gold tables' state."""

    def __init__(self) -> None:
        self._conn = build_fixture_connection()

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
