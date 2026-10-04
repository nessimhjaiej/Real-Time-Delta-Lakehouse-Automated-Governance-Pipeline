"""Tests for the executor interface.

DuckDBExecutor needs live MinIO (it reads real Gold Delta tables), so its
tests are skipped automatically when MinIO isn't reachable rather than
failing CI in an environment without the docker stack up. The factory and
the Databricks stub don't touch MinIO, so they always run.
"""

from __future__ import annotations

import socket

import pytest

from src.governance_agent.executors import factory as factory_module
from src.governance_agent.executors.base import QueryExecutor, QueryResult
from src.governance_agent.executors.databricks_executor import DatabricksExecutor
from src.governance_agent.executors.factory import get_executor


def _minio_is_up(
    host: str = "localhost", port: int = 9000, timeout: float = 0.5
) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


requires_minio = pytest.mark.skipif(
    not _minio_is_up(), reason="MinIO is not running on localhost:9000"
)


class _StubExecutor(QueryExecutor):
    def execute(self, sql: str) -> QueryResult:
        raise NotImplementedError

    def close(self) -> None:
        pass


def test_factory_selects_duckdb_for_local_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Building the real DuckDBExecutor binds views over the Delta tables on
    # MinIO, so it fails without MinIO (e.g. in CI). A stub keeps this test to
    # the selection logic; real construction is covered by TestDuckDBExecutor
    # below, which is skipped when MinIO isn't up.
    monkeypatch.setattr(factory_module, "DuckDBExecutor", _StubExecutor)

    assert isinstance(get_executor("local"), _StubExecutor)


def test_factory_selects_databricks_stub_for_databricks_env() -> None:
    executor = get_executor("databricks")
    assert isinstance(executor, DatabricksExecutor)


def test_factory_rejects_unknown_env() -> None:
    with pytest.raises(ValueError, match="Invalid EXECUTION_ENV"):
        get_executor("not-a-real-env")


def test_factory_reads_execution_env_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTION_ENV", "databricks")

    executor = get_executor()

    assert isinstance(executor, DatabricksExecutor)


def test_databricks_stub_execute_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        DatabricksExecutor().execute("SELECT 1")


def test_databricks_stub_close_is_a_no_op() -> None:
    DatabricksExecutor().close()  # must not raise


@requires_minio
class TestDuckDBExecutor:
    def test_executes_a_real_query_against_gold(self) -> None:
        executor = get_executor("local")
        try:
            result = executor.execute(
                "SELECT SUM(total_amount) AS total FROM fact_orders LIMIT 1"
            )

            assert isinstance(result, QueryResult)
            assert result.columns == ["total"]
            assert result.row_count == 1
            assert result.rows[0][0] > 0
            assert result.duration_ms >= 0
        finally:
            executor.close()

    def test_rejects_non_select_statements(self) -> None:
        executor = get_executor("local")
        try:
            with pytest.raises(ValueError, match="single SELECT"):
                executor.execute("DROP TABLE fact_orders")
        finally:
            executor.close()

    def test_rejects_stacked_statements(self) -> None:
        executor = get_executor("local")
        try:
            with pytest.raises(ValueError, match="single SELECT"):
                executor.execute(
                    "SELECT 1 FROM fact_orders LIMIT 1; DROP TABLE fact_orders"
                )
        finally:
            executor.close()

    def test_joins_across_all_three_gold_tables(self) -> None:
        executor = get_executor("local")
        try:
            sql = (
                "SELECT dim_customers.country AS c, dim_products.category AS cat, "
                "SUM(fact_orders.total_amount) AS r FROM fact_orders "
                "LEFT JOIN dim_customers ON fact_orders.customer_id = dim_customers.customer_id "
                "LEFT JOIN dim_products ON fact_orders.product_id = dim_products.product_id "
                "GROUP BY dim_customers.country, dim_products.category LIMIT 5"
            )

            result = executor.execute(sql)

            assert result.columns == ["c", "cat", "r"]
            assert result.row_count <= 5
        finally:
            executor.close()
