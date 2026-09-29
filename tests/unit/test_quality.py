import great_expectations as gx
from pyspark.sql import DataFrame, SparkSession

from src.pipelines.quality import _validate_table


def _sql_literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, float):
        return f"CAST({value!r} AS DOUBLE)"
    if value is None:
        return "NULL"
    return repr(value)


def local_df(spark: SparkSession, rows: list[tuple], columns: list[str]) -> DataFrame:
    """Build a DataFrame via SQL VALUES (see test_pipeline_validation.py for why)."""
    values = ", ".join(
        "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows
    )
    return spark.sql(f"SELECT * FROM VALUES {values} AS t({', '.join(columns)})")


ORDER_COLUMNS = [
    "order_line_id",
    "customer_id_hash",
    "product_id",
    "quantity",
    "unit_price",
]
PRODUCT_COLUMNS = ["id", "price"]
CUSTOMER_COLUMNS = ["customer_id", "email"]


def test_orders_suite_catches_duplicate_order_line_id_and_bad_values(
    spark: SparkSession,
) -> None:
    # Mirrors the EDA finding in src/notebooks/02_explore_silver.py: a
    # corrupted invoice_id produces two rows sharing one order_line_id.
    rows = [
        ("INV-1003_CUST-1_1", "CUST-1", "1", 2, 25.5),
        ("INV-1003_CUST-1_1", "CUST-1", "1", 2, 25.5),
        ("INV-1004_CUST-2_2", "CUST-2", None, -1, -9.99),
    ]
    df = local_df(spark, rows, ORDER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_orders_batch", df)

    failure_text = "\n".join(failures)
    assert (
        "silver_orders_batch.order_line_id: expect_column_values_to_be_unique"
        in failure_text
    )
    assert (
        "silver_orders_batch.product_id: expect_column_values_to_not_be_null"
        in failure_text
    )
    assert (
        "silver_orders_batch.quantity: expect_column_values_to_be_between"
        in failure_text
    )
    assert (
        "silver_orders_batch.unit_price: expect_column_values_to_be_between"
        in failure_text
    )


def test_dimension_suites_catch_duplicate_and_null_keys(spark: SparkSession) -> None:
    products = local_df(
        spark,
        [(1, 15.99), (1, 15.99), (2, -5.0)],
        PRODUCT_COLUMNS,
    )
    customers = local_df(
        spark,
        [("CUST-1", "a@example.com"), (None, "b@example.com")],
        CUSTOMER_COLUMNS,
    )
    context = gx.get_context(mode="ephemeral")

    product_failures = _validate_table(context, "silver_products_catalog", products)
    customer_failures = _validate_table(context, "silver_customers", customers)

    assert any("id: expect_column_values_to_be_unique" in f for f in product_failures)
    assert any(
        "price: expect_column_values_to_be_between" in f for f in product_failures
    )
    assert any(
        "customer_id: expect_column_values_to_not_be_null" in f
        for f in customer_failures
    )


def test_quality_gate_passes_on_clean_silver_tables(spark: SparkSession) -> None:
    orders = local_df(
        spark,
        [("INV-1_CUST-1_1", "CUST-1", "1", 2, 25.5)],
        ORDER_COLUMNS,
    )
    products = local_df(spark, [(1, 15.99)], PRODUCT_COLUMNS)
    customers = local_df(spark, [("CUST-1", "a@example.com")], CUSTOMER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    assert _validate_table(context, "silver_orders_batch", orders) == []
    assert _validate_table(context, "silver_products_catalog", products) == []
    assert _validate_table(context, "silver_customers", customers) == []
