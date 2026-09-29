"""Regression tests for the Silver-to-Gold GX gate (src/pipelines/quality.py).

Each test reproduces a specific finding from the Silver EDA
(src/notebooks/01_explore_bronze.py, src/notebooks/02_explore_silver.py)
instead of made-up data, so a test failing here points back at the exact
notebook cell that first found the problem. Silver (src/pipelines/silver.py)
is where those problems get *fixed* -- PII hashing, dedup, the composite
order_line_id key; this module only checks that the fix actually held.
"""

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
CUSTOMER_COLUMNS = ["customer_id", "email", "ip_address"]


def test_batch_gate_catches_the_inv_1003_bronze_duplicate(spark: SparkSession) -> None:
    # 02_explore_silver.py: INV-1003 comes out of Bronze corrupted -- the same
    # line item twice with a different timestamp. dedup_cols in
    # silver.cleanse() includes timestamp, so both rows survive
    # dropDuplicates, but order_line_id excludes timestamp so they collide.
    # "there is still 1 duplicate because of the timestamp" /
    # "turns out the data is corrupted from the bronze layer".
    rows = [
        ("INV-1003_CUST-1_1", "CUST-1", "1", 2, 25.5),
        ("INV-1003_CUST-1_1", "CUST-1", "1", 2, 25.5),
    ]
    df = local_df(spark, rows, ORDER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_orders_batch", df)

    assert any(
        "order_line_id: expect_column_values_to_be_unique" in f for f in failures
    )


def test_stream_gate_catches_the_33_duplicate_regression(spark: SparkSession) -> None:
    # 02_explore_silver.py: order_line_id 'INV-STREAM-5028_1' and
    # 'INV-STREAM-5079_2' were each found duplicated in silver_orders_stream
    # (33 affected keys total) -- same root cause as the batch case, just far
    # more frequent on the stream because retries are more common there.
    rows = [
        ("INV-STREAM-5028_1", "CUST-5028", "1", 1, 9.99),
        ("INV-STREAM-5028_1", "CUST-5028", "1", 1, 9.99),
        ("INV-STREAM-5079_2", "CUST-5079", "2", 1, 14.5),
        ("INV-STREAM-5079_2", "CUST-5079", "2", 1, 14.5),
    ]
    df = local_df(spark, rows, ORDER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_orders_stream", df)

    assert any(
        "order_line_id: expect_column_values_to_be_unique" in f for f in failures
    )


def test_orders_gate_catches_bronze_negative_values(spark: SparkSession) -> None:
    # 01_explore_bronze.py's cross-table check:
    # "quantity < 0 OR unit_price < 0 ... OR price < 0". 0 itself was never
    # flagged as invalid, so the gate's quantity floor is 0, not 1.
    rows = [("INV-2000_CUST-2_1", "CUST-2", "1", -1, -9.99)]
    df = local_df(spark, rows, ORDER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_orders_batch", df)

    assert any("quantity: expect_column_values_to_be_between" in f for f in failures)
    assert any("unit_price: expect_column_values_to_be_between" in f for f in failures)


def test_products_gate_catches_duplicate_id_and_negative_price(
    spark: SparkSession,
) -> None:
    # 01_explore_bronze.py / 02_explore_silver.py both found the products
    # catalog "ALL GOOD" (no duplicate ids) -- this guards that staying true,
    # plus the same negative-price check as the orders tables.
    df = local_df(spark, [(1, 15.99), (1, 15.99), (2, -5.0)], PRODUCT_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_products_catalog", df)

    assert any("id: expect_column_values_to_be_unique" in f for f in failures)
    assert any("price: expect_column_values_to_be_between" in f for f in failures)


def test_customers_gate_catches_bronze_null_and_duplicate_keys(
    spark: SparkSession,
) -> None:
    # 01_explore_bronze.py: "select count(*) ... where customer_id is null or
    # email is null or ip_address is null" and the customer_id duplicate
    # check right after it -- both checked as one combined pass over Bronze.
    rows = [
        ("CUST-1", "a@example.com", "10.0.0.1"),
        ("CUST-1", "a@example.com", "10.0.0.1"),
        (None, "b@example.com", "10.0.0.2"),
        ("CUST-3", None, "10.0.0.3"),
        ("CUST-4", "d@example.com", None),
    ]
    df = local_df(spark, rows, CUSTOMER_COLUMNS)
    context = gx.get_context(mode="ephemeral")

    failures = _validate_table(context, "silver_customers", df)

    assert any("customer_id: expect_column_values_to_be_unique" in f for f in failures)
    assert any(
        "customer_id: expect_column_values_to_not_be_null" in f for f in failures
    )
    assert any("email: expect_column_values_to_not_be_null" in f for f in failures)
    assert any("ip_address: expect_column_values_to_not_be_null" in f for f in failures)


def test_quality_gate_passes_on_clean_silver_tables(spark: SparkSession) -> None:
    orders = local_df(
        spark,
        [("INV-1_CUST-1_1", "CUST-1", "1", 2, 25.5)],
        ORDER_COLUMNS,
    )
    products = local_df(spark, [(1, 15.99)], PRODUCT_COLUMNS)
    customers = local_df(
        spark, [("CUST-1", "a@example.com", "10.0.0.1")], CUSTOMER_COLUMNS
    )
    context = gx.get_context(mode="ephemeral")

    assert _validate_table(context, "silver_orders_batch", orders) == []
    assert _validate_table(context, "silver_products_catalog", products) == []
    assert _validate_table(context, "silver_customers", customers) == []
