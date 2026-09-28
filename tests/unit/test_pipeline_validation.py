from pyspark.sql import DataFrame, SparkSession

from src.pipelines.gold import (
    build_dim_customers,
    build_dim_products,
    build_fact_orders,
)
from src.pipelines.silver import cleanse, cleanse_customers

ORDER_COLUMNS = [
    "invoice_id",
    "customer_id",
    "customer_name",
    "email",
    "ip_address",
    "product_id",
    "quantity",
    "unit_price",
    "timestamp",
]
CUSTOMER_COLUMNS = [
    "customer_id",
    "full_name",
    "email",
    "ip_address",
    "country",
    "first_sign_up",
]


def _sql_literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, float):
        return f"CAST({value!r} AS DOUBLE)"
    return repr(value)


def local_df(spark: SparkSession, rows: list[tuple], columns: list[str]) -> DataFrame:
    """Build a DataFrame via SQL VALUES.

    Unlike ``spark.createDataFrame(list)`` this never starts a Python worker,
    which crashes with PySpark 3.5.0 + Python 3.12 on Windows.
    """
    values = ", ".join(
        "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows
    )
    return spark.sql(f"SELECT * FROM VALUES {values} AS t({', '.join(columns)})")


def _order(invoice_id: str, product_id: object, quantity: int, price: float) -> tuple:
    return (
        invoice_id,
        "CUST-100",
        "User_100",
        "user_100@example.com",
        "10.0.0.1",
        product_id,
        quantity,
        price,
        "2026-09-01T10:00:00Z",
    )


def _customers(spark: SparkSession) -> DataFrame:
    row = (
        "CUST-100",
        "Ada Lovelace",
        " User_100@Example.com ",
        "10.0.0.1",
        "UK",
        "2025-01-01",
    )
    return local_df(spark, [row], CUSTOMER_COLUMNS)


def test_silver_orders_do_not_expose_raw_pii(spark: SparkSession) -> None:
    orders = local_df(spark, [_order("INV-1", "1", 2, 25.5)], ORDER_COLUMNS)
    row = cleanse(orders, source_name="test").collect()[0]

    for value in row.asDict().values():
        assert "CUST-100" not in str(value)
        assert "user_100@example.com" not in str(value)
        assert "10.0.0.1" not in str(value)
    assert row["total_amount"] == 51.0


def test_silver_orders_are_deduplicated(spark: SparkSession) -> None:
    row = _order("INV-1", "1", 2, 25.5)
    orders = local_df(spark, [row, row], ORDER_COLUMNS)
    assert cleanse(orders, source_name="test").count() == 1


def test_silver_customer_and_order_hashes_agree(spark: SparkSession) -> None:
    orders = local_df(spark, [_order("INV-1", "1", 1, 9.99)], ORDER_COLUMNS)
    order = cleanse(orders, source_name="test").collect()[0]
    customer = cleanse_customers(_customers(spark)).collect()[0]

    assert customer["customer_id"] == order["customer_id_hash"]
    assert customer["email"] == order["email_hash"]
    assert customer["ip_address"] == order["ip_address_hash"]


def test_gold_fact_orders_join_to_dimensions(spark: SparkSession) -> None:
    # Batch product_id arrives as int (CSV inferSchema), stream as string.
    batch = cleanse(
        local_df(spark, [_order("INV-1", 1, 3, 15.99)], ORDER_COLUMNS),
        source_name="batch",
    )
    stream = cleanse(
        local_df(spark, [_order("INV-2", "1", 1, 15.99)], ORDER_COLUMNS),
        source_name="stream",
    )
    products = spark.sql(
        "SELECT CAST(1 AS BIGINT) AS id, 'bags' AS category, 'desc' AS description,"
        " 15.99D AS price, named_struct('rate', 4.5D, 'count', 10L) AS rating,"
        " 'Backpack' AS title"
    )

    fact = build_fact_orders(batch, stream)
    dim_customers = build_dim_customers(cleanse_customers(_customers(spark)))
    dim_products = build_dim_products(products)

    assert fact.count() == 2
    assert fact.join(dim_customers, "customer_id").count() == 2
    assert fact.join(dim_products, "product_id").count() == 2
    assert dict(fact.dtypes)["product_id"] == dict(dim_products.dtypes)["product_id"]
    assert sorted(r["total_amount"] for r in fact.collect()) == [15.99, 47.97]
