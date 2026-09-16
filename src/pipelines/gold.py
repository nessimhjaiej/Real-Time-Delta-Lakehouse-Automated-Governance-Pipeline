"""Gold layer: staging, cleaning, and dimensional modeling.

Builds the Gold analytical layer from Silver tables:
    - dim_customers  <- silver_customers
    - dim_products   <- silver_products
    - fact_orders    <- silver_orders_batch + silver_orders_stream

Each build_* function is a pure transformation (DataFrame in, DataFrame out)
so it can be unit tested against small in-memory DataFrames without needing
a live Delta/MinIO setup.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

# ---------------------------------------------------------------------------
# Dimension: customers
# ---------------------------------------------------------------------------

DIM_CUSTOMERS_COLUMNS = [
    "customer_id",
    "full_name",
    "email",
    "ip_address",
    "country",
    "first_sign_up",
]


def build_dim_customers(silver_customers: DataFrame) -> DataFrame:
    """Stage and clean ``silver_customers`` into the ``dim_customers`` table.

    Drops technical metadata (``_ingested_at``, ``_source_system``) and keeps
    only the business attributes, keyed by ``customer_id``.
    """
    return silver_customers.select(*DIM_CUSTOMERS_COLUMNS)


# ---------------------------------------------------------------------------
# Dimension: products
# ---------------------------------------------------------------------------

DIM_PRODUCTS_COLUMNS = [
    "product_id",
    "category",
    "description",
    "price",
    "rating_avg",
    "rating_count",
    "title",
]


def build_dim_products(silver_products: DataFrame) -> DataFrame:
    """Stage and clean ``silver_products`` into the ``dim_products`` table.

    Drops technical metadata (``_ingested_at``, ``_source_system``,
    ``_processed_at``), renames ``id`` to ``product_id`` (the dimension's
    primary key), and flattens the ``rating`` struct
    (``{rate, count}``) into ``rating_avg``/``rating_count`` so downstream
    SQL/BI consumers don't have to deal with nested fields.
    """
    return silver_products.select(
        f.col("id").alias("product_id"),
        "category",
        "description",
        "price",
        f.col("rating.rate").alias("rating_avg"),
        f.col("rating.count").alias("rating_count"),
        "title",
    )


# ---------------------------------------------------------------------------
# Fact: orders
# ---------------------------------------------------------------------------

FACT_ORDERS_COLUMNS = [
    "order_line_id",
    "invoice_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "timestamp",
]

_ORDERS_METADATA_COLUMNS = [
    "_silver_source",
    "_processed_at",
    "email_hash",
    "ip_address_hash",
]


def _stage_orders(orders: DataFrame) -> DataFrame:
    """Common cleanup applied to both batch and stream orders sources.

    Casts ``product_id`` to ``string`` (so batch and stream agree on type),
    drops pipeline metadata and PII hash columns, and aliases
    ``customer_id_hash`` to ``customer_id`` to match the dimension key.
    """
    return (
        orders.withColumn("product_id", f.col("product_id").cast("string"))
        .withColumnRenamed("customer_id_hash", "customer_id")
        .drop(*_ORDERS_METADATA_COLUMNS)
    )


def build_fact_orders(
    silver_orders_batch: DataFrame, silver_orders_stream: DataFrame
) -> DataFrame:
    """Stage, union, and enrich orders into the ``fact_orders`` table.

    Grain: one row per ``order_line_id``. Foreign keys: ``customer_id``,
    ``product_id``. Recomputes ``total_amount`` as ``quantity * unit_price``
    in Gold, overwriting whatever value (if any) came from Silver, so Gold
    is the single source of truth for that metric.
    """
    batch = _stage_orders(silver_orders_batch)
    stream = _stage_orders(silver_orders_stream)

    orders = batch.unionByName(stream).withColumn(
        "total_amount", f.col("quantity") * f.col("unit_price")
    )

    return orders.select(*FACT_ORDERS_COLUMNS)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

SILVER_PATHS = {
    "customers": "s3a://lakehouse/silver/customers",
    "products": "s3a://lakehouse/silver/products_catalog",
    "orders_batch": "s3a://lakehouse/silver/orders_batch",
    "orders_stream": "s3a://lakehouse/silver/orders_stream",
}

GOLD_PATHS = {
    "dim_customers": "s3a://lakehouse/gold/dim_customers",
    "dim_products": "s3a://lakehouse/gold/dim_products",
    "fact_orders": "s3a://lakehouse/gold/fact_orders",
}


def main() -> None:
    """Read Silver tables, build the Gold dims/fact, and write them out."""
    from config.spark_config import get_spark_session

    spark = get_spark_session("GoldLayerBuild")

    silver_customers = spark.read.format("delta").load(SILVER_PATHS["customers"])
    silver_products = spark.read.format("delta").load(SILVER_PATHS["products"])
    silver_orders_batch = spark.read.format("delta").load(SILVER_PATHS["orders_batch"])
    silver_orders_stream = spark.read.format("delta").load(
        SILVER_PATHS["orders_stream"]
    )

    dim_customers = build_dim_customers(silver_customers)
    dim_products = build_dim_products(silver_products)
    fact_orders = build_fact_orders(silver_orders_batch, silver_orders_stream)

    dim_customers.write.format("delta").mode("overwrite").save(
        GOLD_PATHS["dim_customers"]
    )
    dim_products.write.format("delta").mode("overwrite").save(
        GOLD_PATHS["dim_products"]
    )
    fact_orders.write.format("delta").mode("overwrite").save(GOLD_PATHS["fact_orders"])

    print("✅ Gold layer built: dim_customers, dim_products, fact_orders")


if __name__ == "__main__":
    main()