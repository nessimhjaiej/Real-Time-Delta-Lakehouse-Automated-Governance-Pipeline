"""Silver layer cleansing and governance checks."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.streaming import StreamingQuery

from config.spark_config import get_spark_session

logger = logging.getLogger(__name__)

BRONZE_BATCH_PATH = "s3a://lakehouse/bronze/orders_batch"
BRONZE_STREAM_PATH = "s3a://lakehouse/bronze/orders_stream"
BRONZE_PRODUCTS_PATH = "s3a://lakehouse/bronze/products_catalog"
SILVER_BATCH_PATH = "s3a://lakehouse/silver/orders_batch"
SILVER_STREAM_PATH = "s3a://lakehouse/silver/orders_stream"
SILVER_PRODUCTS_PATH = "s3a://lakehouse/silver/products_catalog"
SILVER_STREAM_CHECKPOINT = "s3a://lakehouse/silver/_checkpoints/orders_stream"


def cleanse(df: DataFrame, source_name: str, is_streaming: bool = False) -> DataFrame:
    """Deduplicate orders, protect PII, enforce types, and add Silver metadata."""

    # Standardize event timestamp field
    df = df.withColumn("timestamp", f.col("timestamp").cast("timestamp"))

    # Stateful Deduplication
    if is_streaming:
        # Watermarking is mandatory for streaming dropDuplicates
        df = df.withWatermark("timestamp", "10 minutes").dropDuplicates(
            ["invoice_id", "product_id", "timestamp"]
        )
    else:
        df = df.dropDuplicates(["invoice_id", "product_id", "timestamp"])

    return (
        df.withColumn(
            "order_line_id",
            f.concat_ws("_", f.col("invoice_id"), f.col("product_id")),
        )
        .withColumn("quantity", f.col("quantity").cast("integer"))
        .withColumn("unit_price", f.col("unit_price").cast("double"))
        .withColumn("total_amount", f.round(f.col("quantity") * f.col("unit_price"), 2))
        .withColumn("customer_id_hash", f.sha2(f.col("customer_id"), 256))
        .withColumn("email_hash", f.sha2(f.lower(f.trim(f.col("email"))), 256))
        .withColumn("ip_address_hash", f.sha2(f.col("ip_address"), 256))
        .drop("customer_id", "email", "ip_address")
        .withColumn("_silver_source", f.lit(source_name))
        .withColumn("_processed_at", f.current_timestamp())
    )


def cleanse_products(df: DataFrame) -> DataFrame:
    """Remove duplicate catalog entries and add Silver metadata."""
    return (
        df.dropDuplicates(["id"])
        .withColumn("price", f.col("price").cast("double"))
        .withColumn("_processed_at", f.current_timestamp())
    )


def write_batch_silver(spark: SparkSession) -> None:
    """Transform Bronze batch orders and products, then overwrite Silver tables."""
    batch_orders = cleanse(
        spark.read.format("delta").load(BRONZE_BATCH_PATH),
        source_name="uci_batch_csv",
        is_streaming=False,
    )
    products = cleanse_products(spark.read.format("delta").load(BRONZE_PRODUCTS_PATH))

    batch_orders.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(SILVER_BATCH_PATH)

    products.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(SILVER_PRODUCTS_PATH)

    logger.info("Silver batch orders and product catalog written successfully.")


def start_streaming_silver(spark: SparkSession) -> StreamingQuery:
    """Start the Bronze-to-Silver order stream and return its active query."""
    stream_orders = cleanse(
        spark.readStream.format("delta").load(BRONZE_STREAM_PATH),
        source_name="kafka_stream",
        is_streaming=True,
    )

    return (
        stream_orders.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", SILVER_STREAM_CHECKPOINT)
        .start(SILVER_STREAM_PATH)
    )


def run_silver_pipeline() -> StreamingQuery:
    """Run batch Silver transformations and start the Silver streaming query."""
    spark = get_spark_session("SilverTransformation")
    write_batch_silver(spark)
    query = start_streaming_silver(spark)
    logger.info("Silver streaming query started: %s", query.id)
    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_silver_pipeline().awaitTermination()
