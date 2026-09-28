import logging
import os
from pathlib import Path

from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

from config.spark_config import get_spark_session
from src.utils.extractors import ExtractorFactory
from src.utils.sinks import SinkFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Raw Kafka JSON Schema definition
STREAM_ORDER_SCHEMA = (
    StructType()
    .add("invoice_id", StringType())
    .add("customer_id", StringType())
    .add("customer_name", StringType())
    .add("email", StringType())
    .add("ip_address", StringType())
    .add("product_id", StringType())
    .add("quantity", IntegerType())
    .add("unit_price", DoubleType())
    .add("timestamp", StringType())
)


def ingest_batch_sources() -> None:
    """Ingests historical CSV transactions and REST API catalog
    JSON into Bronze Delta tables."""
    spark = get_spark_session("BronzeBatchIngestion")

    try:
        csv_path = str(PROJECT_ROOT / "data" / "raw_transactions" / "orders_batch.csv")
        logger.info("Extracting batch transactions from %s...", csv_path)
        raw_csv_df = ExtractorFactory.create("csv", csv_path).extract(spark)
        bronze_csv_df = raw_csv_df.withColumn(
            "_ingested_at", f.current_timestamp()
        ).withColumn("_source_system", f.lit("uci_batch_csv"))
        # Overwrite: the source is a full static snapshot, so appending on every
        # scheduled run would duplicate the whole file in Bronze each day.
        SinkFactory.create(
            "delta", "s3a://lakehouse/bronze/orders_batch", mode="overwrite"
        ).write(bronze_csv_df)

        api_path = str(PROJECT_ROOT / "data" / "raw_transactions" / "products_api.json")
        logger.info("Extracting product catalog from %s...", api_path)
        raw_api_df = ExtractorFactory.create("api", api_path, multiline="true").extract(
            spark
        )
        bronze_api_df = raw_api_df.withColumn(
            "_ingested_at", f.current_timestamp()
        ).withColumn("_source_system", f.lit("fakestore_api"))
        SinkFactory.create(
            "delta", "s3a://lakehouse/bronze/products_catalog", mode="overwrite"
        ).write(bronze_api_df)

        customer_path = str(PROJECT_ROOT / "data" / "customers.csv")
        logger.info("Extracting batch customers from %s...", customer_path)
        raw_customers_df = ExtractorFactory.create("csv", customer_path).extract(spark)
        bronze_customers_df = raw_customers_df.withColumn(
            "_ingested_at", f.current_timestamp()
        ).withColumn("_source_system", f.lit("uci_batch_csv"))
        SinkFactory.create(
            "delta", "s3a://lakehouse/bronze/customers", mode="overwrite"
        ).write(bronze_customers_df)
    finally:
        spark.stop()


def ingest_streaming_sources() -> None:
    """Consumes live streaming orders from Kafka and appends directly to MinIO Delta Lake."""
    spark = get_spark_session("BronzeStreamingIngestion")

    bronze_stream_target = "s3a://lakehouse/bronze/orders_stream"
    checkpoint_target = "s3a://lakehouse/bronze/_checkpoints/orders_stream"

    logger.info("Connecting to Kafka topic 'ecommerce.orders.v1'...")
    kafka_extractor = ExtractorFactory.create(
        "kafka",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic="ecommerce.orders.v1",
    )
    raw_kafka_df = kafka_extractor.extract(spark)

    # Deserialize byte payload and unpack JSON schema
    parsed_stream_df = (
        raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(f.from_json(f.col("json_payload"), STREAM_ORDER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("_ingested_at", f.current_timestamp())
        .withColumn("_source_system", f.lit("kafka_stream"))
    )

    logger.info(f"Writing streaming records to {bronze_stream_target}...")
    query = (
        parsed_stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_target)
        .start(bronze_stream_target)
    )

    query.awaitTermination()


def run_bronze_pipeline() -> None:
    """Executes full Bronze ingestion suite."""
    logger.info("Starting Bronze Ingestion Pipeline...")
    ingest_batch_sources()
    logger.info("Bronze Batch Ingestion complete.")
    ingest_streaming_sources()
    logger.info("Bronze Streaming Ingestion complete.")


if __name__ == "__main__":
    run_bronze_pipeline()
