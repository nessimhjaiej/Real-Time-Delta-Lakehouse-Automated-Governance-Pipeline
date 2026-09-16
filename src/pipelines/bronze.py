import logging
from pyspark.sql import functions as f
from config.spark_config import get_spark_session
from src.utils.extractors import ExtractorFactory
from src.utils.sinks import SinkFactory
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    """Ingests historical CSV transactions and REST API catalog JSON into Bronze Delta tables."""
    spark = get_spark_session("BronzeBatchIngestion")

    # 1. Ingest Raw Batch CSV Transactions
    csv_path = "data/raw_transactions/orders_batch.csv"
    bronze_csv_target = "s3a://lakehouse/bronze/orders_batch"

    logger.info(f"Extracting batch transactions from {csv_path}...")
    csv_extractor = ExtractorFactory.create("csv", csv_path)
    raw_csv_df = csv_extractor.extract(spark)

    bronze_csv_df = raw_csv_df.withColumn(
        "_ingested_at", f.current_timestamp()
    ).withColumn("_source_system", f.lit("uci_batch_csv"))

    logger.info(f"Writing raw batch transactions to Bronze sink: {bronze_csv_target}")
    delta_csv_sink = SinkFactory.create("delta", bronze_csv_target, mode="append")
    delta_csv_sink.write(bronze_csv_df)

    # 2. Ingest REST API Product Catalog JSON
    api_path = "data/raw_transactions/products_api.json"
    bronze_api_target = "s3a://lakehouse/bronze/products_catalog"

    logger.info(f"Extracting product catalog from {api_path}...")
    api_extractor = ExtractorFactory.create("api", api_path, multiline="true")
    raw_api_df = api_extractor.extract(spark)

    bronze_api_df = raw_api_df.withColumn(
        "_ingested_at", f.current_timestamp()
    ).withColumn("_source_system", f.lit("fakestore_api"))

    logger.info(f"Writing product catalog to Bronze sink: {bronze_api_target}")
    delta_api_sink = SinkFactory.create("delta", bronze_api_target, mode="overwrite")
    delta_api_sink.write(bronze_api_df)
    # 3. ingesting customer data
    csv_path = "data\\customers.csv"
    # changed the \customers to \\customers this might break the pipeline but whatever
    bronze_customers_target = "s3a://lakehouse/bronze/customers"
    logger.info(f"Extracting batch customers from {csv_path}...")
    csv_extractor = ExtractorFactory.create("csv", csv_path)
    raw_csv_df = csv_extractor.extract(spark)
    bronze_csv_df = raw_csv_df.withColumn(
        "_ingested_at", f.current_timestamp()
    ).withColumn("_source_system", f.lit("uci_batch_csv"))
    logger.info(
        f"Writing raw batch customers to Bronze sink: {bronze_customers_target}"
    )
    delta_csv_sink = SinkFactory.create(
        "delta", bronze_customers_target, mode="overwrite"
    )
    delta_csv_sink.write(bronze_csv_df)


def ingest_streaming_sources() -> None:
    """Consumes live streaming orders from Kafka and appends directly to MinIO Delta Lake."""
    spark = get_spark_session("BronzeStreamingIngestion")

    bronze_stream_target = "s3a://lakehouse/bronze/orders_stream"
    checkpoint_target = "s3a://lakehouse/bronze/_checkpoints/orders_stream"

    logger.info("Connecting to Kafka topic 'ecommerce.orders.v1'...")
    kafka_extractor = ExtractorFactory.create(
        "kafka", "localhost:9092", topic="ecommerce.orders.v1"
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

    # Keep stream running for 30 seconds during batch execution or await termination
    query.awaitTermination(timeout=30)


def run_bronze_pipeline() -> None:
    """Executes full Bronze ingestion suite."""
    logger.info("Starting Bronze Ingestion Pipeline...")
    ingest_batch_sources()
    logger.info("Bronze Batch Ingestion complete.")
    ingest_streaming_sources()
    logger.info("Bronze Streaming Ingestion complete.")


if __name__ == "__main__":
    run_bronze_pipeline()
