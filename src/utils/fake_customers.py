import csv
import logging
from pathlib import Path

from faker import Faker

from config.spark_config import get_spark_session

logger = logging.getLogger(__name__)
fake = Faker()

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "customers.csv"

BRONZE_BATCH_PATH = "s3a://lakehouse/bronze/orders_batch"
BRONZE_STREAM_PATH = "s3a://lakehouse/bronze/orders_stream"


def generate_initial_customers() -> None:
    spark = get_spark_session("GenerateInitialCustomers")
    logger.info("Spark session initialized. Reading from Bronze MinIO...")

    bronze_batch = spark.read.format("delta").load(BRONZE_BATCH_PATH)
    bronze_stream = spark.read.format("delta").load(BRONZE_STREAM_PATH)

    batch_count = bronze_batch.count()
    stream_count = bronze_stream.count()
    logger.info(f"Bronze batch rows: {batch_count} | Bronze stream rows: {stream_count}")
    if batch_count == 0 and stream_count == 0:
        logger.warning("Both bronze sources are empty — output will be empty too.")

    combined_orders = bronze_batch.select("customer_id", "email", "ip_address").union(
        bronze_stream.select("customer_id", "email", "ip_address")
    )

    unique_customers = combined_orders.filter(
        combined_orders.customer_id.isNotNull()
    ).dropDuplicates(["customer_id"])

    rows = unique_customers.collect()
    logger.info(f"Collected {len(rows)} unique customers to driver for enrichment.")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(
            ["customer_id", "full_name", "email", "ip_address", "country", "first_sign_up"]
        )
        for row in rows:
            writer.writerow(
                [
                    row["customer_id"],
                    fake.name(),
                    row["email"],
                    row["ip_address"],
                    fake.country(),
                    fake.date_between(start_date="-2y", end_date="today"),
                ]
            )

    logger.info(f"✅ Initial customer data generated successfully at '{OUTPUT_FILE}'")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_initial_customers()