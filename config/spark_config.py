import logging
import os
import sys
import os
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "RealtimeGovernanceEngine") -> SparkSession:
    """
    Constructs or binds to a cloud-agnostic PySpark Session.

    Environment Control:
    - EXECUTION_ENV="local": Configures Delta Lake 3.x, S3A (MinIO), and
      Kafka connector JARs.
    - EXECUTION_ENV="databricks": Binds to the active Databricks managed
      SparkContext & Unity Catalog.
    """
    env = os.getenv("EXECUTION_ENV", "local").lower().strip()
    logger.info(f"Initializing Spark Factory in '{env}' mode...")

    if env == "databricks":
        # Databricks manages Spark & Unity Catalog natively
        try:
            spark = (
                SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            )
            logger.info("Successfully attached to active Databricks SparkSession.")
            return spark
        except Exception as e:
            logger.error(f"Failed to bind to Databricks execution environment: {e}")
            raise RuntimeError("Databricks SparkSession unavailable.") from e

    elif env == "local":
            minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
            minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")

            builder = (
                SparkSession.builder.appName(app_name)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config(
                    "spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                )
                # MinIO (S3-compatible) Connection Settings
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
                # 👇 CRITICAL FIX: Forces S3A to use the explicit keys provided above
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )

            spark = builder.getOrCreate()
            logger.info(f"Local PySpark Session active. Spark Version: {spark.version}")
            return spark

    else:
        raise ValueError(
            f"Invalid EXECUTION_ENV='{env}'. "
            "Supported values are 'local' or 'databricks'."
        )


if __name__ == "__main__":
    # Smoke test for local execution
    os.environ["EXECUTION_ENV"] = "local"
    spark_session = get_spark_session()

    print("\n--- Spark Environment Diagnostics ---")
    print(f"Spark Version: {spark_session.version}")
    print(f"Application Name: {spark_session.sparkContext.appName}")
    print(f"Master: {spark_session.sparkContext.master}")
