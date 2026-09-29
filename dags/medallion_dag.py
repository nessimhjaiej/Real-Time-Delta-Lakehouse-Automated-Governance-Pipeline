from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from src.pipelines.bronze import ingest_batch_sources
from src.pipelines.gold import run_gold_pipeline
from src.pipelines.quality import run_quality_gate
from src.pipelines.silver import run_silver_batch_pipeline
from src.utils.callbacks import log_quality_gate_failure

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Dataset lineage: paired with the OpenLineage Spark listener
# (config/spark_config.py), these give Marquez both the DAG-level
# orchestration view (via these inlets/outlets) and the query-level Delta
# read/write view (via Spark) for the same tables.
BRONZE_ORDERS_BATCH = Dataset("s3a://lakehouse/bronze/orders_batch")
BRONZE_PRODUCTS = Dataset("s3a://lakehouse/bronze/products_catalog")
BRONZE_CUSTOMERS = Dataset("s3a://lakehouse/bronze/customers")

SILVER_ORDERS_BATCH = Dataset("s3a://lakehouse/silver/orders_batch")
SILVER_ORDERS_STREAM = Dataset("s3a://lakehouse/silver/orders_stream")
SILVER_PRODUCTS = Dataset("s3a://lakehouse/silver/products_catalog")
SILVER_CUSTOMERS = Dataset("s3a://lakehouse/silver/customers")

GOLD_DIM_CUSTOMERS = Dataset("s3a://lakehouse/gold/dim_customers")
GOLD_DIM_PRODUCTS = Dataset("s3a://lakehouse/gold/dim_products")
GOLD_FACT_ORDERS = Dataset("s3a://lakehouse/gold/fact_orders")

SILVER_INLETS = [
    SILVER_ORDERS_BATCH,
    SILVER_ORDERS_STREAM,
    SILVER_PRODUCTS,
    SILVER_CUSTOMERS,
]

with DAG(
    dag_id="medallion_pipeline",
    description="Bronze -> Silver -> Gold ",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "spark", "delta", "great_expectations"],
) as dag:

    bronze_layer = PythonOperator(
        task_id="bronze_layer",
        python_callable=ingest_batch_sources,
        # MinIO/network can be transiently flaky right after the stack comes
        # up; more, shorter-spaced attempts than the DAG default.
        retries=3,
        retry_delay=timedelta(minutes=2),
        outlets=[BRONZE_ORDERS_BATCH, BRONZE_PRODUCTS, BRONZE_CUSTOMERS],
    )

    silver_layer = PythonOperator(
        task_id="silver_layer",
        python_callable=run_silver_batch_pipeline,
        inlets=[BRONZE_ORDERS_BATCH, BRONZE_PRODUCTS, BRONZE_CUSTOMERS],
        outlets=[SILVER_ORDERS_BATCH, SILVER_PRODUCTS, SILVER_CUSTOMERS],
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_quality_gate,
        # A GX failure means the data is genuinely bad, not transient --
        # retrying just delays the alert.
        retries=0,
        on_failure_callback=log_quality_gate_failure,
        inlets=SILVER_INLETS,
    )

    gold_layer = PythonOperator(
        task_id="gold_layer",
        python_callable=run_gold_pipeline,
        inlets=SILVER_INLETS,
        outlets=[GOLD_DIM_CUSTOMERS, GOLD_DIM_PRODUCTS, GOLD_FACT_ORDERS],
    )

    bronze_layer >> silver_layer >> quality_gate >> gold_layer
