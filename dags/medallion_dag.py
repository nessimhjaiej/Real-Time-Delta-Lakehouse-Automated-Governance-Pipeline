from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipelines.bronze import ingest_batch_sources
from src.pipelines.gold import run_gold_pipeline
from src.pipelines.quality import run_quality_gate
from src.pipelines.silver import run_silver_batch_pipeline

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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
    )

    silver_layer = PythonOperator(
        task_id="silver_layer",
        python_callable=run_silver_batch_pipeline,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_quality_gate,
    )

    gold_layer = PythonOperator(
        task_id="gold_layer",
        python_callable=run_gold_pipeline,
    )

    bronze_layer >> silver_layer >> quality_gate >> gold_layer
