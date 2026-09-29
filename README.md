# Realtime Governance Engine

A Python/Spark foundation for governed medallion ETL pipelines.

## Architecture

- **Bronze** preserves source data and adds ingestion metadata.
- **Silver** validates required fields and removes duplicate records.
- **Gold** exposes business-ready aggregates.
- **Utilities** provide factory-backed extractor and sink implementations.

## Local setup

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
docker compose up -d
pytest
```

Airflow is intended to run through Docker on Windows (or WSL2), not directly in
the Windows virtual environment. Start the stack from the repository root:

```bash
docker compose up -d --build
docker compose ps
```

Airflow: `http://localhost:8080` (user `airflow`, password `airflow`)  
MinIO console: `http://localhost:9001` (user `minioadmin`, password `minioadminpassword`)  
Marquez (lineage UI): `http://localhost:3000`

Lineage is emitted by two layers into the same Marquez backend: the OpenLineage
Spark listener (`config/spark_config.py`) reports every Delta table a Spark job
actually reads/writes, and Airflow's `apache-airflow-providers-openlineage`
reports DAG/task-level runs using the `Dataset` inlets/outlets declared in
`dags/medallion_dag.py`. Both are fire-and-forget -- if Marquez is down, jobs
still run, lineage just doesn't get recorded.

The Airflow DAG runs the finite batch path:
`Bronze batch -> Silver batch -> Gold`. Kafka Bronze and Delta Silver streaming
run independently as `streaming-bronze` and `streaming-silver`.

Trigger a run from the Airflow UI, or with:

```bash
docker compose exec airflow-scheduler airflow dags trigger medallion_pipeline
```

## Quality checks

```bash
ruff check .
black --check .
mypy src config
pytest
```
Challenges : challenge setting up conflicting packages (uncompatible versions)
spark session should be closed before using it in the jupyter notebook 
Managing Local Environment & Package Compatibility
Installing airflow with docker and actually running the DAG was harder than writing the DAG code itself 
Used AI agent (model used = gpt5.6 luna)  :
setup my docker files for airflow and corrected my dockercompose yaml 
suggesting seperating streaming and batch from my pipelines 
