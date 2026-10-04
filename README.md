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

## Phase 5: governed natural-language analytics agent

A user asks a business question in plain English; the agent answers with
numbers that come only from executed, validated SQL against the Gold Delta
tables -- never from the LLM. See `semantic/`, `policies/`, and
`src/governance_agent/` for the implementation; the design decisions this
was built against are in that package's module docstrings.

**Setup** (in addition to the stack above):

```bash
docker compose up -d qdrant
cp .env.example .env            # fill in OPENAI_API_KEY
python -m src.governance_agent.rag.ingest   # chunk + embed metrics/policies into Qdrant
```

**Re-embedding after you change rules or metrics.** The agent retrieves from
what was last ingested into Qdrant, so editing a file changes nothing until
you re-embed. No API restart is needed afterwards, since retrieval queries
Qdrant on every request.

1. Edit `policies/docs/*.md` (each rule is one `## Rule: <title>` heading;
   text before the first such heading is ignored) or `semantic/metrics/*.yml`.
2. Make sure Qdrant is up (`docker compose up -d qdrant`) and `.env` has
   `OPENAI_API_KEY`.
3. Re-embed, picking the mode that matches what you changed:

   ```bash
   # Edited a rule/metric, or added a new one: updated in place
   python -m src.governance_agent.rag.ingest

   # Deleted or renamed a rule/metric, or changed the embedding model:
   # drops the collection and rebuilds it from the files
   python -m src.governance_agent.rag.ingest --overwrite
   ```

   Chunk ids come from the file name plus the rule title (`metric:<name>` for
   metrics), so a plain run overwrites edited text but leaves behind the old
   chunk of anything you deleted or renamed. Only `--overwrite` removes those,
   and only `--overwrite` can fix a collection built with a different
   embedding model (its vector size won't match).
4. Check the result: the command prints the chunk count, which should equal
   your number of metrics plus rules, and
   `curl http://localhost:6333/collections/governance_agent` shows
   `points_count` (and the vector `size`, currently 1536).

Each run re-embeds every chunk through the OpenAI embeddings API, which costs
a fraction of a cent for a dozen chunks.

**Run the API:**

```bash
uvicorn src.governance_agent.api.main:app --reload
```

```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -H "X-Role: analyst" \
  -d '{"question": "What is our total revenue by country?"}'
```

A blocked request (PII, an unknown metric, a row limit over the role's
cap, ...) comes back as `403` with the policy citation that
explains why, not a `200` with an empty or wrong answer.

**How a question is answered** (`src/governance_agent/agent/graph.py`,
LangGraph): interpret -> retrieve -> compile -> validate -> execute ->
narrate, with conditional edges to a `blocked` node after compile and
after validate. The LLM only ever picks a metric/dimensions/filters as
structured output (`src/governance_agent/compiler.py` turns that into SQL
via `sqlglot`) and writes a short narration of rows it's handed -- it never
produces a number itself. `src/governance_agent/validator.py` re-parses
and checks the compiled SQL independently before anything executes:
single `SELECT` only, allowlisted tables/columns per role, no PII columns
(`policies/access.yml`), a `LIMIT` within the role's cap, no comments, no
stacked statements.

**Eval harness** (`tests/eval/`): 30+ questions against a small,
hand-computed fixture dataset (not the real Gold tables, so expected
answers stay stable), covering both normal questions and adversarial ones
(PII requests, SQL-injection-shaped question text, prompt injection,
unknown metrics, a nonexistent role). Runs as a normal pytest file in CI
with every LLM call mocked; run it standalone for a report:

```bash
python -m tests.eval.run_eval          # mocked LLM, free, deterministic
python -m tests.eval.run_eval --real   # real OpenAI calls, costs a little, tests actual model judgment
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
