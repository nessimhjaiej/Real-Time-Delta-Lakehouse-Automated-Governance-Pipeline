# Handy commands for realtime-governance-engine.   Run `make` (or `make help`) to list them.
#
# Needs GNU make. Windows: `winget install ezwinports.make` or `choco install make`,
# then fully restart VS Code (a new terminal alone does not pick up the new PATH).
# Recipes use only make + python, so they work in PowerShell, cmd and Git Bash.

# Pick the project's virtualenv if there is one, else whatever python is on PATH.
# Override any time:  make test PY=python3
ifneq ($(wildcard venv/Scripts/python.exe),)
PY ?= "venv/Scripts/python.exe"
else ifneq ($(wildcard .venv/Scripts/python.exe),)
PY ?= ".venv/Scripts/python.exe"
else ifneq ($(wildcard venv/bin/python),)
PY ?= "venv/bin/python"
else ifneq ($(wildcard .venv/bin/python),)
PY ?= ".venv/bin/python"
else
PY ?= python
endif

COMPOSE   ?= docker compose
CI_PATHS  := config/ src/ tests/
TOOLKIT   := reports/toolkit

.DEFAULT_GOAL := help

.PHONY: help
help: ## List the commands
	@$(PY) -c "import re; [print('  ' + m.group(1).ljust(18) + m.group(2)) for m in (re.match(r'([a-zA-Z0-9_.-]+):.*## (.*)', l) for l in open('Makefile')) if m]"

# ---------------------------------------------------------------- setup
.PHONY: install
install: ## Install Python dependencies and the pre-commit hook
	$(PY) -m pip install -r requirements.txt
	$(PY) -m pre_commit install

# ------------------------------------------------------- code quality
.PHONY: fmt
fmt: ## Auto-format (black) and auto-fix lint (ruff)
	$(PY) -m black $(CI_PATHS)
	$(PY) -m ruff check --fix $(CI_PATHS)

.PHONY: lint
lint: ## Same lint checks as CI: black --check, ruff, mypy
	$(PY) -m black --check $(CI_PATHS)
	$(PY) -m ruff check $(CI_PATHS)
	$(PY) -m mypy config/ src/

.PHONY: test
test: ## Unit tests (Qdrant/MinIO tests skip themselves if those aren't up)
	$(PY) -m pytest tests/

.PHONY: test-fast
test-fast: ## Tests without the slow Spark-based ones
	$(PY) -m pytest tests/ --ignore=tests/unit/test_pipeline_validation.py --ignore=tests/unit/test_quality.py

.PHONY: check
check: lint test ## Everything CI runs: lint + tests

# ------------------------------------------------------------- stack
.PHONY: up
up: ## Start the whole docker stack (Airflow, Kafka, MinIO, Marquez, Qdrant, streaming)
	$(COMPOSE) up -d --build

.PHONY: up-min
up-min: ## Start only what the agent needs: MinIO + Qdrant (light on RAM)
	$(COMPOSE) up -d minio qdrant

.PHONY: down
down: ## Stop the stack (data volumes are kept)
	$(COMPOSE) down

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Follow logs, e.g. make logs S=streaming-silver
	$(COMPOSE) logs -f --tail=100 $(S)

# ----------------------------------------------------------- pipeline
.PHONY: seed
seed: ## Regenerate the raw source files (orders CSV, products API JSON)
	$(PY) -m src.utils.seed_data
	$(PY) -m src.utils.fetch_api_data

.PHONY: bronze silver gold quality
bronze: ## Run the Bronze batch ingest by hand (needs MinIO)
	$(PY) -m src.pipelines.bronze
silver: ## Run the Silver batch pipeline by hand
	$(PY) -m src.pipelines.silver
gold: ## Run the Gold pipeline by hand
	$(PY) -m src.pipelines.gold
quality: ## Run the Great Expectations quality gate on Silver (slow: starts Spark)
	$(PY) -m src.pipelines.quality

.PHONY: dag
dag: ## Trigger the Airflow medallion DAG (needs `make up`)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger medallion_pipeline

.PHONY: producer
producer: ## Start the Kafka order producer (needs Kafka up)
	$(PY) -m src.utils.kafka_producer

# -------------------------------------------------------------- agent
.PHONY: ingest
ingest: ## Embed metrics + policy docs into Qdrant (needs Qdrant + OPENAI_API_KEY)
	$(PY) -m src.governance_agent.rag.ingest

.PHONY: reingest
reingest: ## Same, but wipe and rebuild the collection first
	$(PY) -m src.governance_agent.rag.ingest --overwrite

.PHONY: api
api: ## Run the API + web UI at http://127.0.0.1:8000 (needs MinIO, Qdrant, .env)
	$(PY) -m uvicorn src.governance_agent.api.main:app --reload

.PHONY: eval
eval: ## Eval harness with a mocked LLM (free, deterministic)
	$(PY) -m tests.eval.run_eval

.PHONY: eval-real
eval-real: ## Eval harness with the REAL model (costs a little OpenAI money)
	$(PY) -m tests.eval.run_eval --real

# ------------------------------------------------- reports and site
.PHONY: toolkit
toolkit: ## Build the DE toolkit site (reuses live snapshots); needs MinIO for new data
	$(PY) -m toolkit.build
	@echo "Open $(TOOLKIT)/index.html"

.PHONY: toolkit-refresh
toolkit-refresh: ## Rebuild the site re-reading MinIO/docker/Spark (quality gate takes ~5 min)
	$(PY) -m toolkit.build --refresh

.PHONY: profile
profile: ## Profile a table to HTML, e.g. make profile T=silver/orders_stream KEY=order_line_id
	$(if $(T),,$(error usage: make profile T=layer/table  and optionally KEY=column))
	$(PY) .claude/skills/data-profiling/scripts/profile_table.py s3a://lakehouse/$(T) \
		$(if $(KEY),--key $(KEY),) --html reports/data-profiling/$(subst /,_,$(T)).html

# -------------------------------------------------------------- misc
.PHONY: clean
clean: ## Remove caches (keeps reports/ and your data)
	$(PY) -c "import pathlib,shutil; [shutil.rmtree(p, ignore_errors=True) for pat in ('__pycache__','.pytest_cache','.mypy_cache','.ruff_cache') for p in pathlib.Path('.').rglob(pat) if 'venv' not in p.parts and '.venv' not in p.parts]"
