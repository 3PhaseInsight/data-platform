# AGENTS.md — 3Phi Data Platform

Guidance for AI coding agents working in this repository.

## What This Project Does

The **3-Phase-Insight (3Phi) Data Platform** orchestrates smart meter data processing for LV (low-voltage) electrical network analysis. It ingests CSV meter data into Parquet, validates network topology, classifies smart meters, and labels heat pump phases using statistical analysis.

**Key components:**
- **Apache Airflow** (task orchestration, Celery executor, Redis broker)
- **Dask** (distributed compute cluster — scheduler + workers)
- **MinIO** (S3-compatible object storage for Parquet files and logs)
- **PostgreSQL** (metadata, topology, and run results)
- **data-platform-api** (customer-facing FastAPI service for querying Data App results)
- **`threephi_framework`** (internal library — provides BaseDataApp, S3Connector, TopologyController, MetaController, DataExtractor, etc.)

## Repository Layout

```
dags/                         # Airflow DAG definitions (one .py per pipeline)
dags/configs/                 # YAML configs loaded by DAGs at runtime
plugins/utils.py              # Shared DAG helpers (e.g. load_dag_config)
data-platform-infrastructure/ # PostgreSQL schema (Sqitch migrations) + Postgres/MinIO compose services
data-platform-frontend/       # Airflow webserver compose service
data-platform-api/            # FastAPI service (own pyproject, tests, README)
Dockerfile.airflow / .dask / .api   # Production Docker images
Dockerfile.*.dev              # Dev images (install local framework wheel)
docker-compose.yml            # Core services (scheduler, worker, dask, redis, api)
docker-compose.override.yml   # Local dev overrides (dev images, volume mounts)
.env                          # Service config; committed values are local-dev defaults only
dev.env                       # Optional: local framework dev path (see below)
requirements.txt              # Python dependencies for Airflow/Dask images
```

## Development Environment

### Standard setup (no local framework changes)

```bash
make up      # Build images and start all services
make down    # Stop all services
make logs    # Stream container logs
make ps      # List running containers
```

`make up` works out of the box as long as `requirements.txt` contains a `3phi-framework` entry pointing to a reachable package.

### Local framework development (optional)

If you need to iterate on `3phi-framework` locally:

1. Clone `3phi-framework` as a sibling repo (`../3phi-framework` by default)
2. Copy and edit the dev env file:
   ```bash
   cp dev.env.example dev.env   # then set FRAMEWORK_PATH=/path/to/3phi-framework
   ```
3. Comment out the `3phi-framework` line in `requirements.txt` (do **not** commit this change)
4. Run `make up` — it will build a wheel from your local checkout and install it into the dev images

The `dev.env` file is never required for `make up`; it only activates the local-wheel build path.

## Writing DAGs

All DAGs follow the same pattern:

- Import the data app from `threephi_framework` (e.g. `SMClassifier`, `StatLabeler`, `TimeseriesIngestor`) — DAG files contain no domain logic
- Use `load_dag_config(__file__)` from `utils` to load the YAML config — always **inside** the callable, never at module level
- Use `PythonOperator` to wrap each step; keep DAG-level code minimal
- Set `max_active_runs=1` and `catchup=False` on every DAG
- Never access the database or S3 directly — always go through the framework abstractions

The canonical shape of every DAG file:

```python
from utils import load_dag_config

def run_my_pipeline():
    with MyDataApp(config=load_dag_config(__file__)) as app:
        app.run()

with DAG(dag_id="my_pipeline", ...) as dag:
    PythonOperator(task_id="run", python_callable=run_my_pipeline)
```

**Config file convention:** the YAML config name must match the DAG filename (e.g. `sm_classifier.py` → `sm_classifier_config.yaml`). `load_dag_config` derives this automatically from `__file__`.

## Plugin Code (`plugins/`)

Only shared DAG helpers live here (`plugins/utils.py`, e.g. `load_dag_config`). Airflow automatically adds `/plugins` to `sys.path`. All domain logic lives in `threephi_framework` (its `dtu/` package) — change it there and bump/rebuild the framework dependency instead of adding platform-side modules.

## Environment Variables

Runtime config comes from `.env`. The committed `.env` holds local-dev defaults only; real deployments override every secret. Never hardcode credentials in code or compose files. Key variables:

| Variable | Purpose |
|---|---|
| `DB_*` | PostgreSQL connection |
| `MINIO_ROOT_USER/PASSWORD` | MinIO credentials |
| `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY` | S3-compatible access (mapped from MinIO) |
| `AIRFLOW__API__SECRET_KEY` | Airflow API auth |
| `REDIS_PASSWORD` | Celery broker auth |
| `API_KEYS` / `API_PORT` | data-platform-api auth keys and host port |
| `OBJECT_STORAGE_BACKEND` | Optional: object-storage backend for data apps ("s3" default, or "azure"); can also be set per-DAG via `object_storage_backend` in the config YAML |

## Testing & Validation

`data-platform-api/tests/` is a pytest suite (auth, error envelope, OpenAPI snapshot, and DB-backed route tests — the DB-backed tests skip when `DB_*` env vars are unset). See `data-platform-api/README.md`.

The DAG layer has no standalone test suite. Validation is done by running DAGs directly:

- `test_topology` DAG — validates topology data integrity
- `test_and_time_workflows` DAG — benchmarks data access patterns

To verify a DAG appears in Airflow after changes, run `airflow dags reserialize` in the webserver container (Docker Desktop → airflow-webserver → Exec).

## CI/CD

There is no CI pipeline. Images are built locally (or on the target host) — see "Custom Docker Images" in the README. `3phi-framework` is published on public PyPI, so no private index or build secret is required.

## Common Tasks

**Add a new pipeline:**
1. Create `dags/your_dag.py` — follow the canonical pattern above, use `load_dag_config(__file__)`
2. Create `dags/configs/your_dag_config.yaml`
3. `make down && make up`, then reserialize DAGs in the Airflow webserver

**Modify domain logic (data apps / dtu):**
1. Edit it in the `3phi-framework` repo (`src/threephi_framework/...`)
2. Use the local-framework dev flow (`dev.env` + `FRAMEWORK_PATH`) and `make up` to rebuild the dev images with your local wheel

**Update Python dependencies:**
1. Edit `requirements.txt`
2. `make up` rebuilds the images

**Inspect a running DAG:**
- Airflow UI: http://localhost:8080
- Dask dashboard: http://localhost:8787
- Container logs: `make logs`
