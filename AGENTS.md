# AGENTS.md — 3Phi Data Platform

Guidance for AI coding agents working in this repository.

## What This Project Does

The **3-Phase-Insight (3Phi) Data Platform** orchestrates smart meter data processing for LV (low-voltage) electrical network analysis. It ingests CSV meter data into Parquet, validates network topology, classifies smart meters, and labels heat pump phases using statistical analysis.

**Key components:**
- **Apache Airflow** (task orchestration, Celery executor, Redis broker)
- **Dask** (distributed compute cluster — scheduler + workers)
- **MinIO** (S3-compatible object storage for Parquet files and logs)
- **PostgreSQL** (metadata, topology, and run results)
- **`threephi_framework`** (internal library — provides BaseDataApp, S3Connector, TopologyController, MetaController, DataExtractor, etc.)

## Repository Layout

```
dags/                         # Airflow DAG definitions (one .py per pipeline)
dags/configs/                 # YAML configs loaded by DAGs at runtime
dags/utils.py                 # Shared DAG helpers (e.g. load_dag_config)
plugins/dtu/                  # Domain-specific Python modules imported by DAGs
data-platform-infrastructure/ # PostgreSQL schema (Sqitch migrations)
data-platform-frontend/       # Airflow webserver compose service
Dockerfile.airflow / .dask    # Production Docker images
Dockerfile.airflow.dev / .dask.dev  # Dev images (install local framework wheel)
docker-compose.yml            # Core services (scheduler, worker, dask, redis, postgres)
docker-compose.override.yml   # Local dev overrides (dev images, volume mounts)
.env                          # Runtime secrets and service config
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

- Inherit from `threephi_framework.BaseDataApp` (or a subclass like `TimeseriesIngestor`)
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

## Plugin Code (`plugins/dtu/`)

Custom domain logic lives here. These modules are imported by DAGs as:

```python
from dtu.sm_classifier import _meter_evaluation
```

Airflow automatically adds `/plugins` to `sys.path`. Restart containers after modifying plugin code (`make down && make up`).

## Environment Variables

Runtime config comes from `.env`. Never hardcode credentials. Key variables:

| Variable | Purpose |
|---|---|
| `DB_*` | PostgreSQL connection |
| `MINIO_ROOT_USER/PASSWORD` | MinIO credentials |
| `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY` | S3-compatible access (mapped from MinIO) |
| `AIRFLOW__API__SECRET_KEY` | Airflow API auth |
| `REDIS_PASSWORD` | Celery broker auth |

## Testing & Validation

There is no standalone test suite. Validation is done by running DAGs directly:

- `test_topology` DAG — validates topology data integrity
- `test_and_time_workflows` DAG — benchmarks data access patterns

To verify a DAG appears in Airflow after changes, run `airflow dags reserialize` in the webserver container (Docker Desktop → airflow-webserver → Exec).

## CI/CD

GitLab CI (`.gitlab-ci.yml`) builds `Dockerfile.airflow` and `Dockerfile.dask` and pushes to the registry:
- Triggers automatically on commits to `main`
- Requires manual trigger on merge requests
- Uses a BuildKit secret for private PyPI access (`3phi-framework`)

When modifying `requirements.txt` or either Dockerfile, ensure the CI build will still resolve `3phi-framework` from the private index.

## Common Tasks

**Add a new pipeline:**
1. Create `dags/your_dag.py` — follow the canonical pattern above, use `load_dag_config(__file__)`
2. Create `dags/configs/your_dag_config.yaml`
3. `make down && make up`, then reserialize DAGs in the Airflow webserver

**Modify plugin code:**
1. Edit `plugins/dtu/*.py`
2. `make down && make up` (changes are picked up via volume mount in dev, or image rebuild in prod)

**Update Python dependencies:**
1. Edit `requirements.txt`
2. `make up` rebuilds the images

**Inspect a running DAG:**
- Airflow UI: http://localhost:8080
- Dask dashboard: http://localhost:8787
- Container logs: `make logs`
