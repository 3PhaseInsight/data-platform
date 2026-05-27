# data-platform-api

Customer-facing HTTP API for the 3PhaseInsight Data Platform.

## v1 endpoint

```
GET /v1/data-apps/{data_app}/meters/{meter_id}/results/latest
Header: X-API-Key: <key>
```

Returns the most recent `run_result` rows that the named Data App produced
for the given meter, identified by the run with the latest `created_at`.

**Important — v1 only:** `{data_app}` is passed through as a literal Airflow
`dag_id`. Today there is no alias layer, so callers must use the internal DAG
identifiers (e.g. `sm_classifier`, `stat_labeler`). A friendlier alias layer
is planned for a later version; the path segment is already named generically
(`data-app`) so the URL contract will not need to break when it ships.

## Running locally

When the service runs inside the data-platform docker-compose stack, its
env vars come from the top-level `.env` file (`docker compose --env-file
.env ...`). Add these to `.env` to configure the API container:

```
# data-platform-api
API_KEYS=dev-key            # comma-separated list of accepted X-API-Key values
API_PORT=8000               # host port the API binds to (container port stays 8000)
```

Note: `dev.env` is read by the Makefile and only feeds `FRAMEWORK_PATH` for
the local-wheel build. It is not propagated to compose, so `API_KEYS` does
not belong there.

To run uvicorn directly (without docker) for quick iteration:

```bash
DB_USER=... DB_PASSWORD=... DB_HOST=... DB_PORT=... DB_NAME=... \
  API_KEYS=dev-key \
  uvicorn data_platform_api.main:app --port 8000
```

Swagger UI is served at `/docs` once the server is running.

## Tests

```bash
pip install -e .[dev]
DB_USER=... DB_PASSWORD=... DB_HOST=... DB_PORT=... DB_NAME=... \
  pytest -v
```

Tests touch a real Postgres (the platform's dev DB or a disposable container)
because `run_result` uses JSONB and Postgres-specific enums. If the DB env
vars are not set, integration tests are skipped.

## Regenerating the OpenAPI snapshot

`tests/openapi.json` is checked in as a contract snapshot. When the API surface
changes intentionally, regenerate it:

```bash
python -c "import json; from data_platform_api.main import create_app; \
  print(json.dumps(create_app().openapi(), indent=2))" > tests/openapi.json
```

## See also

- DB migration: `../data-platform-infrastructure/sqitch/deploy/09_run_result_created_at.sql`
