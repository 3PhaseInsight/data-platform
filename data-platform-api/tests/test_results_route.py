import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from data_platform_api.main import create_app


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("API_KEYS", "test-key")
    from data_platform_api import auth

    auth._load_keys.cache_clear()
    return create_app()


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def headers():
    return {"X-API-Key": "test-key"}


def _make_row(*, dag_id, run_id, meter_id, phase, label_value, confidence, created_at):
    from threephi_framework.models.meta.run_result import RunResultModel  # lazy: only needed with DB

    return RunResultModel(
        id=uuid.uuid4(),
        dag_id=dag_id,
        run_id=run_id,
        meter_id=meter_id,
        phase=phase,
        label_type="data_quality",
        label_value=label_value,
        confidence=confidence,
        source="test",
        result={"k": "v"},
        created_at=created_at,
    )


def test_returns_latest_run_rows_for_meter(client, headers, clean_run_result):
    now = datetime.now(UTC)
    older_run = "scheduled__2024-05-12T10:00:00+00:00"
    newer_run = "scheduled__2024-05-13T10:00:00+00:00"

    clean_run_result.add_all(
        [
            _make_row(
                dag_id="sm_classifier",
                run_id=older_run,
                meter_id=42,
                phase="L1",
                label_value="medium",
                confidence=0.5,
                created_at=now - timedelta(days=1),
            ),
            _make_row(
                dag_id="sm_classifier",
                run_id=newer_run,
                meter_id=42,
                phase="L1",
                label_value="good",
                confidence=0.9,
                created_at=now,
            ),
            _make_row(
                dag_id="sm_classifier",
                run_id=newer_run,
                meter_id=42,
                phase="L2",
                label_value="good",
                confidence=0.85,
                created_at=now,
            ),
            _make_row(
                dag_id="sm_classifier",
                run_id=newer_run,
                meter_id=99,
                phase="L1",
                label_value="bad",
                confidence=0.1,
                created_at=now,
            ),
        ]
    )
    clean_run_result.commit()

    r = client.get("/v1/data-apps/sm_classifier/meters/42/results/latest", headers=headers)
    assert r.status_code == 200
    body = r.json()
    assert body["data_app"] == "sm_classifier"
    assert body["meter_id"] == 42
    assert len(body["results"]) == 2

    label_values = {row["label_value"] for row in body["results"]}
    assert label_values == {"good"}


def test_returns_404_when_no_results(client, headers, clean_run_result):
    r = client.get("/v1/data-apps/sm_classifier/meters/42/results/latest", headers=headers)
    assert r.status_code == 404
    assert r.json() == {"error": "no_results", "data_app": "sm_classifier", "meter_id": 42}


def test_returns_401_without_api_key(client, clean_run_result):
    r = client.get("/v1/data-apps/sm_classifier/meters/42/results/latest")
    assert r.status_code == 401
    assert r.json() == {"error": "missing_api_key"}


def test_returns_403_with_wrong_api_key(client, clean_run_result):
    r = client.get(
        "/v1/data-apps/sm_classifier/meters/42/results/latest",
        headers={"X-API-Key": "wrong"},
    )
    assert r.status_code == 403
    assert r.json() == {"error": "invalid_api_key"}
