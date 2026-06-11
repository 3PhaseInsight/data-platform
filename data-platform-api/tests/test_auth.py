import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException as StarletteHTTPException

from data_platform_api.auth import require_api_key
from data_platform_api.errors import http_exception_handler


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("API_KEYS", "key-one,key-two")
    from data_platform_api import auth

    auth._load_keys.cache_clear()

    app = FastAPI()
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)

    @app.get("/protected")
    def protected(_: None = require_api_key()):
        return {"ok": True}

    return TestClient(app)


def test_missing_api_key_returns_401(client):
    r = client.get("/protected")
    assert r.status_code == 401
    assert r.json() == {"error": "missing_api_key"}


def test_invalid_api_key_returns_403(client):
    r = client.get("/protected", headers={"X-API-Key": "nope"})
    assert r.status_code == 403
    assert r.json() == {"error": "invalid_api_key"}


def test_valid_api_key_passes(client):
    r = client.get("/protected", headers={"X-API-Key": "key-one"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_second_configured_key_also_passes(client):
    r = client.get("/protected", headers={"X-API-Key": "key-two"})
    assert r.status_code == 200
