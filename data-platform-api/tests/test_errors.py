from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException as StarletteHTTPException

from data_platform_api.errors import http_exception_handler


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)

    @app.get("/raises-dict")
    def raises_dict() -> None:
        raise HTTPException(status_code=418, detail={"error": "teapot"})

    @app.get("/raises-string")
    def raises_string() -> None:
        raise HTTPException(status_code=418, detail="i am a teapot")

    return app


def test_dict_detail_passes_through_as_envelope():
    client = TestClient(_build_app())
    r = client.get("/raises-dict")
    assert r.status_code == 418
    assert r.json() == {"error": "teapot"}


def test_string_detail_is_wrapped_in_envelope():
    client = TestClient(_build_app())
    r = client.get("/raises-string")
    assert r.status_code == 418
    assert r.json() == {"error": "i am a teapot"}


def test_fastapi_internal_404_uses_envelope():
    """Unknown routes raise an HTTPException with a string detail; the handler
    should normalize that into {"error": "..."} like everything else.
    """
    client = TestClient(_build_app())
    r = client.get("/this-route-does-not-exist")
    assert r.status_code == 404
    assert r.json() == {"error": "Not Found"}
