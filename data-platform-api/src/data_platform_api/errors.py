from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Return all HTTPException bodies in a flat `{"error": ...}` envelope.

    Application code raises HTTPException with `detail={"error": "..."}` so the
    body is already in the right shape. Framework-internal exceptions (404 for
    unknown routes, 405 for wrong method, etc.) use a plain string detail —
    wrap those in the same envelope so every error response has a consistent
    shape.

    Registered against `starlette.exceptions.HTTPException` (the parent of
    FastAPI's `HTTPException`) so it catches both application-raised and
    framework-raised cases.
    """
    body = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content=body)
