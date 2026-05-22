import hmac
import os
from functools import lru_cache

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse


@lru_cache(maxsize=1)
def _load_keys() -> frozenset[str]:
    raw = os.getenv("API_KEYS", "")
    return frozenset(k.strip() for k in raw.split(",") if k.strip())


def _check(x_api_key: str | None) -> None:
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "missing_api_key"},
        )
    configured = _load_keys()
    if not any(hmac.compare_digest(x_api_key, k) for k in configured):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "invalid_api_key"},
        )


def require_api_key() -> object:
    """FastAPI dependency factory. Use as: `Depends(require_api_key())` or `_: None = require_api_key()`."""
    def _dep(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
        _check(x_api_key)
    return Depends(_dep)


async def api_key_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.detail)
