from fastapi import FastAPI, HTTPException

from data_platform_api.auth import api_key_exception_handler
from data_platform_api.routes import results


def create_app() -> FastAPI:
    app = FastAPI(
        title="3PhaseInsight Data Platform API",
        version="0.1.0",
        description=(
            "Customer-facing API for querying Data App results. "
            "Authenticate with the `X-API-Key` header. "
            "Note: in v1 the {data_app} path segment must be a literal Airflow DAG ID "
            "(e.g. `sm_classifier`). An alias layer is planned for a later version."
        ),
    )
    app.add_exception_handler(HTTPException, api_key_exception_handler)
    app.include_router(results.router)
    return app


app = create_app()
