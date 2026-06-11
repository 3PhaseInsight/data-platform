from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from data_platform_api.auth import require_api_key
from data_platform_api.db import get_session
from data_platform_api.schemas import LatestResultsResponse, ResultRow

router = APIRouter(prefix="/v1/data-apps", tags=["data-apps"])


@router.get(
    "/{data_app}/meters/{meter_id}/results/latest",
    response_model=LatestResultsResponse,
    dependencies=[require_api_key()],
    responses={
        401: {"description": "Missing API key"},
        403: {"description": "Invalid API key"},
        404: {"description": "No results for this data_app and meter_id"},
    },
)
def get_latest_results(
    data_app: str = Path(..., description="Data App slug (currently the literal dag_id)."),
    meter_id: int = Path(..., description="meta.meter.id"),
    session: Session = Depends(get_session),
) -> LatestResultsResponse:
    # lazy import: avoids heavy framework init at module import time
    from threephi_framework.resources.meta.run_result import RunResultResource

    rows = RunResultResource(session).get_latest_for_meter(dag_id=data_app, meter_id=meter_id)

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "no_results", "data_app": data_app, "meter_id": meter_id},
        )

    return LatestResultsResponse(
        data_app=data_app,
        meter_id=meter_id,
        generated_at=max(row.created_at for row in rows),
        results=[
            ResultRow(
                phase=row.phase,
                label_type=row.label_type,
                label_value=row.label_value,
                confidence=row.confidence,
                details=row.result,
            )
            for row in rows
        ],
    )
