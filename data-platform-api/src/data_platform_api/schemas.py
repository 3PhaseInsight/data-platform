from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ResultRow(BaseModel):
    phase: str
    label_type: str
    label_value: str
    confidence: float
    details: dict[str, Any] | None = Field(default=None)


class LatestResultsResponse(BaseModel):
    data_app: str
    meter_id: int
    generated_at: datetime
    results: list[ResultRow]


class ErrorResponse(BaseModel):
    error: str


class NoResultsResponse(BaseModel):
    error: str
    data_app: str
    meter_id: int
