"""Request/response models for the FastAPI app."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, StringConstraints

MAX_QUESTION_LENGTH = 500


class AskRequest(BaseModel):
    question: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True, min_length=1, max_length=MAX_QUESTION_LENGTH
        ),
    ]


class AskResponse(BaseModel):
    request_id: str
    verdict: str
    answer: str | None = None
    columns: list[str] = []
    rows: list[list[Any]] = []
    sql: str | None = None
    metric_used: str | None = None
    citations: list[str] = []
    block_reason: str | None = None


class MetricInfo(BaseModel):
    name: str
    description: str
    table: str


class MetricsResponse(BaseModel):
    metrics: list[MetricInfo]
    dimensions: list[str]


class HealthResponse(BaseModel):
    status: str


class RoleInfo(BaseModel):
    name: str
    description: str
    max_rows: int


class RolesResponse(BaseModel):
    roles: list[RoleInfo]
