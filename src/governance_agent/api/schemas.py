"""Request/response models for the FastAPI app."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class AskRequest(BaseModel):
    question: str


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
