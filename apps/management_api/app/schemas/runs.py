from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.enums import PipelineRunStatus


class TriggerRunRequest(BaseModel):
    pipeline_id: str
    pipeline_version_id: str | None = None
    trigger_type: str = "manual"
    input_override: list[Any] | None = None


class PipelineRunRead(BaseModel):
    id: str
    pipeline_id: str
    pipeline_version_id: str
    status: PipelineRunStatus
    execution_mode: str
    trigger_type: str
    initiated_by: str
    start_time: datetime | None
    end_time: datetime | None
    duration_seconds: float | None
    error_message: str | None
    artifact_pointers: dict[str, Any]
    metrics_summary: dict[str, Any]
    stop_requested: bool
    created_at: datetime


class RunEventRead(BaseModel):
    id: str
    run_id: str
    event_type: str
    stage_id: str | None
    message: str
    payload: dict[str, Any]
    created_at: datetime


class RunMetricSummary(BaseModel):
    run_id: str
    metrics: dict[str, Any]
