from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.enums import AccessLevel, PipelineVersionStatus, ReviewDecision
from app.schemas.pipeline_spec import PipelineSpecDocument, StructuredDiff


class PipelineCreate(BaseModel):
    external_id: str = Field(min_length=1, max_length=128)
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    execution_mode: str = "streaming"
    owner_team_id: str | None = None
    metadata_links: dict[str, Any] = Field(default_factory=dict)


class PipelineUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    execution_mode: str | None = None
    owner_team_id: str | None = None
    metadata_links: dict[str, Any] | None = None


class PipelineRead(BaseModel):
    id: str
    external_id: str
    name: str
    description: str
    tags: list[str]
    execution_mode: str
    owner_user_id: str
    owner_team_id: str | None
    metadata_links: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class PipelineShareInput(BaseModel):
    team_id: str
    access_level: AccessLevel = AccessLevel.READ


class PipelineShareRead(BaseModel):
    id: str
    team_id: str
    access_level: AccessLevel


class PipelineVersionCreate(BaseModel):
    spec: PipelineSpecDocument
    change_summary: str = ""


class PipelineVersionRead(BaseModel):
    id: str
    pipeline_id: str
    version_number: int
    status: PipelineVersionStatus
    is_active: bool
    spec: PipelineSpecDocument
    change_summary: str
    created_by: str
    created_at: datetime
    review_requested_at: datetime | None
    published_at: datetime | None


class VersionActionRequest(BaseModel):
    comments: str = ""


class ReviewRead(BaseModel):
    id: str
    pipeline_version_id: str
    reviewer_id: str
    decision: ReviewDecision
    comments: str
    created_at: datetime


class VersionDiffResponse(BaseModel):
    from_version_id: str
    to_version_id: str
    diff: StructuredDiff
