from __future__ import annotations

import enum


class RoleName(str, enum.Enum):
    INFRA_ADMIN = "INFRA_ADMIN"
    PIPELINE_DEV = "PIPELINE_DEV"
    AIOPS_ENGINEER = "AIOPS_ENGINEER"


class PipelineVersionStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    IN_REVIEW = "IN_REVIEW"
    PUBLISHED = "PUBLISHED"
    REJECTED = "REJECTED"


class PipelineRunStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class ReviewDecision(str, enum.Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class IncidentSeverity(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class IncidentStatus(str, enum.Enum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    RESOLVED = "RESOLVED"


class AccessLevel(str, enum.Enum):
    READ = "READ"
    WRITE = "WRITE"
    OWNER = "OWNER"
