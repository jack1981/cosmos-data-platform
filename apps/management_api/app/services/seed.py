from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_password_hash
from app.models import (
    Pipeline,
    PipelineVersion,
    PipelineVersionStatus,
    Role,
    RoleName,
    Team,
    TeamMember,
    User,
    UserRole,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Identity + Access seeding
# ---------------------------------------------------------------------------


def _get_or_create_role(db: Session, role_name: RoleName, description: str) -> Role:
    role = db.execute(select(Role).where(Role.name == role_name)).scalar_one_or_none()
    if role:
        return role
    role = Role(name=role_name, description=description)
    db.add(role)
    db.flush()
    return role


def _get_or_create_user(db: Session, email: str, full_name: str, password: str) -> User:
    user = db.execute(select(User).where(User.email == email)).scalar_one_or_none()
    if user:
        return user
    user = User(email=email, full_name=full_name, hashed_password=get_password_hash(password), is_active=True)
    db.add(user)
    db.flush()
    return user


def _ensure_user_role(db: Session, user_id: str, role_id: str) -> None:
    assignment = db.execute(
        select(UserRole).where(UserRole.user_id == user_id, UserRole.role_id == role_id)
    ).scalar_one_or_none()
    if assignment:
        return
    db.add(UserRole(user_id=user_id, role_id=role_id))


def _get_or_create_team(db: Session, name: str, description: str) -> Team:
    team = db.execute(select(Team).where(Team.name == name)).scalar_one_or_none()
    if team:
        return team
    team = Team(name=name, description=description)
    db.add(team)
    db.flush()
    return team


def _ensure_team_member(db: Session, team_id: str, user_id: str) -> None:
    member = db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == user_id)
    ).scalar_one_or_none()
    if member:
        return
    db.add(TeamMember(team_id=team_id, user_id=user_id))


# ---------------------------------------------------------------------------
# Starter pipeline templates
# ---------------------------------------------------------------------------


def _make_linear_edges(stage_ids: list[str]) -> list[dict[str, str]]:
    return [{"source": stage_ids[idx], "target": stage_ids[idx + 1]} for idx in range(len(stage_ids) - 1)]


_ARTIFACT_ROOT = "/tmp/pipelineforge_artifacts"


def _stage_download(*, max_bytes: int = 2_097_152, timeout_seconds: float = 3.0) -> dict[str, Any]:
    return {
        "stage_id": "download",
        "name": "Download Videos",
        "stage_template": "builtin.video_download",
        "resources": {"cpus": 1.0, "gpus": 0.0},
        "batch_size": 10,
        "concurrency_hint": 4,
        "retries": 1,
        "params": {
            "url_field": "video_url",
            "output_field": "video_bytes",
            "timeout_seconds": timeout_seconds,
            "max_bytes": max_bytes,
        },
    }


def _stage_caption(*, model_name: str = "demo-vlm-mini") -> dict[str, Any]:
    return {
        "stage_id": "caption",
        "name": "Caption Videos",
        "stage_template": "builtin.video_caption",
        "resources": {"cpus": 1.0, "gpus": 1.0},
        "batch_size": 5,
        "concurrency_hint": 2,
        "retries": 0,
        "params": {"input_bytes_field": "video_bytes", "caption_field": "caption", "model_name": model_name},
    }


def _stage_quality_gate() -> dict[str, Any]:
    return {
        "stage_id": "quality_gate",
        "name": "Quality Gate",
        "stage_template": "builtin.video_quality_gate",
        "resources": {"cpus": 1.0, "gpus": 0.0},
        "batch_size": 10,
        "concurrency_hint": 2,
        "retries": 0,
        "params": {
            "min_bytes": 65_536,
            "min_confidence": 0.8,
            "allow_simulated_downloads": True,
            "drop_failed": False,
        },
    }


def _stage_incident() -> dict[str, Any]:
    return {
        "stage_id": "incident_enrich",
        "name": "Incident Enrichment",
        "stage_template": "builtin.video_incident_enrich",
        "resources": {"cpus": 1.0, "gpus": 0.0},
        "batch_size": 10,
        "concurrency_hint": 2,
        "retries": 0,
        "params": {
            "text_fields": ["caption", "ops_hint", "camera_id", "location"],
            "output_field": "incident",
        },
    }


def _stage_writer(path_suffix: str) -> dict[str, Any]:
    return {
        "stage_id": "writer",
        "name": "Write Artifact",
        "stage_template": "builtin.video_writer",
        "resources": {"cpus": 1.0, "gpus": 0.0},
        "batch_size": 10,
        "concurrency_hint": 2,
        "retries": 0,
        "params": {
            "output_path": f"{_ARTIFACT_ROOT}/{path_suffix}",
            "drop_output": True,
        },
    }


def _template_spec(
    *,
    pipeline_id: str,
    name: str,
    description: str,
    tags: list[str],
    stages: list[dict[str, Any]],
    source_data: list[dict[str, Any]],
    sink_path: str,
    datasets: list[str],
    models: list[str],
) -> dict[str, Any]:
    return {
        "pipeline_id": pipeline_id,
        "name": name,
        "description": description,
        "tags": tags,
        "owners": [],
        "team_ids": [],
        "execution_mode": "batch",
        "stages": stages,
        "edges": _make_linear_edges([stage["stage_id"] for stage in stages]),
        "io": {
            "source": {
                "kind": "inline",
                "static_data": source_data,
            },
            "sink": {"kind": "artifact_uri", "uri": f"file://{_ARTIFACT_ROOT}/{sink_path}"},
        },
        "runtime": {"ray_address": "auto", "autoscaling": {}, "retry_policy": {}},
        "observability": {"log_level": "INFO", "metrics_enabled": True, "tracing_enabled": False},
        "metadata_links": {"datasets": datasets, "models": models},
    }


def _seed_template_specs() -> list[dict[str, Any]]:
    caption_stages = [_stage_download(), _stage_caption(), _stage_writer("template_video_caption_batch.jsonl")]
    quality_stages = [
        _stage_download(max_bytes=1_572_864, timeout_seconds=2.5),
        _stage_caption(),
        _stage_quality_gate(),
        _stage_writer("template_video_quality_review.jsonl"),
    ]
    incident_stages = [
        _stage_download(max_bytes=1_572_864, timeout_seconds=2.5),
        _stage_caption(),
        _stage_incident(),
        _stage_writer("template_video_incident_triage.jsonl"),
    ]

    return [
        {
            "external_id": "template_video_caption_batch",
            "name": "Template: Video Caption Batch",
            "description": "Download videos, caption them, and write a caption manifest artifact.",
            "execution_mode": "batch",
            "tags": ["template", "starter", "video", "batch"],
            "metadata_links": {
                "is_template": True,
                "datasets": ["dataset://demo/video-surveillance"],
                "models": ["model://demo/vlm-mini"],
            },
            "spec": _template_spec(
                pipeline_id="template_video_caption_batch",
                name="Template: Video Caption Batch",
                description="Video pipeline example: Download -> Caption -> Writer.",
                tags=["template", "starter", "video", "batch"],
                stages=caption_stages,
                source_data=[
                    {
                        "video_id": "cam-001",
                        "video_url": "https://samplelib.com/lib/preview/mp4/sample-5s.mp4",
                        "camera_id": "dock-01",
                        "caption_uri": "s3://pipelineforge-demo/captions/cam-001.json",
                    },
                    {
                        "video_id": "cam-002",
                        "video_url": "https://samplelib.com/lib/preview/mp4/sample-10s.mp4",
                        "camera_id": "dock-02",
                        "caption_uri": "s3://pipelineforge-demo/captions/cam-002.json",
                    },
                    {
                        "video_id": "cam-003",
                        "video_url": "s3://pipelineforge-demo/raw/cam-003.mp4",
                        "camera_id": "dock-03",
                        "caption_uri": "s3://pipelineforge-demo/captions/cam-003.json",
                    },
                ],
                sink_path="template_video_caption_batch.jsonl",
                datasets=["dataset://demo/video-surveillance"],
                models=["model://demo/vlm-mini"],
            ),
        },
        {
            "external_id": "template_video_quality_review",
            "name": "Template: Video Quality Review",
            "description": "Run captioning with quality checks and keep pass/fail reasons in output.",
            "execution_mode": "batch",
            "tags": ["template", "starter", "video", "quality"],
            "metadata_links": {
                "is_template": True,
                "datasets": ["dataset://demo/video-quality"],
                "models": ["model://demo/vlm-mini"],
            },
            "spec": _template_spec(
                pipeline_id="template_video_quality_review",
                name="Template: Video Quality Review",
                description="Download + caption + quality gate + writer pipeline.",
                tags=["template", "starter", "video", "quality"],
                stages=quality_stages,
                source_data=[
                    {
                        "video_id": "qc-001",
                        "video_url": "https://filesamples.com/samples/video/mp4/sample_640x360.mp4",
                        "camera_id": "line-a",
                        "ops_hint": "normal loading activity",
                    },
                    {
                        "video_id": "qc-002",
                        "video_url": "https://example.invalid/unreachable-video.mp4",
                        "camera_id": "line-b",
                        "ops_hint": "night shift low light",
                    },
                    {
                        "video_id": "qc-003",
                        "video_url": "s3://pipelineforge-demo/raw/qc-003.mp4",
                        "camera_id": "line-c",
                        "ops_hint": "forklift crossing",
                    },
                ],
                sink_path="template_video_quality_review.jsonl",
                datasets=["dataset://demo/video-quality"],
                models=["model://demo/vlm-mini"],
            ),
        },
        {
            "external_id": "template_video_incident_triage",
            "name": "Template: Video Incident Triage",
            "description": "Operationally-focused video triage with severity tagging and recommendations.",
            "execution_mode": "batch",
            "tags": ["template", "starter", "video", "ops"],
            "metadata_links": {
                "is_template": True,
                "datasets": ["dataset://demo/video-incidents"],
                "models": ["model://demo/vlm-mini"],
            },
            "spec": _template_spec(
                pipeline_id="template_video_incident_triage",
                name="Template: Video Incident Triage",
                description="Download + caption + incident enrichment + writer pipeline.",
                tags=["template", "starter", "video", "ops"],
                stages=incident_stages,
                source_data=[
                    {
                        "video_id": "inc-001",
                        "video_url": "https://samplelib.com/lib/preview/mp4/sample-5s.mp4",
                        "camera_id": "north-yard",
                        "location": "north-yard",
                        "ops_hint": "possible smoke near loading dock",
                    },
                    {
                        "video_id": "inc-002",
                        "video_url": "s3://pipelineforge-demo/raw/inc-002.mp4",
                        "camera_id": "warehouse-aisle-4",
                        "location": "aisle-4",
                        "ops_hint": "forklift collision crash reported",
                    },
                    {
                        "video_id": "inc-003",
                        "video_url": "https://samplelib.com/lib/preview/mp4/sample-10s.mp4",
                        "camera_id": "east-fence",
                        "location": "east-fence",
                        "ops_hint": "intrusion alert near perimeter fence",
                    },
                ],
                sink_path="template_video_incident_triage.jsonl",
                datasets=["dataset://demo/video-incidents"],
                models=["model://demo/vlm-mini"],
            ),
        },
    ]


# ---------------------------------------------------------------------------
# Pipeline/template persistence
# ---------------------------------------------------------------------------


def _spec_json_changed(current: dict[str, Any], desired: dict[str, Any]) -> bool:
    import json

    current_norm = json.dumps(current, sort_keys=True, separators=(",", ":"))
    desired_norm = json.dumps(desired, sort_keys=True, separators=(",", ":"))
    return current_norm != desired_norm


def _ensure_template_pipeline(
    db: Session,
    *,
    owner_user_id: str,
    owner_team_id: str,
    template: dict[str, Any],
) -> None:
    pipeline = db.execute(select(Pipeline).where(Pipeline.external_id == template["external_id"])).scalar_one_or_none()

    if pipeline is None:
        pipeline = Pipeline(
            external_id=template["external_id"],
            name=template["name"],
            description=template["description"],
            tags=template["tags"],
            execution_mode=template["execution_mode"],
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            metadata_links=template["metadata_links"],
            is_deleted=False,
            created_by=owner_user_id,
        )
        db.add(pipeline)
        db.flush()
    else:
        pipeline.name = template["name"]
        pipeline.description = template["description"]
        pipeline.tags = template["tags"]
        pipeline.execution_mode = template["execution_mode"]
        pipeline.owner_user_id = owner_user_id
        pipeline.owner_team_id = owner_team_id
        pipeline.metadata_links = template["metadata_links"]
        pipeline.is_deleted = False

    publish_time = datetime.now(timezone.utc)
    active_version = db.execute(
        select(PipelineVersion).where(PipelineVersion.pipeline_id == pipeline.id, PipelineVersion.is_active.is_(True))
    ).scalar_one_or_none()
    if active_version is not None:
        if active_version.status == PipelineVersionStatus.PUBLISHED and not _spec_json_changed(
            active_version.spec_json, template["spec"]
        ):
            return
        active_version.is_active = False

    next_version = (
        db.execute(
            select(func.max(PipelineVersion.version_number)).where(PipelineVersion.pipeline_id == pipeline.id)
        ).scalar_one()
        or 0
    ) + 1
    version = PipelineVersion(
        pipeline_id=pipeline.id,
        version_number=next_version,
        status=PipelineVersionStatus.PUBLISHED,
        is_active=True,
        spec_json=template["spec"],
        change_summary="Seeded/updated starter template",
        created_by=owner_user_id,
        published_at=publish_time,
    )
    db.add(version)


# ---------------------------------------------------------------------------
# Public seed entrypoint
# ---------------------------------------------------------------------------


def seed_defaults(db: Session) -> None:
    settings = get_settings()

    role_admin = _get_or_create_role(db, RoleName.INFRA_ADMIN, "Full system administration")
    role_dev = _get_or_create_role(db, RoleName.PIPELINE_DEV, "Pipeline authoring and execution")
    role_aiops = _get_or_create_role(db, RoleName.AIOPS_ENGINEER, "Run observability and operational controls")

    admin_user = _get_or_create_user(
        db,
        settings.default_admin_email,
        "Infra Admin",
        settings.default_admin_password,
    )
    dev_user = _get_or_create_user(db, settings.default_dev_email, "Pipeline Developer", settings.default_dev_password)
    aiops_user = _get_or_create_user(
        db,
        settings.default_aiops_email,
        "AIOps Engineer",
        settings.default_aiops_password,
    )

    _ensure_user_role(db, admin_user.id, role_admin.id)
    _ensure_user_role(db, dev_user.id, role_dev.id)
    _ensure_user_role(db, aiops_user.id, role_aiops.id)

    default_team = _get_or_create_team(db, "platform-team", "Default shared team for local development")
    _ensure_team_member(db, default_team.id, dev_user.id)
    _ensure_team_member(db, default_team.id, aiops_user.id)

    for template in _seed_template_specs():
        _ensure_template_pipeline(
            db,
            owner_user_id=dev_user.id,
            owner_team_id=default_team.id,
            template=template,
        )

    db.commit()
