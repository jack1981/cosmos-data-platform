from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user
from app.db.session import get_db
from app.models import (
    AccessLevel,
    Pipeline,
    PipelineShare,
    PipelineVersion,
    PipelineVersionStatus,
    ReviewDecision,
    RoleName,
)
from app.schemas.pipelines import (
    PipelineCreate,
    PipelineRead,
    PipelineShareInput,
    PipelineShareRead,
    PipelineUpdate,
    PipelineVersionCreate,
    PipelineVersionRead,
    ReviewRead,
    VersionActionRequest,
    VersionDiffResponse,
)
from app.services.audit import add_audit_entry
from app.services.rbac import (
    assert_pipeline_access,
    assert_pipeline_write_access,
    assert_roles,
    get_user_team_ids,
)
from app.services.spec_diff import build_structured_diff
from app.services.stage_registry import list_templates

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _pipeline_to_read(row: Pipeline) -> PipelineRead:
    return PipelineRead(
        id=row.id,
        external_id=row.external_id,
        name=row.name,
        description=row.description,
        tags=row.tags or [],
        execution_mode=row.execution_mode,
        owner_user_id=row.owner_user_id,
        owner_team_id=row.owner_team_id,
        metadata_links=row.metadata_links or {},
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _version_to_read(row: PipelineVersion) -> PipelineVersionRead:
    return PipelineVersionRead(
        id=row.id,
        pipeline_id=row.pipeline_id,
        version_number=row.version_number,
        status=row.status,
        is_active=row.is_active,
        spec=row.spec_json,
        change_summary=row.change_summary,
        created_by=row.created_by,
        created_at=row.created_at,
        review_requested_at=row.review_requested_at,
        published_at=row.published_at,
    )


@router.get("/stage-templates")
def stage_templates(current_user: CurrentUser = Depends(get_current_user)) -> list[dict]:
    del current_user
    return list_templates()


@router.get("", response_model=list[PipelineRead])
def list_pipelines(
    search: str | None = Query(default=None),
    tag: str | None = Query(default=None),
    owner_user_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[PipelineRead]:
    candidates = db.execute(select(Pipeline).where(Pipeline.is_deleted.is_(False)).order_by(Pipeline.updated_at.desc())).scalars().all()
    visible: list[Pipeline] = []

    for pipeline in candidates:
        try:
            assert_pipeline_access(db, current_user.context, pipeline.id, write=False)
        except HTTPException:
            continue
        visible.append(pipeline)

    filtered: list[Pipeline] = []
    for pipeline in visible:
        if search and search.lower() not in f"{pipeline.name} {pipeline.description}".lower():
            continue
        if tag and tag not in (pipeline.tags or []):
            continue
        if owner_user_id and pipeline.owner_user_id != owner_user_id:
            continue
        filtered.append(pipeline)

    return [_pipeline_to_read(row) for row in filtered]


@router.post("", response_model=PipelineRead, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    payload: PipelineCreate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRead:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN, RoleName.PIPELINE_DEV])

    existing = db.execute(select(Pipeline).where(Pipeline.external_id == payload.external_id)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Pipeline external_id already exists")

    pipeline = Pipeline(
        external_id=payload.external_id,
        name=payload.name,
        description=payload.description,
        tags=payload.tags,
        execution_mode=payload.execution_mode,
        owner_user_id=current_user.user.id,
        owner_team_id=payload.owner_team_id,
        metadata_links=payload.metadata_links,
        created_by=current_user.user.id,
    )
    db.add(pipeline)
    db.flush()

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.create",
        resource_type="pipeline",
        resource_id=pipeline.id,
        details={"external_id": payload.external_id},
    )
    db.commit()
    db.refresh(pipeline)
    return _pipeline_to_read(pipeline)


@router.get("/{pipeline_id}", response_model=PipelineRead)
def get_pipeline(
    pipeline_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRead:
    pipeline = assert_pipeline_access(db, current_user.context, pipeline_id, write=False)
    return _pipeline_to_read(pipeline)


@router.patch("/{pipeline_id}", response_model=PipelineRead)
def update_pipeline(
    pipeline_id: str,
    payload: PipelineUpdate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRead:
    pipeline = assert_pipeline_write_access(db, current_user.context, pipeline_id)

    update_fields = payload.model_dump(exclude_unset=True)
    for key, value in update_fields.items():
        setattr(pipeline, key, value)
    pipeline.updated_at = datetime.now(timezone.utc)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.update",
        resource_type="pipeline",
        resource_id=pipeline.id,
        details={"changed": list(update_fields.keys())},
    )
    db.commit()
    db.refresh(pipeline)
    return _pipeline_to_read(pipeline)


@router.delete("/{pipeline_id}")
def delete_pipeline(
    pipeline_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> dict[str, str]:
    pipeline = assert_pipeline_write_access(db, current_user.context, pipeline_id)
    pipeline.is_deleted = True
    pipeline.updated_at = datetime.now(timezone.utc)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.delete",
        resource_type="pipeline",
        resource_id=pipeline.id,
    )
    db.commit()
    return {"status": "deleted"}


@router.get("/{pipeline_id}/shares", response_model=list[PipelineShareRead])
def list_pipeline_shares(
    pipeline_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[PipelineShareRead]:
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)
    rows = db.execute(select(PipelineShare).where(PipelineShare.pipeline_id == pipeline_id)).scalars().all()
    return [PipelineShareRead(id=row.id, team_id=row.team_id, access_level=row.access_level) for row in rows]


@router.post("/{pipeline_id}/shares", response_model=PipelineShareRead, status_code=status.HTTP_201_CREATED)
def upsert_pipeline_share(
    pipeline_id: str,
    payload: PipelineShareInput,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineShareRead:
    assert_pipeline_write_access(db, current_user.context, pipeline_id)

    row = db.execute(
        select(PipelineShare).where(PipelineShare.pipeline_id == pipeline_id, PipelineShare.team_id == payload.team_id)
    ).scalar_one_or_none()

    if row is None:
        row = PipelineShare(pipeline_id=pipeline_id, team_id=payload.team_id, access_level=payload.access_level)
        db.add(row)
    else:
        row.access_level = payload.access_level

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.share.update",
        resource_type="pipeline",
        resource_id=pipeline_id,
        details={"team_id": payload.team_id, "access_level": payload.access_level.value},
    )
    db.commit()
    db.refresh(row)
    return PipelineShareRead(id=row.id, team_id=row.team_id, access_level=row.access_level)


@router.post("/{pipeline_id}/versions", response_model=PipelineVersionRead, status_code=status.HTTP_201_CREATED)
def create_draft_version(
    pipeline_id: str,
    payload: PipelineVersionCreate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineVersionRead:
    pipeline = assert_pipeline_write_access(db, current_user.context, pipeline_id)

    next_version = (
        db.execute(select(func.max(PipelineVersion.version_number)).where(PipelineVersion.pipeline_id == pipeline_id)).scalar_one()
        or 0
    ) + 1

    spec_dict = payload.spec.model_dump(mode="json")
    spec_dict["pipeline_id"] = pipeline.external_id

    version = PipelineVersion(
        pipeline_id=pipeline_id,
        version_number=next_version,
        status=PipelineVersionStatus.DRAFT,
        is_active=False,
        spec_json=spec_dict,
        change_summary=payload.change_summary,
        created_by=current_user.user.id,
    )
    db.add(version)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.version.create",
        resource_type="pipeline_version",
        resource_id=str(next_version),
        details={"pipeline_id": pipeline_id, "version_number": next_version},
    )
    db.commit()
    db.refresh(version)
    return _version_to_read(version)


@router.get("/{pipeline_id}/versions", response_model=list[PipelineVersionRead])
def list_versions(
    pipeline_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[PipelineVersionRead]:
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)
    versions = db.execute(
        select(PipelineVersion)
        .where(PipelineVersion.pipeline_id == pipeline_id)
        .order_by(PipelineVersion.version_number.desc())
    ).scalars().all()
    return [_version_to_read(row) for row in versions]


@router.get("/{pipeline_id}/versions/{version_id}", response_model=PipelineVersionRead)
def get_version(
    pipeline_id: str,
    version_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineVersionRead:
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)
    version = db.get(PipelineVersion, version_id)
    if version is None or version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")
    return _version_to_read(version)


@router.post("/{pipeline_id}/versions/{version_id}/submit-review", response_model=PipelineVersionRead)
def submit_review(
    pipeline_id: str,
    version_id: str,
    payload: VersionActionRequest,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineVersionRead:
    del payload
    assert_pipeline_write_access(db, current_user.context, pipeline_id)
    version = db.get(PipelineVersion, version_id)
    if version is None or version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")
    if version.status != PipelineVersionStatus.DRAFT:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only draft versions can be submitted")

    version.status = PipelineVersionStatus.IN_REVIEW
    version.review_requested_at = datetime.now(timezone.utc)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.version.submit_review",
        resource_type="pipeline_version",
        resource_id=version.id,
    )
    db.commit()
    db.refresh(version)
    return _version_to_read(version)


@router.post("/{pipeline_id}/versions/{version_id}/approve", response_model=ReviewRead)
def approve_version(
    pipeline_id: str,
    version_id: str,
    payload: VersionActionRequest,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> ReviewRead:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)

    version = db.get(PipelineVersion, version_id)
    if version is None or version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")

    if version.status not in {PipelineVersionStatus.DRAFT, PipelineVersionStatus.IN_REVIEW}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Version is not reviewable")

    from app.models import PipelineReview

    review = PipelineReview(
        pipeline_version_id=version.id,
        reviewer_id=current_user.user.id,
        decision=ReviewDecision.APPROVED,
        comments=payload.comments,
    )
    version.status = PipelineVersionStatus.IN_REVIEW
    db.add(review)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.version.approve",
        resource_type="pipeline_version",
        resource_id=version.id,
    )
    db.commit()
    db.refresh(review)

    return ReviewRead(
        id=review.id,
        pipeline_version_id=review.pipeline_version_id,
        reviewer_id=review.reviewer_id,
        decision=review.decision,
        comments=review.comments,
        created_at=review.created_at,
    )


@router.post("/{pipeline_id}/versions/{version_id}/reject", response_model=ReviewRead)
def reject_version(
    pipeline_id: str,
    version_id: str,
    payload: VersionActionRequest,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> ReviewRead:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)

    version = db.get(PipelineVersion, version_id)
    if version is None or version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")

    from app.models import PipelineReview

    review = PipelineReview(
        pipeline_version_id=version.id,
        reviewer_id=current_user.user.id,
        decision=ReviewDecision.REJECTED,
        comments=payload.comments,
    )
    version.status = PipelineVersionStatus.REJECTED
    db.add(review)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.version.reject",
        resource_type="pipeline_version",
        resource_id=version.id,
    )
    db.commit()
    db.refresh(review)

    return ReviewRead(
        id=review.id,
        pipeline_version_id=review.pipeline_version_id,
        reviewer_id=review.reviewer_id,
        decision=review.decision,
        comments=review.comments,
        created_at=review.created_at,
    )


@router.post("/{pipeline_id}/versions/{version_id}/publish", response_model=PipelineVersionRead)
def publish_version(
    pipeline_id: str,
    version_id: str,
    payload: VersionActionRequest,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineVersionRead:
    del payload
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])
    pipeline = assert_pipeline_access(db, current_user.context, pipeline_id, write=False)

    version = db.get(PipelineVersion, version_id)
    if version is None or version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")

    if version.status == PipelineVersionStatus.REJECTED:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Rejected versions cannot be published")

    current_active = db.execute(
        select(PipelineVersion).where(PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.is_active.is_(True))
    ).scalars().all()
    for row in current_active:
        row.is_active = False

    version.status = PipelineVersionStatus.PUBLISHED
    version.is_active = True
    version.published_at = datetime.now(timezone.utc)
    pipeline.execution_mode = version.spec_json.get("execution_mode", pipeline.execution_mode)

    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="pipeline.version.publish",
        resource_type="pipeline_version",
        resource_id=version.id,
    )
    db.commit()
    db.refresh(version)
    return _version_to_read(version)


@router.get("/{pipeline_id}/diff", response_model=VersionDiffResponse)
def diff_versions(
    pipeline_id: str,
    from_version_id: str,
    to_version_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> VersionDiffResponse:
    assert_pipeline_access(db, current_user.context, pipeline_id, write=False)

    from_version = db.get(PipelineVersion, from_version_id)
    to_version = db.get(PipelineVersion, to_version_id)

    if from_version is None or to_version is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")

    if from_version.pipeline_id != pipeline_id or to_version.pipeline_id != pipeline_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Both versions must belong to pipeline")

    diff = build_structured_diff(from_version.spec_json, to_version.spec_json)
    return VersionDiffResponse(from_version_id=from_version_id, to_version_id=to_version_id, diff=diff)


@router.get("/{pipeline_id}/access-summary")
def pipeline_access_summary(
    pipeline_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> dict:
    pipeline = assert_pipeline_access(db, current_user.context, pipeline_id, write=False)
    team_ids = sorted(get_user_team_ids(db, current_user.user.id))

    return {
        "pipeline_id": pipeline.id,
        "owner_user_id": pipeline.owner_user_id,
        "owner_team_id": pipeline.owner_team_id,
        "viewer_team_ids": team_ids,
        "roles": [role.value for role in current_user.roles],
        "can_edit": current_user.context.is_admin or current_user.context.is_pipeline_dev,
    }
