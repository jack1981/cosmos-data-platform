from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user
from app.db.session import SessionLocal, get_db
from app.models import (
    PipelineRun,
    PipelineRunStatus,
    PipelineVersion,
    PipelineVersionStatus,
    RunEvent,
    RoleName,
)
from app.schemas.runs import (
    PipelineRunRead,
    RunEventRead,
    RunMetricSummary,
    TriggerRunRequest,
)
from app.services.audit import add_audit_entry
from app.services.log_store import run_log_store
from app.services.rbac import assert_pipeline_access, assert_run_operation_access
from app.services.runner import pipeline_runner_service

router = APIRouter(prefix="/runs", tags=["runs"])


def _run_to_schema(row: PipelineRun) -> PipelineRunRead:
    return PipelineRunRead(
        id=row.id,
        pipeline_id=row.pipeline_id,
        pipeline_version_id=row.pipeline_version_id,
        status=row.status,
        execution_mode=row.execution_mode,
        trigger_type=row.trigger_type,
        initiated_by=row.initiated_by,
        start_time=row.start_time,
        end_time=row.end_time,
        duration_seconds=row.duration_seconds,
        error_message=row.error_message,
        artifact_pointers=row.artifact_pointers or {},
        metrics_summary=row.metrics_summary or {},
        stop_requested=row.stop_requested,
        created_at=row.created_at,
    )


def _resolve_target_version(db: Session, pipeline_id: str, requested_version_id: str | None) -> PipelineVersion:
    if requested_version_id:
        version = db.get(PipelineVersion, requested_version_id)
        if version is None or version.pipeline_id != pipeline_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline version not found")
        return version

    version = db.execute(
        select(PipelineVersion)
        .where(PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.is_active.is_(True))
        .order_by(PipelineVersion.version_number.desc())
    ).scalar_one_or_none()

    if version:
        return version

    version = db.execute(
        select(PipelineVersion)
        .where(PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.status == PipelineVersionStatus.PUBLISHED)
        .order_by(PipelineVersion.version_number.desc())
    ).scalar_one_or_none()
    if version:
        return version

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No published pipeline version found")


@router.post("/trigger", response_model=PipelineRunRead, status_code=status.HTTP_201_CREATED)
def trigger_run(
    payload: TriggerRunRequest,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRunRead:
    pipeline = assert_pipeline_access(db, current_user.context, payload.pipeline_id, write=False)
    if not any(role in current_user.roles for role in [RoleName.PIPELINE_DEV, RoleName.INFRA_ADMIN, RoleName.AIOPS_ENGINEER]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role cannot trigger runs")

    version = _resolve_target_version(db, pipeline.id, payload.pipeline_version_id)

    run = PipelineRun(
        pipeline_id=pipeline.id,
        pipeline_version_id=version.id,
        status=PipelineRunStatus.QUEUED,
        execution_mode=version.spec_json.get("execution_mode", pipeline.execution_mode),
        trigger_type=payload.trigger_type,
        initiated_by=current_user.user.id,
    )
    db.add(run)
    db.flush()

    db.add(RunEvent(run_id=run.id, event_type="run_queued", stage_id=None, message="Run queued", payload={}))
    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="run.trigger",
        resource_type="pipeline_run",
        resource_id=run.id,
        details={"pipeline_id": pipeline.id, "version_id": version.id},
    )
    db.commit()
    db.refresh(run)

    pipeline_runner_service.submit_run(run.id)
    return _run_to_schema(run)


@router.get("", response_model=list[PipelineRunRead])
def list_runs(
    pipeline_id: str | None = Query(default=None),
    status_filter: PipelineRunStatus | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[PipelineRunRead]:
    runs = db.execute(select(PipelineRun).order_by(PipelineRun.created_at.desc()).limit(limit)).scalars().all()
    visible: list[PipelineRunRead] = []

    for run in runs:
        if pipeline_id and run.pipeline_id != pipeline_id:
            continue
        if status_filter and run.status != status_filter:
            continue

        try:
            assert_pipeline_access(db, current_user.context, run.pipeline_id, write=False)
        except HTTPException:
            continue
        visible.append(_run_to_schema(run))

    return visible


@router.get("/{run_id}", response_model=PipelineRunRead)
def get_run(
    run_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRunRead:
    run = db.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    assert_pipeline_access(db, current_user.context, run.pipeline_id, write=False)
    return _run_to_schema(run)


@router.post("/{run_id}/stop", response_model=PipelineRunRead)
def stop_run(
    run_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRunRead:
    run = db.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    assert_run_operation_access(db, current_user.context, run.pipeline_id)
    if RoleName.AIOPS_ENGINEER not in current_user.roles and RoleName.INFRA_ADMIN not in current_user.roles:
        if RoleName.PIPELINE_DEV not in current_user.roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role cannot stop run")

    run.stop_requested = True
    db.add(RunEvent(run_id=run.id, event_type="stop_requested", stage_id=None, message="Stop requested", payload={}))
    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="run.stop",
        resource_type="pipeline_run",
        resource_id=run.id,
    )
    db.commit()

    pipeline_runner_service.request_stop(run.id)
    db.refresh(run)
    return _run_to_schema(run)


@router.post("/{run_id}/rerun", response_model=PipelineRunRead, status_code=status.HTTP_201_CREATED)
def rerun(
    run_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> PipelineRunRead:
    existing = db.get(PipelineRun, run_id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    assert_run_operation_access(db, current_user.context, existing.pipeline_id)

    rerun_item = PipelineRun(
        pipeline_id=existing.pipeline_id,
        pipeline_version_id=existing.pipeline_version_id,
        status=PipelineRunStatus.QUEUED,
        execution_mode=existing.execution_mode,
        trigger_type="rerun",
        initiated_by=current_user.user.id,
    )
    db.add(rerun_item)
    db.flush()

    db.add(RunEvent(run_id=rerun_item.id, event_type="run_queued", stage_id=None, message="Rerun queued", payload={}))
    add_audit_entry(
        db,
        actor_user_id=current_user.user.id,
        action="run.rerun",
        resource_type="pipeline_run",
        resource_id=rerun_item.id,
        details={"source_run_id": run_id},
    )
    db.commit()
    db.refresh(rerun_item)

    pipeline_runner_service.submit_run(rerun_item.id)
    return _run_to_schema(rerun_item)


@router.get("/{run_id}/events", response_model=list[RunEventRead])
def list_run_events(
    run_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[RunEventRead]:
    run = db.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    assert_pipeline_access(db, current_user.context, run.pipeline_id, write=False)

    events = db.execute(select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.created_at.asc())).scalars().all()
    return [
        RunEventRead(
            id=event.id,
            run_id=event.run_id,
            event_type=event.event_type,
            stage_id=event.stage_id,
            message=event.message,
            payload=event.payload,
            created_at=event.created_at,
        )
        for event in events
    ]


@router.get("/{run_id}/metrics-summary", response_model=RunMetricSummary)
def metrics_summary(
    run_id: str,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> RunMetricSummary:
    run = db.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    assert_pipeline_access(db, current_user.context, run.pipeline_id, write=False)

    metrics = run.metrics_summary or {}
    return RunMetricSummary(run_id=run.id, metrics=metrics)


@router.get("/{run_id}/logs/stream")
async def stream_run_logs(
    run_id: str,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    run = db.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    assert_pipeline_access(db, current_user.context, run.pipeline_id, write=False)

    async def _event_stream() -> str:
        cursor = 0
        while True:
            lines, cursor = run_log_store.get_since(run_id, cursor)
            for line in lines:
                yield f"event: log\ndata: {line}\n\n"

            with SessionLocal() as stream_db:
                current_run = stream_db.get(PipelineRun, run_id)
                if current_run is None:
                    yield "event: end\ndata: run-not-found\n\n"
                    break
                if current_run.status in {
                    PipelineRunStatus.SUCCEEDED,
                    PipelineRunStatus.FAILED,
                    PipelineRunStatus.STOPPED,
                } and not lines:
                    yield "event: end\ndata: completed\n\n"
                    break

            await asyncio.sleep(1.0)

    return StreamingResponse(_event_stream(), media_type="text/event-stream")
