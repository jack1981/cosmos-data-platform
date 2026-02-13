from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AccessLevel, Pipeline, PipelineShare, Role, RoleName, TeamMember, UserRole


class AuthContext:
    def __init__(self, user_id: str, roles: list[RoleName]):
        self.user_id = user_id
        self.roles = roles

    @property
    def is_admin(self) -> bool:
        return RoleName.INFRA_ADMIN in self.roles

    @property
    def is_pipeline_dev(self) -> bool:
        return RoleName.PIPELINE_DEV in self.roles

    @property
    def is_aiops(self) -> bool:
        return RoleName.AIOPS_ENGINEER in self.roles


def get_user_roles(db: Session, user_id: str) -> list[RoleName]:
    stmt = select(Role.name).join(UserRole, UserRole.role_id == Role.id).where(UserRole.user_id == user_id)
    return [row[0] for row in db.execute(stmt).all()]


def assert_roles(ctx: AuthContext, allowed_roles: list[RoleName]) -> None:
    if not any(role in ctx.roles for role in allowed_roles):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")


def get_user_team_ids(db: Session, user_id: str) -> set[str]:
    stmt = select(TeamMember.team_id).where(TeamMember.user_id == user_id)
    return {row[0] for row in db.execute(stmt).all()}


def _assert_pipeline_exists(db: Session, pipeline_id: str) -> Pipeline:
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None or pipeline.is_deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    return pipeline


def assert_pipeline_access(db: Session, ctx: AuthContext, pipeline_id: str, write: bool = False) -> Pipeline:
    pipeline = _assert_pipeline_exists(db, pipeline_id)

    if ctx.is_admin:
        return pipeline

    if pipeline.owner_user_id == ctx.user_id:
        return pipeline

    team_ids = get_user_team_ids(db, ctx.user_id)
    if pipeline.owner_team_id and pipeline.owner_team_id in team_ids:
        return pipeline

    share_stmt = select(PipelineShare).where(PipelineShare.pipeline_id == pipeline_id)
    shares = db.execute(share_stmt).scalars().all()

    for share in shares:
        if share.team_id not in team_ids:
            continue
        if write and share.access_level not in {AccessLevel.WRITE, AccessLevel.OWNER}:
            continue
        return pipeline

    if not write and ctx.is_aiops:
        return pipeline

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Pipeline access denied")


def assert_pipeline_write_access(db: Session, ctx: AuthContext, pipeline_id: str) -> Pipeline:
    if ctx.is_aiops and not ctx.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="AIOps role cannot edit pipeline definitions")
    return assert_pipeline_access(db, ctx, pipeline_id, write=True)


def assert_run_operation_access(db: Session, ctx: AuthContext, pipeline_id: str) -> Pipeline:
    # AIOps can stop/rerun and inspect all shared/accessible pipelines.
    return assert_pipeline_access(db, ctx, pipeline_id, write=False)
