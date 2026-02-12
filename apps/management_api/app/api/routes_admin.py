from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user
from app.core.security import get_password_hash
from app.db.session import get_db
from app.models import AuditLog, Role, RoleName, User, UserRole
from app.schemas.admin import UserCreate, UserRead
from app.services.audit import add_audit_entry
from app.services.rbac import assert_roles, get_user_roles

router = APIRouter(prefix="/admin", tags=["admin"])


def _user_to_schema(db: Session, user: User) -> UserRead:
    return UserRead(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        roles=get_user_roles(db, user.id),
        is_active=user.is_active,
        created_at=user.created_at,
    )


@router.get("/roles")
def list_roles(
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[dict]:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN, RoleName.PIPELINE_DEV, RoleName.AIOPS_ENGINEER])
    roles = db.execute(select(Role).order_by(Role.name.asc())).scalars().all()
    return [{"id": role.id, "name": role.name.value, "description": role.description} for role in roles]


@router.get("/users", response_model=list[UserRead])
def list_users(
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[UserRead]:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])
    users = db.execute(select(User).order_by(User.created_at.desc())).scalars().all()
    return [_user_to_schema(db, user) for user in users]


@router.post("/users", response_model=UserRead, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreate,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> UserRead:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])

    existing = db.execute(select(User).where(User.email == payload.email)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")

    user = User(
        email=payload.email,
        full_name=payload.full_name,
        hashed_password=get_password_hash(payload.password),
        is_active=True,
    )
    db.add(user)
    db.flush()

    role_rows = db.execute(select(Role).where(Role.name.in_(payload.roles))).scalars().all()
    if len(role_rows) != len(payload.roles):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="One or more roles are invalid")
    for role in role_rows:
        db.add(UserRole(user_id=user.id, role_id=role.id))

    add_audit_entry(
        db,
        current_user.user.id,
        "admin.user.create",
        "user",
        user.id,
        {"roles": [role.value for role in payload.roles]},
    )
    db.commit()
    db.refresh(user)
    return _user_to_schema(db, user)


@router.get("/audit-log")
def list_audit_log(
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[dict]:
    assert_roles(current_user.context, [RoleName.INFRA_ADMIN])
    rows = db.execute(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit)).scalars().all()
    return [
        {
            "id": row.id,
            "actor_user_id": row.actor_user_id,
            "action": row.action,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "details": row.details,
            "created_at": row.created_at,
        }
        for row in rows
    ]
