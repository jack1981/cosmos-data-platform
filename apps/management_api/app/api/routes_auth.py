from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import CurrentUser, get_current_user
from app.core.security import create_access_token, create_refresh_token, decode_token, verify_password
from app.db.session import get_db
from app.models import User
from app.schemas.auth import LoginRequest, RefreshRequest, TokenResponse, UserInfo
from app.services.rbac import get_user_roles

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    user = db.execute(select(User).where(User.email == payload.email)).scalar_one_or_none()
    if user is None or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User is inactive")

    roles = [role.value for role in get_user_roles(db, user.id)]
    return TokenResponse(
        access_token=create_access_token(user.id, roles),
        refresh_token=create_refresh_token(user.id),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh(payload: RefreshRequest, db: Session = Depends(get_db)) -> TokenResponse:
    decoded = decode_token(payload.refresh_token)
    if decoded.get("type") != "refresh":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expected refresh token")

    user_id = decoded.get("sub")
    if not isinstance(user_id, str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")

    user = db.get(User, user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive user")

    roles = [role.value for role in get_user_roles(db, user.id)]
    return TokenResponse(
        access_token=create_access_token(user.id, roles),
        refresh_token=create_refresh_token(user.id),
    )


@router.get("/me", response_model=UserInfo)
def me(current_user: CurrentUser = Depends(get_current_user), db: Session = Depends(get_db)) -> UserInfo:
    roles = get_user_roles(db, current_user.user.id)
    return UserInfo(
        id=current_user.user.id,
        email=current_user.user.email,
        full_name=current_user.user.full_name,
        roles=roles,
        is_active=current_user.user.is_active,
        created_at=current_user.user.created_at,
    )
