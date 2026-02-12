from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.core.security import TokenError, decode_token
from app.db.session import get_db
from app.models import RoleName, User
from app.services.rbac import AuthContext, get_user_roles

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class CurrentUser:
    user: User
    roles: list[RoleName]

    @property
    def context(self) -> AuthContext:
        return AuthContext(self.user.id, self.roles)


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    access_token: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> CurrentUser:
    token = credentials.credentials if credentials else access_token
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

    try:
        payload = decode_token(token)
    except TokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid auth token") from exc

    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expected access token")

    user_id = payload.get("sub")
    if not isinstance(user_id, str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid subject")

    user = db.get(User, user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not active")

    roles = get_user_roles(db, user.id)
    return CurrentUser(user=user, roles=roles)


def require_roles(*allowed: RoleName):
    def _guard(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not any(role in current_user.roles for role in allowed):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return current_user

    return _guard
