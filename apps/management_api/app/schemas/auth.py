from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.models.enums import RoleName


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class UserInfo(BaseModel):
    id: str
    email: str
    full_name: str
    roles: list[RoleName]
    is_active: bool
    created_at: datetime
