from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.models.enums import RoleName


class UserCreate(BaseModel):
    email: str
    full_name: str
    password: str
    roles: list[RoleName]


class UserRead(BaseModel):
    id: str
    email: str
    full_name: str
    roles: list[RoleName]
    is_active: bool
    created_at: datetime
