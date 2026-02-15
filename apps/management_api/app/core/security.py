from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography.fernet import Fernet
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import get_settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class TokenError(Exception):
    pass


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def _create_token(
    subject: str, token_type: str, expires_delta_minutes: int, extra_claims: dict[str, Any] | None = None
) -> str:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "sub": subject,
        "type": token_type,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_delta_minutes)).timestamp()),
    }
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, settings.secret_key, algorithm="HS256")


def create_access_token(subject: str, roles: list[str]) -> str:
    settings = get_settings()
    return _create_token(subject, "access", settings.access_token_expire_minutes, {"roles": roles})


def create_refresh_token(subject: str) -> str:
    settings = get_settings()
    return _create_token(subject, "refresh", settings.refresh_token_expire_minutes)


def decode_token(token: str) -> dict[str, Any]:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.secret_key, algorithms=["HS256"])
    except JWTError as exc:
        raise TokenError("Invalid token") from exc


def encrypt_secret(value: str) -> str:
    settings = get_settings()
    fernet = Fernet(settings.effective_encryption_key)
    return fernet.encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str) -> str:
    settings = get_settings()
    fernet = Fernet(settings.effective_encryption_key)
    return fernet.decrypt(value.encode("utf-8")).decode("utf-8")
