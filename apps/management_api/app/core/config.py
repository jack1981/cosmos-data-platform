from __future__ import annotations

import base64
import hashlib
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "cosmos-xenna-management-api"
    api_v1_prefix: str = "/api/v1"
    environment: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"

    secret_key: str = "dev-change-me"
    secret_encryption_key: str | None = None
    access_token_expire_minutes: int = 30
    refresh_token_expire_minutes: int = 60 * 24 * 7

    database_url: str = "postgresql+psycopg2://xenna:xenna@postgres:5432/xenna_management"
    ray_mode: Literal["local", "k8s"] = "local"
    ray_address: str = "auto"
    runner_max_workers: int = 4

    frontend_origin: str = "http://localhost:3000"

    default_admin_email: str = "admin@xenna.local"
    default_admin_password: str = "Admin123!"
    default_dev_email: str = "dev@xenna.local"
    default_dev_password: str = "Dev123!"
    default_aiops_email: str = "aiops@xenna.local"
    default_aiops_password: str = "Aiops123!"

    @property
    def cors_origins(self) -> list[str]:
        return [self.frontend_origin]

    @property
    def effective_encryption_key(self) -> bytes:
        if self.secret_encryption_key:
            return self.secret_encryption_key.encode("utf-8")

        digest = hashlib.sha256(self.secret_key.encode("utf-8")).digest()
        return base64.urlsafe_b64encode(digest)


@lru_cache
def get_settings() -> Settings:
    return Settings()
