from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///./management_api_test.db")
os.environ.setdefault("SECRET_KEY", "test-secret-key")
os.environ.setdefault("DEFAULT_ADMIN_EMAIL", "admin@xenna.local")
os.environ.setdefault("DEFAULT_ADMIN_PASSWORD", "Admin123!")
os.environ.setdefault("DEFAULT_DEV_EMAIL", "dev@xenna.local")
os.environ.setdefault("DEFAULT_DEV_PASSWORD", "Dev123!")
os.environ.setdefault("DEFAULT_AIOPS_EMAIL", "aiops@xenna.local")
os.environ.setdefault("DEFAULT_AIOPS_PASSWORD", "Aiops123!")

from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.services.seed import seed_defaults

TEST_ENGINE = create_engine(os.environ["DATABASE_URL"], connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(bind=TEST_ENGINE, autocommit=False, autoflush=False)


@pytest.fixture(autouse=True)
def reset_database() -> Generator[None, None, None]:
    Base.metadata.drop_all(bind=TEST_ENGINE)
    Base.metadata.create_all(bind=TEST_ENGINE)
    with TestingSessionLocal() as db:
        seed_defaults(db)
    yield


def override_get_db() -> Generator[Session, None, None]:
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as test_client:
        yield test_client
