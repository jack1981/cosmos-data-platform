from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.api.router import api_router
from app.core.config import get_settings
from app.core.logging_config import configure_logging
from app.db.session import SessionLocal
from app.services.seed import seed_defaults

settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(
    title="PipelineForge Management API",
    version="0.1.0",
    description="Control plane for PipelineForge pipeline metadata and execution.",
    openapi_tags=[
        {"name": "auth", "description": "Authentication and token lifecycle"},
        {"name": "pipelines", "description": "Pipeline metadata, versions, review, publishing"},
        {"name": "runs", "description": "Pipeline run control, events, and logs"},
        {"name": "admin", "description": "Users, roles, and audit trail"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.api_v1_prefix)


@app.on_event("startup")
def on_startup() -> None:
    with SessionLocal() as db:
        seed_defaults(db)


@app.get("/healthz", tags=["health"])
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz", tags=["health"])
def readyz() -> dict[str, str]:
    with SessionLocal() as db:
        db.execute(text("SELECT 1"))
    return {"status": "ready"}
