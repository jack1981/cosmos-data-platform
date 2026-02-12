from fastapi import APIRouter

from app.api.routes_admin import router as admin_router
from app.api.routes_auth import router as auth_router
from app.api.routes_pipelines import router as pipelines_router
from app.api.routes_runs import router as runs_router

api_router = APIRouter()
api_router.include_router(auth_router)
api_router.include_router(pipelines_router)
api_router.include_router(runs_router)
api_router.include_router(admin_router)
