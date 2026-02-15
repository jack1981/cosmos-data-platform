# PipelineForge — Project One-Pager

## Overview

PipelineForge is a lightweight data-pipeline control plane for designing, versioning, and executing ETL/data-transformation pipelines. It provides a web console for pipeline building, a REST API for orchestration, and an extensible stage-template registry backed by distributed compute.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                       Browser                            │
└────────────────────────┬─────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │   Next.js Web App   │  :3000
              │  (React 19 / SSR)   │
              └──────────┬──────────┘
                         │  REST / JSON
              ┌──────────▼──────────┐
              │  FastAPI Mgmt API   │  :8000
              │   (Python 3.11)     │
              ├─────────────────────┤
              │  Runner Service     │──► Stage Template Registry
              │  (Ray distributed)  │      core · video · dataset
              └──────────┬──────────┘
                         │  SQLAlchemy / Alembic
              ┌──────────▼──────────┐
              │   PostgreSQL 16     │  :5432
              └─────────────────────┘
```

**Key patterns:** service-oriented backend, event-driven run execution, pipeline versioning (draft → review → publish), role-based access control (admin / developer / AIOps).

---

## Technology Stack

| Layer        | Technology                                          |
|--------------|-----------------------------------------------------|
| Frontend     | Next.js 16, React 19, Radix UI, Tailwind CSS        |
| API          | FastAPI, Uvicorn, Pydantic v2                        |
| Auth         | JWT (python-jose), bcrypt via passlib                |
| ORM / DB     | SQLAlchemy 2.0, Alembic migrations, PostgreSQL 16   |
| Compute      | Ray (local / distributed), Daft + Lance (Arrow)     |
| Build        | Docker multi-stage, GNU Make                         |
| CI           | GitHub Actions — lint (ruff, tsc), test (pytest, vitest), build |
| Language     | Python 3.11, TypeScript / Node 20                   |

---

## Deployment

### Local Development

```bash
cp .env.example .env          # configure secrets & ports
make docker-build              # build API + Web images
make docker-run                # start postgres, api, web via Compose
```

### Container Stack (docker-compose.yml)

| Service    | Image                        | Port | Notes                        |
|------------|------------------------------|------|------------------------------|
| `postgres` | postgres:16-alpine           | 5432 | Health-checked, persistent vol |
| `api`      | pipelineforge/api:local      | 8000 | Non-root, `/healthz` probe   |
| `web`      | pipelineforge/web:local      | 3000 | Standalone Next.js, non-root |

Both application images use **multi-stage Docker builds** with non-root runtime users (UID 10001) and built-in health checks.

### CI/CD (GitHub Actions)

Push / PR → Python 3.11 + Node 20 setup → `ruff` + `tsc` lint → `pytest` + `vitest` tests → Next.js production build.

---

## Data Model (core entities)

```
Users ─┬─ Roles / Teams
       │
Pipelines ─── PipelineVersions (draft/review/published)
       │           │
       │      PipelineShares
       │
PipelineRuns ─── RunEvents / RunLogs
       │
StageTemplates (core, video, dataset)
```

---

## Key Makefile Targets

```
make run            # local API dev server (uvicorn --reload)
make test           # pytest + vitest
make lint           # ruff + tsc
make fmt            # auto-format Python
make docker-build   # build container images
make docker-run     # docker compose up
make clean          # remove caches
```
