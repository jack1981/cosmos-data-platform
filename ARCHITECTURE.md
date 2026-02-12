# Cosmos-Xenna Management Plane Architecture (v1)

## Goals
- Add a Dockerized management plane and web UI without breaking existing `cosmos_xenna` APIs.
- Provide Airflow-like UX for pipeline authoring, runs, logs, and monitoring.
- Support three roles: `INFRA_ADMIN`, `PIPELINE_DEV`, `AIOPS_ENGINEER`.

## Existing Xenna Integration Surface
- Existing execution contract remains unchanged:
  - `cosmos_xenna.pipelines.v1.Stage`
  - `cosmos_xenna.pipelines.v1.StageSpec`
  - `cosmos_xenna.pipelines.v1.PipelineSpec`
  - `cosmos_xenna.pipelines.v1.PipelineConfig`
  - `cosmos_xenna.pipelines.v1.run_pipeline(...)`
- Existing execution modes remain authoritative:
  - `STREAMING`, `BATCH`, `SERVING`

## New Components
- `apps/management_api`: FastAPI control plane.
- `apps/web`: Next.js UI (Tailwind v4 + shadcn/ui-style primitives).
- `deploy/compose`: local Docker Compose stack.

## Boundaries
- **Core Xenna library is not rewritten.**
- Management API is an orchestration/metadata layer around Xenna.
- Pipeline runtime specs are stored as JSON and converted into Xenna objects at run trigger time.

## Metadata Model
- `pipelines`: logical pipeline identity and ownership metadata.
- `pipeline_versions`: immutable versioned JSON specs (`DRAFT`, `IN_REVIEW`, `PUBLISHED`, `REJECTED`).
- `pipeline_reviews`: approval/rejection records with comments.
- `pipeline_runs`: run lifecycle records (`QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `STOPPED`).
- `run_events`: structured timeline entries.
- `run_logs_index`: log pointers (inline/dev buffer or Loki query metadata).
- `users`/`roles`/`teams`: RBAC and sharing.
- `connections`: encrypted external endpoints and credentials references.
- `audit_log`: immutable action trail.
- Optional: `incidents` for AIOps annotations.

## Pipeline JSON Spec (UI Authorable)
The UI stores a stable JSON document with:
- identity: `pipeline_id`, `name`, `description`, `tags`
- ownership: `owners`, `team_ids`
- behavior: `execution_mode` (`streaming|batch|serving`)
- stages: ordered list (`stage_id`, `name`, `python_import_path` or template id, params/resources)
- io/runtime/observability sections
- metadata links (`datasets`, `models`, artifact pointers)

### DAG-like UX + Linear Core
- UI offers node/edge editing for familiarity.
- Server validation enforces a strict linear chain for v1:
  - exactly one root, one leaf
  - each intermediate node has in-degree=1 and out-degree=1
  - topological order must map to `stages[]` sequence
- Non-linear DAG remains a documented future extension.

## Runner Adapter
1. Load published `pipeline_version.spec_json`.
2. Validate schema and linear chain constraints.
3. Resolve each stage:
   - by dynamic import path (`module.submodule:ClassName`) or
   - by stage template registry entry.
4. Instantiate stage instances with typed params.
5. Build Xenna `PipelineSpec` + `PipelineConfig` (including execution mode mapping).
6. Execute via `run_pipeline` in background worker thread/process.
7. Persist run state transitions/events/log pointers for UI.

## Security
- JWT access + refresh tokens.
- Password hashing with `passlib`/bcrypt.
- Role and per-pipeline access checks in API layer.
- Sensitive connection payloads encrypted at rest with Fernet key from env (`SECRET_ENCRYPTION_KEY`).

## Observability
- Core profile:
  - DB-backed run events
  - API log streaming endpoint (SSE)
  - health checks for API/DB/Ray endpoint connectivity
- Optional `obs` compose profile:
  - Prometheus/Grafana/Loki/Promtail/Otel Collector/Jaeger
  - run log pointers reference Loki labels/query hints

## Versioning/Review Flow (v1)
- Pipeline edits always create new `DRAFT` version.
- `PIPELINE_DEV` can submit review.
- `INFRA_ADMIN` can approve/reject and publish.
- Publish sets one active version per pipeline.

## Compatibility
- Existing library users can continue using direct Python APIs unchanged.
- Management plane is additive and optional.
