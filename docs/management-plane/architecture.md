# Architecture

## Components
- **Web UI (`apps/web`)**: Next.js, Tailwind v4, shadcn-style components.
- **Management API (`apps/management_api`)**: FastAPI control plane for metadata, auth, runs.
- **PostgreSQL**: stores users, pipelines, versions, runs, and audit events.

## Runtime Flow
1. User authenticates to FastAPI via JWT (access + refresh).
2. UI creates/edits a pipeline draft JSON spec.
3. API validates linear-chain DAG constraints and stores immutable pipeline version rows.
4. Publish marks one active version.
5. Trigger run enqueues run execution in background runner.
6. Runner materializes stage chain and executes:
   - via Xenna adapter when runtime import succeeds, or
   - via local linear simulation fallback for local dev safety.
7. API exposes runs/events/log-stream/metrics summary to UI.

## Integration Boundary
- Existing `cosmos_xenna.pipelines.v1` APIs are unchanged.
- Management plane is additive and decoupled.
