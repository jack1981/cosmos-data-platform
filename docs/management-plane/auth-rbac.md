# Auth & RBAC

## Authentication
- JWT-based auth with:
  - short-lived **access token**
  - longer-lived **refresh token**
- Login endpoint: `POST /api/v1/auth/login`
- Refresh endpoint: `POST /api/v1/auth/refresh`

## Roles
- `INFRA_ADMIN`
  - Full access: users/roles/audit and all pipeline/run operations.
- `PIPELINE_DEV`
  - Create/edit pipelines, create draft versions, submit review, trigger runs.
- `AIOPS_ENGINEER`
  - Read pipeline definitions, inspect run health/logs/metrics, rerun/stop runs.

## Pipeline Access Model
- Ownership by user and optional owner team.
- Additional team shares via `pipeline_shares` (`READ`, `WRITE`, `OWNER`).
- API enforces access checks server-side.

## Audit
- Mutating actions write entries into `audit_log`.
