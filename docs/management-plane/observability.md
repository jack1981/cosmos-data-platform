# Run Telemetry

The v1 management plane keeps observability intentionally simple:

- Structured JSON logs from FastAPI stdout.
- `run_events` table stores lifecycle and per-stage events.
- In-memory log ring buffer is exposed via `GET /api/v1/runs/{run_id}/logs/stream` (SSE).
- `pipeline_runs.metrics_summary` stores small per-run metric snapshots.

No external observability stack is required for local development.
