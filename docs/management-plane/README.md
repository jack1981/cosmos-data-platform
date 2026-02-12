# Management Plane Docs

This folder documents the new Dockerized management plane for Cosmos-Xenna.

## Contents
- [Architecture](./architecture.md)
- [Auth & RBAC](./auth-rbac.md)
- [Pipeline Spec Schema](./pipeline-spec-schema.md)
- [Run Telemetry](./observability.md)
- [Troubleshooting](./troubleshooting.md)

## Seeded learning templates
The control plane seeds three starter templates at startup so new users can learn immediately:
- Video Caption Batch
- Video Quality Review
- Video Incident Triage

These are visible on the Pipelines page and include one-click run actions. Each template writes artifact output to `/tmp/xenna_artifacts/*.jsonl` inside the API container.
