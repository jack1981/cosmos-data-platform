# Repository Cleanup / Refactor / Rebrand Summary

## Phase 0 — Baseline & Safety

### Baseline snapshot
- Baseline timestamp (UTC): `2026-02-13T19:07:29Z`
- Baseline tracked files: `225` (`git ls-files | wc -l`)
- Baseline status: dirty worktree with existing local edits; no unrelated rollback performed.

### Baseline LOC method
- `cloc` was unavailable.
- Deterministic script added: `scripts/loc.py`
- Measurement command: `python3 scripts/loc.py`
- Exclusions: lock files, virtualenvs, node_modules, target/build artifacts, binaries, db files, vendored/generated paths.

### Baseline LOC result
- `TOTAL_LOC = 42660`

### Baseline safety validation
- Command: `PYTHONPATH=apps/management_api .venv-management/bin/pytest -q apps/management_api/tests/test_schema_validation.py`
- Result: passed.

## Phase 1 — Audit

### Entrypoints before cleanup
- API server: `apps/management_api/app/main.py`
- Web app: `apps/web` (Next.js)
- Runtime helper scripts under `deployment/scripts/`
- Multiple deployment paths (compose overlays + k8s manifests + helper scripts)

### Build/config map before cleanup
- No canonical `Makefile`
- Multiple shell wrappers for build/up/down/smoke/e2e
- Compose defined under `deployment/compose/` (two-file stack)
- Three Dockerfiles (API/web/runtime)
- No active workflows under `.github/workflows/`
- Duplicated env templates (`.env.example` and `deployment/.env.example`)

### Main duplication/dead-code hotspots identified
- Large legacy upstream package trees not used by current API/web execution path.
- Redundant orchestration scripts duplicating Docker Compose commands.
- Legacy migration/template scaffolding and related tests.
- Overlapping docs with stale/competing run instructions.

## Phase 2 — High-impact architecture simplification

### Changes
- Removed optional dual-runner branch in API execution service:
  - Deleted adapter path and unified on one local linear execution path for sample-mode runs.
- Consolidated dataset stage module naming:
  - legacy dataset stage module renamed to `dataset_stages.py`
  - Removed legacy fallback node stage tied to legacy migration input format.
- Removed legacy migration compiler stack and fixtures.
- Replaced oversized template seeding logic with a smaller, explicit PipelineForge starter-template seed implementation.

### Net effect
- Fewer execution paths, less indirection, smaller maintenance surface.

## Phase 3 — Build-system cleanup

### Changes
- Added canonical `Makefile` with required targets:
  - `help`, `fmt`, `lint`, `test`, `build`, `run`, `docker-build`, `docker-run`, `clean`
- Standardized Compose to one root file:
  - Added root `docker-compose.yml`
  - Removed compose overlays and script wrappers.
- Simplified Docker assets:
  - Kept only API/web Dockerfiles
  - Removed runtime Dockerfile and runtime requirements.
- Added minimal CI workflow aligned to Make targets:
  - `.github/workflows/ci.yml`

## Phase 4 — Dead code/config cleanup

### Removed or neutralized legacy surfaces
- Deleted legacy deployment scripts and k8s manifests.
- Deleted legacy migration tests and migration source files.
- Deleted stale duplicated docs.
- Removed obsolete API startup sample-prep script and startup hook.
- Neutralized legacy upstream code trees not used by the active API/web stack.

## Phase 5 — Rebrand to PipelineForge

### Applied across user-facing/config surfaces
- Project/docs naming updated to `PipelineForge`.
- Compose project/image defaults renamed (`pipelineforge/*`).
- Default local user emails renamed to `@pipelineforge.local`.
- Runtime env key renamed to `PIPELINEFORGE_DATASET_STORAGE_OPTIONS_JSON`.
- Web local-storage keys/labels and template naming updated.

### Compatibility
- Dataset storage options loader keeps fallback support for legacy env key in code path.

## Phase 6 — README rewrite

- Rewrote `README.md` to match current repository reality:
  - overview
  - features
  - architecture diagram
  - quickstart
  - local dev workflow
  - docker workflow
  - env/config reference
  - troubleshooting
  - migration notes

## Phase 7 — Validation and LOC gate

### Validation commands
- `make fmt`
- `make lint`
- `make test`
- `make build`
- Legacy-name scan:
  - keyword scan across source text returned no hits

### LOC measurement (same method as baseline)
- Final timestamp (UTC): `2026-02-13T19:29:41Z`
- Final command: `python3 scripts/loc.py`
- Final `TOTAL_LOC = 11477`

### LOC gate result
- Baseline LOC: `42660`
- Final LOC: `11477`
- Reduction: `31183` lines (`73.10%`)
- Gate (`>=20%`) status: **PASSED**

## Top 5 reductions (why)

1. `src/` (`-15445 LOC`): legacy upstream Rust implementation removed from active stack.
2. Legacy upstream Python package tree (`-14426 LOC`): removed from active stack.
3. `deployment/` (`-1349 LOC`): removed k8s + runtime/script sprawl; kept minimal API/web Docker assets.
4. `examples/` (`-246 LOC`): removed stale legacy sample not aligned with current run path.
5. `docs/` (`-187 LOC`): removed stale duplicated docs replaced by one authoritative README.

## Canonical developer commands

- Format: `make fmt`
- Lint: `make lint`
- Test: `make test`
- Build: `make build`
- Run API locally: `make run`
- Docker build: `make docker-build`
- Docker run: `make docker-run`
