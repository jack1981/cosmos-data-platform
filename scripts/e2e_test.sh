#!/usr/bin/env bash
# End-to-end smoke test for PipelineForge
# Tests: health, auth, pipelines CRUD, versions, runs
set -euo pipefail

API="http://localhost:8000"
WEB="http://localhost:3000"
PASS=0
FAIL=0
RUN_TS=$(date +%s)

green()  { printf "\033[32m✓ %s\033[0m\n" "$1"; }
red()    { printf "\033[31m✗ %s\033[0m\n" "$1"; }
header() { printf "\n\033[1;34m── %s ──\033[0m\n" "$1"; }

assert_eq() {
  local label="$1" actual="$2" expected="$3"
  if [ "$actual" = "$expected" ]; then
    green "$label"; PASS=$((PASS+1))
  else
    red "$label (expected=$expected, got=$actual)"; FAIL=$((FAIL+1))
  fi
}

assert_not_empty() {
  local label="$1" value="$2"
  if [ -n "$value" ] && [ "$value" != "null" ]; then
    green "$label"; PASS=$((PASS+1))
  else
    red "$label (value was empty/null)"; FAIL=$((FAIL+1))
  fi
}

# Helper: build JSON via python to avoid bash escaping issues
json() { python3 -c "import json,sys; print(json.dumps($1))"; }

# ── 1. Health checks ──────────────────────────────────────────────
header "Health Checks"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/healthz")
assert_eq "API /healthz returns 200" "$HTTP" "200"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/readyz")
assert_eq "API /readyz returns 200" "$HTTP" "200"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$WEB/")
assert_eq "Web UI returns 200" "$HTTP" "200"

# ── 2. Authentication ─────────────────────────────────────────────
header "Authentication"

# Login as admin
LOGIN_BODY=$(json '{"email":"admin@pipelineforge.local","password":"Admin123!"}')
RESP=$(curl -s -X POST "$API/api/v1/auth/login" \
  -H "Content-Type: application/json" -d "$LOGIN_BODY")
ACCESS_TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
assert_not_empty "Admin login returns access_token" "$ACCESS_TOKEN"

REFRESH_TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('refresh_token',''))" 2>/dev/null || echo "")
assert_not_empty "Admin login returns refresh_token" "$REFRESH_TOKEN"

AUTH="Authorization: Bearer $ACCESS_TOKEN"

# Get current user
RESP=$(curl -s "$API/api/v1/auth/me" -H "$AUTH")
EMAIL=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('email',''))" 2>/dev/null || echo "")
assert_eq "GET /auth/me returns admin email" "$EMAIL" "admin@pipelineforge.local"

# Token refresh
REFRESH_BODY=$(json "{\"refresh_token\":\"$REFRESH_TOKEN\"}")
RESP=$(curl -s -X POST "$API/api/v1/auth/refresh" \
  -H "Content-Type: application/json" -d "$REFRESH_BODY")
NEW_TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
assert_not_empty "Token refresh returns new access_token" "$NEW_TOKEN"

# Login as dev user
DEV_BODY=$(json '{"email":"dev@pipelineforge.local","password":"Dev123!"}')
RESP=$(curl -s -X POST "$API/api/v1/auth/login" \
  -H "Content-Type: application/json" -d "$DEV_BODY")
DEV_TOKEN=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
assert_not_empty "Dev login returns access_token" "$DEV_TOKEN"

# Invalid login
INVALID_BODY=$(json '{"email":"bad@nope.com","password":"wrong"}')
HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/auth/login" \
  -H "Content-Type: application/json" -d "$INVALID_BODY")
assert_eq "Invalid login returns 401" "$HTTP" "401"

# ── 3. Stage Templates ────────────────────────────────────────────
header "Stage Templates"

RESP=$(curl -s "$API/api/v1/pipelines/stage-templates" -H "$AUTH")
TMPL_COUNT=$(echo "$RESP" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
assert_not_empty "GET /stage-templates returns templates" "$TMPL_COUNT"
echo "  ($TMPL_COUNT stage templates available)"

# ── 4. Pipeline CRUD ──────────────────────────────────────────────
header "Pipeline CRUD"

# Create pipeline (external_id is required)
PIPELINE_BODY=$(json "{\"external_id\":\"e2e-test-pipe-$RUN_TS\",\"name\":\"E2E Test Pipeline\",\"description\":\"Created by e2e test\",\"tags\":[\"e2e\",\"test\"]}")
RESP=$(curl -s -w '\n%{http_code}' -X POST "$API/api/v1/pipelines" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$PIPELINE_BODY")
HTTP=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
assert_eq "POST /pipelines returns 201" "$HTTP" "201"

PIPELINE_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
assert_not_empty "Created pipeline has an id" "$PIPELINE_ID"
echo "  Pipeline ID: $PIPELINE_ID"

# List pipelines
RESP=$(curl -s "$API/api/v1/pipelines" -H "$AUTH")
COUNT=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else d.get('total',0))" 2>/dev/null || echo "0")
assert_not_empty "GET /pipelines returns results" "$COUNT"

# Get single pipeline
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines/$PIPELINE_ID" -H "$AUTH")
assert_eq "GET /pipelines/{id} returns 200" "$HTTP" "200"

# Update pipeline
UPDATE_BODY=$(json '{"description":"Updated by e2e test"}')
HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X PATCH "$API/api/v1/pipelines/$PIPELINE_ID" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$UPDATE_BODY")
assert_eq "PATCH /pipelines/{id} returns 200" "$HTTP" "200"

# Search pipeline
RESP=$(curl -s "$API/api/v1/pipelines?search=E2E" -H "$AUTH")
FOUND=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else d.get('total',0))" 2>/dev/null || echo "0")
assert_not_empty "GET /pipelines?search=E2E finds results" "$FOUND"

# ── 5. Pipeline Versions ──────────────────────────────────────────
header "Pipeline Versions"

# Create draft version (spec needs name + stages with stage_template set)
VERSION_BODY=$(json '{
  "spec": {
    "name": "E2E Test Pipeline",
    "stages": [
      {
        "stage_id": "s1",
        "name": "Constants Stage",
        "stage_template": "builtin.datafiner_add_constants",
        "params": {"constants": {"test_key": "test_value"}}
      }
    ]
  },
  "change_summary": "Initial version from e2e test"
}')
RESP=$(curl -s -w '\n%{http_code}' -X POST "$API/api/v1/pipelines/$PIPELINE_ID/versions" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$VERSION_BODY")
HTTP=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
assert_eq "POST /versions returns 201" "$HTTP" "201"

VERSION_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
assert_not_empty "Created version has an id" "$VERSION_ID"
echo "  Version ID: $VERSION_ID"

# List versions
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines/$PIPELINE_ID/versions" -H "$AUTH")
assert_eq "GET /versions returns 200" "$HTTP" "200"

# Get single version
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines/$PIPELINE_ID/versions/$VERSION_ID" -H "$AUTH")
assert_eq "GET /versions/{id} returns 200" "$HTTP" "200"

# ── 6. Version Workflow (submit -> approve -> publish) ─────────────
header "Version Workflow"

ACTION_BODY=$(json '{"comments":"e2e test"}')

HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$API/api/v1/pipelines/$PIPELINE_ID/versions/$VERSION_ID/submit-review" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$ACTION_BODY")
assert_eq "Submit for review returns 200" "$HTTP" "200"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$API/api/v1/pipelines/$PIPELINE_ID/versions/$VERSION_ID/approve" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$ACTION_BODY")
assert_eq "Approve version returns 200" "$HTTP" "200"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$API/api/v1/pipelines/$PIPELINE_ID/versions/$VERSION_ID/publish" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$ACTION_BODY")
assert_eq "Publish version returns 200" "$HTTP" "200"

# ── 7. Pipeline Runs ──────────────────────────────────────────────
header "Pipeline Runs"

# Trigger a run
TRIGGER_BODY=$(json "{\"pipeline_id\":\"$PIPELINE_ID\"}")
RESP=$(curl -s -w '\n%{http_code}' -X POST "$API/api/v1/runs/trigger" \
  -H "$AUTH" -H "Content-Type: application/json" -d "$TRIGGER_BODY")
HTTP=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP" = "201" ] || [ "$HTTP" = "200" ]; then
  green "POST /runs/trigger returns $HTTP"
  PASS=$((PASS+1))
  RUN_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
  assert_not_empty "Triggered run has an id" "$RUN_ID"
  echo "  Run ID: $RUN_ID"

  # Wait briefly for run to initialize
  sleep 2

  # Get run details
  RESP=$(curl -s "$API/api/v1/runs/$RUN_ID" -H "$AUTH")
  RUN_STATUS=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
  assert_not_empty "GET /runs/{id} returns status" "$RUN_STATUS"
  echo "  Run status: $RUN_STATUS"

  # Get run events
  HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/runs/$RUN_ID/events" -H "$AUTH")
  assert_eq "GET /runs/{id}/events returns 200" "$HTTP" "200"

  # Get run metrics summary
  HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/runs/$RUN_ID/metrics-summary" -H "$AUTH")
  assert_eq "GET /runs/{id}/metrics-summary returns 200" "$HTTP" "200"
else
  red "POST /runs/trigger returned $HTTP (body: $BODY)"
  FAIL=$((FAIL+1))
fi

# List runs
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/runs" -H "$AUTH")
assert_eq "GET /runs returns 200" "$HTTP" "200"

# List runs filtered by pipeline
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/runs?pipeline_id=$PIPELINE_ID" -H "$AUTH")
assert_eq "GET /runs?pipeline_id=... returns 200" "$HTTP" "200"

# ── 7b. Datafiner Pipeline Run ────────────────────────────────────
header "Datafiner Pipeline (Read/Write)"

# Find the seeded template_datafiner_read_write pipeline
RESP=$(curl -s "$API/api/v1/pipelines?search=Datafiner+Read+Write" -H "$AUTH")
DF_PID=$(echo "$RESP" | python3 -c "
import sys, json
pipes = json.load(sys.stdin)
items = pipes if isinstance(pipes, list) else pipes.get('items', [])
for p in items:
    if 'read_write' in p.get('external_id', ''):
        print(p['id']); break
else:
    print('')
" 2>/dev/null || echo "")
assert_not_empty "Found seeded datafiner_read_write pipeline" "$DF_PID"

if [ -n "$DF_PID" ] && [ "$DF_PID" != "null" ]; then
  echo "  Datafiner Pipeline ID: $DF_PID"

  # Trigger run on the seeded datafiner pipeline
  DF_TRIGGER=$(json "{\"pipeline_id\":\"$DF_PID\"}")
  RESP=$(curl -s -w '\n%{http_code}' -X POST "$API/api/v1/runs/trigger" \
    -H "$AUTH" -H "Content-Type: application/json" -d "$DF_TRIGGER")
  HTTP=$(echo "$RESP" | tail -1)
  BODY=$(echo "$RESP" | sed '$d')

  if [ "$HTTP" = "201" ] || [ "$HTTP" = "200" ]; then
    green "Datafiner run triggered ($HTTP)"
    PASS=$((PASS+1))
    DF_RUN_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
    assert_not_empty "Datafiner run has an id" "$DF_RUN_ID"
    echo "  Run ID: $DF_RUN_ID"

    # Poll for completion (up to 30s)
    echo "  Waiting for run to complete..."
    for _i in $(seq 1 15); do
      sleep 2
      DF_STATUS=$(curl -s "$API/api/v1/runs/$DF_RUN_ID" -H "$AUTH" | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
      if [ "$DF_STATUS" = "SUCCEEDED" ] || [ "$DF_STATUS" = "FAILED" ]; then
        break
      fi
    done
    echo "  Final status: $DF_STATUS"
    assert_eq "Datafiner run completes successfully" "$DF_STATUS" "SUCCEEDED"
  else
    red "Datafiner run trigger failed: $HTTP (body: $BODY)"
    FAIL=$((FAIL+1))
  fi
fi

# ── 7c. Text Pre-Training Curation Pipeline ──────────────────────
header "Text Pre-Training Curation Pipeline"

RESP=$(curl -s "$API/api/v1/pipelines?search=Text+Pre-Training+Dataset+Curation" -H "$AUTH")
CUR_PID=$(echo "$RESP" | python3 -c "
import sys, json
pipes = json.load(sys.stdin)
items = pipes if isinstance(pipes, list) else pipes.get('items', [])
for p in items:
    if 'text_pretraining_curation' in p.get('external_id', ''):
        print(p['id']); break
else:
    print('')
" 2>/dev/null || echo "")
assert_not_empty "Found seeded text_pretraining_curation pipeline" "$CUR_PID"

if [ -n "$CUR_PID" ] && [ "$CUR_PID" != "null" ]; then
  echo "  Curation Pipeline ID: $CUR_PID"

  # Trigger run on the seeded curation pipeline
  CUR_TRIGGER=$(json "{\"pipeline_id\":\"$CUR_PID\"}")
  RESP=$(curl -s -w '\n%{http_code}' -X POST "$API/api/v1/runs/trigger" \
    -H "$AUTH" -H "Content-Type: application/json" -d "$CUR_TRIGGER")
  HTTP=$(echo "$RESP" | tail -1)
  BODY=$(echo "$RESP" | sed '$d')

  if [ "$HTTP" = "201" ] || [ "$HTTP" = "200" ]; then
    green "Curation run triggered ($HTTP)"
    PASS=$((PASS+1))
    CUR_RUN_ID=$(echo "$BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
    assert_not_empty "Curation run has an id" "$CUR_RUN_ID"
    echo "  Run ID: $CUR_RUN_ID"

    # Poll for completion (up to 120s since 100K rows through 16 stages take longer)
    echo "  Waiting for curation run to complete..."
    for _i in $(seq 1 60); do
      sleep 2
      CUR_STATUS=$(curl -s "$API/api/v1/runs/$CUR_RUN_ID" -H "$AUTH" | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
      if [ "$CUR_STATUS" = "SUCCEEDED" ] || [ "$CUR_STATUS" = "FAILED" ]; then
        break
      fi
    done
    echo "  Final status: $CUR_STATUS"
    assert_eq "Curation run completes successfully" "$CUR_STATUS" "SUCCEEDED"
  else
    red "Curation run trigger failed: $HTTP (body: $BODY)"
    FAIL=$((FAIL+1))
  fi
fi

# ── 8. Access Control ─────────────────────────────────────────────
header "Access Control"

# Unauthenticated request
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines")
assert_eq "Unauthenticated GET /pipelines returns 401" "$HTTP" "401"

# Access summary
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines/$PIPELINE_ID/access-summary" -H "$AUTH")
if [ "$HTTP" = "200" ] || [ "$HTTP" = "404" ]; then
  green "GET /access-summary returns $HTTP"
  PASS=$((PASS+1))
else
  red "GET /access-summary returned unexpected $HTTP"
  FAIL=$((FAIL+1))
fi

# ── 9. Cleanup (delete test pipeline) ─────────────────────────────
header "Cleanup"

HTTP=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/api/v1/pipelines/$PIPELINE_ID" -H "$AUTH")
assert_eq "DELETE /pipelines/{id} returns 200 or 204" "$HTTP" "200"

# Verify deleted
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/pipelines/$PIPELINE_ID" -H "$AUTH")
assert_eq "GET deleted pipeline returns 404" "$HTTP" "404"

# ── Summary ───────────────────────────────────────────────────────
header "Results"
TOTAL=$((PASS+FAIL))
printf "Total: %d | " "$TOTAL"
printf "\033[32mPassed: %d\033[0m | " "$PASS"
printf "\033[31mFailed: %d\033[0m\n" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
echo "All tests passed!"
