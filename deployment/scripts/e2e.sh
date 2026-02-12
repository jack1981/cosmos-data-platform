#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deployment/.env"
EXAMPLE_ENV_FILE="${ROOT_DIR}/deployment/.env.example"

mode="local"
skip_bootstrap="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      mode="${2:-}"
      shift 2
      ;;
    --skip-bootstrap)
      skip_bootstrap="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--mode local|k8s] [--skip-bootstrap]"
      exit 1
      ;;
  esac
done

if [[ "${mode}" != "local" && "${mode}" != "k8s" ]]; then
  echo "Unsupported mode: ${mode}. Use local or k8s."
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${EXAMPLE_ENV_FILE}" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.example"
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

E2E_EMAIL="${E2E_EMAIL:-${DEFAULT_DEV_EMAIL:-dev@xenna.local}}"
E2E_PASSWORD="${E2E_PASSWORD:-${DEFAULT_DEV_PASSWORD:-Dev123!}}"
E2E_TIMEOUT_SECONDS="${E2E_TIMEOUT_SECONDS:-300}"
E2E_POLL_SECONDS="${E2E_POLL_SECONDS:-2}"
E2E_PIPELINE_ID="${E2E_PIPELINE_ID:-}"

API_BASE=""
PF_PID=""

cleanup() {
  if [[ -n "${PF_PID}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

extract_json_field() {
  local field="$1"
  python3 - "${field}" <<'PY'
import json
import sys

field = sys.argv[1]
payload = sys.stdin.read()
if not payload.strip():
    raise SystemExit("Empty response body (expected JSON)")
try:
    obj = json.loads(payload)
except json.JSONDecodeError as exc:
    print(f"Invalid JSON response: {exc}", file=sys.stderr)
    print(payload[:800], file=sys.stderr)
    raise SystemExit(1) from exc
for part in field.split('.'):
    if part.isdigit():
        obj = obj[int(part)]
    else:
        obj = obj[part]
print(obj)
PY
}

select_first_pipeline_id() {
  python3 <<'PY'
import json
import sys

payload = sys.stdin.read()
if not payload.strip():
    raise SystemExit("Empty pipeline list response")
items = json.loads(payload)
if not isinstance(items, list) or not items:
    raise SystemExit("No pipelines returned by API. Seed data may be missing.")
print(items[0]["id"])
PY
}

api_request_json() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  local token="${4:-}"

  local -a curl_args
  curl_args=(-sS -X "${method}" "${url}" -H "accept: application/json")
  if [[ -n "${token}" ]]; then
    curl_args+=(-H "Authorization: Bearer ${token}")
  fi
  if [[ -n "${body}" ]]; then
    curl_args+=(-H "content-type: application/json" -d "${body}")
  fi
  curl_args+=(-w $'\n%{http_code}')

  local response http_code response_body
  response="$(curl "${curl_args[@]}" || true)"
  http_code="${response##*$'\n'}"
  response_body="${response%$'\n'*}"

  if [[ ! "${http_code}" =~ ^[0-9]+$ ]]; then
    echo "Request failed for ${method} ${url}" >&2
    echo "${response}" >&2
    return 1
  fi

  if (( http_code < 200 || http_code >= 300 )); then
    echo "HTTP ${http_code} for ${method} ${url}" >&2
    if [[ -n "${response_body}" ]]; then
      echo "${response_body}" | head -c 1000 >&2
      echo >&2
    fi
    return 1
  fi

  if [[ -z "${response_body}" ]]; then
    echo "Empty HTTP ${http_code} response for ${method} ${url}" >&2
    return 1
  fi

  printf '%s' "${response_body}"
}

wait_for_api_health() {
  local base="$1"
  local health_url="${base%/api/v1}/healthz"
  local attempts=20
  local sleep_s=2

  for ((i=1; i<=attempts; i++)); do
    if curl -fsS "${health_url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${sleep_s}"
  done

  echo "API health check failed at ${health_url}" >&2
  return 1
}

login_and_get_token() {
  local api="$1"
  api_request_json "POST" "${api}/auth/login" \
    "{\"email\":\"${E2E_EMAIL}\",\"password\":\"${E2E_PASSWORD}\"}" \
    | extract_json_field "access_token"
}

if [[ "${skip_bootstrap}" != "true" ]]; then
  if [[ "${mode}" == "local" ]]; then
    "${SCRIPT_DIR}/build.sh"
    "${SCRIPT_DIR}/up.sh"
    "${SCRIPT_DIR}/smoke.sh"
  else
    "${SCRIPT_DIR}/k8s-deploy.sh"
    "${SCRIPT_DIR}/k8s-smoke.sh"
  fi
fi

if [[ "${mode}" == "local" ]]; then
  API_BASE="http://127.0.0.1:8000/api/v1"
else
  NS="${K8S_NAMESPACE:-cosmos-xenna}"
  kubectl -n "${NS}" port-forward svc/api 18000:8000 >/tmp/cosmos-e2e-api-pf.log 2>&1 &
  PF_PID=$!
  sleep 4
  API_BASE="http://127.0.0.1:18000/api/v1"
fi

wait_for_api_health "${API_BASE}"

echo "Logging in as ${E2E_EMAIL}"
TOKEN="$(login_and_get_token "${API_BASE}")"

if [[ -z "${E2E_PIPELINE_ID}" ]]; then
  echo "Resolving pipeline id from /pipelines"
  E2E_PIPELINE_ID="$(api_request_json "GET" "${API_BASE}/pipelines" "" "${TOKEN}" | select_first_pipeline_id)"
fi

if [[ -z "${E2E_PIPELINE_ID}" ]]; then
  echo "No pipeline id found. Set E2E_PIPELINE_ID in deployment/.env."
  exit 1
fi

echo "Triggering run for pipeline ${E2E_PIPELINE_ID}"
RUN_ID="$(api_request_json "POST" "${API_BASE}/runs/trigger" \
  "{\"pipeline_id\":\"${E2E_PIPELINE_ID}\",\"trigger_type\":\"manual\"}" \
  "${TOKEN}" | extract_json_field "id")"

echo "Run id: ${RUN_ID}"

deadline=$(( $(date +%s) + E2E_TIMEOUT_SECONDS ))
status=""
while [[ $(date +%s) -lt ${deadline} ]]; do
  status="$(api_request_json "GET" "${API_BASE}/runs/${RUN_ID}" "" "${TOKEN}" | extract_json_field "status")"
  echo "Run status: ${status}"

  if [[ "${status}" == "SUCCEEDED" ]]; then
    break
  fi
  if [[ "${status}" == "FAILED" || "${status}" == "STOPPED" ]]; then
    echo "Run terminated with status ${status}"
    api_request_json "GET" "${API_BASE}/runs/${RUN_ID}/events" "" "${TOKEN}" | python3 -m json.tool || true
    exit 1
  fi

  sleep "${E2E_POLL_SECONDS}"
done

if [[ "${status}" != "SUCCEEDED" ]]; then
  echo "Run did not complete in ${E2E_TIMEOUT_SECONDS}s"
  api_request_json "GET" "${API_BASE}/runs/${RUN_ID}/events" "" "${TOKEN}" | python3 -m json.tool || true
  exit 1
fi

echo "Run succeeded. Fetching summary/events."
api_request_json "GET" "${API_BASE}/runs/${RUN_ID}/metrics-summary" "" "${TOKEN}" | python3 -m json.tool
api_request_json "GET" "${API_BASE}/runs/${RUN_ID}/events" "" "${TOKEN}" | python3 -m json.tool

if [[ "${mode}" == "local" ]]; then
  docker compose \
    --env-file "${ENV_FILE}" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.yml" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.ray-local.yml" \
    exec -T runtime python deployment/scripts/smoke_test.py --component runtime
else
  NS="${K8S_NAMESPACE:-cosmos-xenna}"
  RUNTIME_POD="$(kubectl -n "${NS}" get pod -l app=runtime -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "${NS}" exec "${RUNTIME_POD}" -- python deployment/scripts/smoke_test.py --component runtime
fi

echo "E2E test passed (mode=${mode}, run_id=${RUN_ID})"
