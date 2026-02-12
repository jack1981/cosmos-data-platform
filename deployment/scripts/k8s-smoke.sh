#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deployment/.env"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a; source "${ENV_FILE}"; set +a
fi

NS="${K8S_NAMESPACE:-cosmos-xenna}"

kubectl -n "${NS}" wait --for=condition=available --timeout=180s deployment/ray-head
kubectl -n "${NS}" wait --for=condition=available --timeout=180s deployment/ray-worker
kubectl -n "${NS}" wait --for=condition=available --timeout=180s deployment/api
kubectl -n "${NS}" wait --for=condition=available --timeout=180s deployment/web
kubectl -n "${NS}" wait --for=condition=available --timeout=180s deployment/runtime

RUNTIME_POD="$(kubectl -n "${NS}" get pod -l app=runtime -o jsonpath='{.items[0].metadata.name}')"
kubectl -n "${NS}" exec "${RUNTIME_POD}" -- python deployment/scripts/smoke_test.py --component runtime

cleanup() {
  if [[ -n "${API_PF_PID:-}" ]]; then kill "${API_PF_PID}" >/dev/null 2>&1 || true; fi
  if [[ -n "${WEB_PF_PID:-}" ]]; then kill "${WEB_PF_PID}" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

kubectl -n "${NS}" port-forward svc/api 18000:8000 >/tmp/cosmos-api-pf.log 2>&1 &
API_PF_PID=$!
kubectl -n "${NS}" port-forward svc/web 13000:3000 >/tmp/cosmos-web-pf.log 2>&1 &
WEB_PF_PID=$!

sleep 4
python3 "${SCRIPT_DIR}/smoke_test.py" --component api --api-url "http://127.0.0.1:18000/healthz"
python3 "${SCRIPT_DIR}/smoke_test.py" --component web --web-url "http://127.0.0.1:13000/"

echo "All Kubernetes smoke checks passed"
