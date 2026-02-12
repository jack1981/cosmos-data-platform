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
API_IMAGE="${API_IMAGE:-cosmos-xenna/api:local}"
WEB_IMAGE="${WEB_IMAGE:-cosmos-xenna/web:local}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-cosmos-xenna/runtime:local}"

if [[ "${NS}" == "cosmos-xenna" ]]; then
  kubectl apply -f "${ROOT_DIR}/deployment/k8s/app/namespace.yaml"
else
  kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -
fi
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/ray"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/configmap.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/secret.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/postgres.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/redis.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/api.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/web.yaml"
kubectl -n "${NS}" apply -f "${ROOT_DIR}/deployment/k8s/app/runtime.yaml"

kubectl -n "${NS}" set image deployment/api api="${API_IMAGE}"
kubectl -n "${NS}" set image deployment/web web="${WEB_IMAGE}"
kubectl -n "${NS}" set image deployment/runtime runtime="${RUNTIME_IMAGE}"

kubectl -n "${NS}" rollout status deployment/ray-head --timeout=180s
kubectl -n "${NS}" rollout status deployment/ray-worker --timeout=180s
kubectl -n "${NS}" rollout status deployment/postgres --timeout=180s
kubectl -n "${NS}" rollout status deployment/api --timeout=180s
kubectl -n "${NS}" rollout status deployment/web --timeout=180s
kubectl -n "${NS}" rollout status deployment/runtime --timeout=180s

echo "Kubernetes deploy complete in namespace ${NS}"
