#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deployment/.env"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a; source "${ENV_FILE}"; set +a
fi

API_IMAGE="${1:-${API_IMAGE:-cosmos-xenna/api:local}}"
WEB_IMAGE="${2:-${WEB_IMAGE:-cosmos-xenna/web:local}}"
RUNTIME_IMAGE="${3:-${RUNTIME_IMAGE:-cosmos-xenna/runtime:local}}"

api_limit_mb=300
runtime_limit_mb=350
web_limit_mb=200

bytes_to_mb() {
  local bytes="$1"
  awk -v b="${bytes}" 'BEGIN { printf "%.1f", b / 1024 / 1024 }'
}

check_one() {
  local label="$1"
  local image="$2"
  local limit_mb="$3"

  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "[WARN] ${label}: image not found (${image})"
    return 0
  fi

  local size_bytes
  size_bytes="$(docker image inspect "${image}" --format '{{.Size}}')"
  local size_mb
  size_mb="$(bytes_to_mb "${size_bytes}")"

  local status="OK"
  if (( size_bytes > limit_mb * 1024 * 1024 )); then
    status="WARN"
  fi

  echo "[${status}] ${label}: ${image} -> ${size_mb} MB (target <= ${limit_mb} MB)"
}

echo "Checking Docker image sizes..."
check_one "API" "${API_IMAGE}" "${api_limit_mb}"
check_one "Web" "${WEB_IMAGE}" "${web_limit_mb}"
check_one "Runtime" "${RUNTIME_IMAGE}" "${runtime_limit_mb}"
