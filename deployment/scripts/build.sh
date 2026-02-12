#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deployment/.env"
EXAMPLE_ENV_FILE="${ROOT_DIR}/deployment/.env.example"

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${EXAMPLE_ENV_FILE}" "${ENV_FILE}"
  echo "Created ${ENV_FILE} from .env.example"
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

API_IMAGE="${API_IMAGE:-cosmos-xenna/api:local}"
WEB_IMAGE="${WEB_IMAGE:-cosmos-xenna/web:local}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-cosmos-xenna/runtime:local}"

export DOCKER_BUILDKIT=1

echo "Building API image: ${API_IMAGE}"
docker build \
  -f "${ROOT_DIR}/deployment/docker/Dockerfile.api" \
  -t "${API_IMAGE}" \
  "${ROOT_DIR}"

echo "Building Web image: ${WEB_IMAGE}"
docker build \
  -f "${ROOT_DIR}/deployment/docker/Dockerfile.web" \
  -t "${WEB_IMAGE}" \
  "${ROOT_DIR}"

echo "Building Runtime image: ${RUNTIME_IMAGE}"
docker build \
  -f "${ROOT_DIR}/deployment/docker/Dockerfile.runtime" \
  -t "${RUNTIME_IMAGE}" \
  "${ROOT_DIR}"

"${SCRIPT_DIR}/check-image-size.sh" "${API_IMAGE}" "${WEB_IMAGE}" "${RUNTIME_IMAGE}"
