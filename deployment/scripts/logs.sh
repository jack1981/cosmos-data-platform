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

service="${1:-}"

if [[ -n "${service}" ]]; then
  docker compose \
    --env-file "${ENV_FILE}" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.yml" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.ray-local.yml" \
    logs -f "${service}"
else
  docker compose \
    --env-file "${ENV_FILE}" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.yml" \
    -f "${ROOT_DIR}/deployment/compose/docker-compose.ray-local.yml" \
    logs -f
fi
