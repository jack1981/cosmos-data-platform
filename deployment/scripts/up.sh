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

docker compose \
  --env-file "${ENV_FILE}" \
  -f "${ROOT_DIR}/deployment/compose/docker-compose.yml" \
  -f "${ROOT_DIR}/deployment/compose/docker-compose.ray-local.yml" \
  up -d --build

echo "Local stack is starting."
echo "API: http://localhost:8000/healthz"
echo "Web: http://localhost:3000"
echo "Ray dashboard: http://localhost:8265"
