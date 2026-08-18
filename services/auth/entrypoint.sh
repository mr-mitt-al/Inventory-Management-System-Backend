#!/usr/bin/env bash
# One image, several process roles. `command:` in docker-compose picks the role.
#
#   api     -> the FastAPI HTTP process
#   outbox  -> the transactional outbox publisher
#
# Consumers and the API are separate PROCESSES on purpose. If consumers ran
# inside the API process, scaling the API to N replicas would put N members in
# the consumer group and trigger a rebalance on every deploy.

set -euo pipefail

ROLE="${1:-api}"

if [[ "${RUN_MIGRATIONS:-false}" == "true" ]]; then
  echo "[entrypoint] running alembic migrations"
  alembic upgrade head
fi

case "$ROLE" in
  api)
    echo "[entrypoint] starting api"
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --no-access-log
    ;;
  outbox)
    echo "[entrypoint] starting outbox publisher"
    exec python -m app.outbox_publisher
    ;;
  *)
    echo "[entrypoint] unknown role: $ROLE (expected: api | outbox)" >&2
    exit 1
    ;;
esac
