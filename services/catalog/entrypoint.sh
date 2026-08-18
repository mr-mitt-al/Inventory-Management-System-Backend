#!/usr/bin/env bash
# One image, two process roles:
#   api       -> FastAPI HTTP process
#   consumer  -> kafka consumer (inventory.stock.changed)
#
# The consumer is a separate PROCESS, not a background task inside the API. If
# it lived in the API process, scaling the API to N replicas would put N members
# in the consumer group and trigger a rebalance on every deploy.

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
  consumer)
    echo "[entrypoint] starting consumer"
    exec python -m app.consumer
    ;;
  outbox)
    echo "[entrypoint] starting outbox publisher"
    exec python -m app.outbox_publisher
    ;;
  seed)
    echo "[entrypoint] seeding demo data"
    exec python -m app.seed
    ;;
  *)
    echo "[entrypoint] unknown role: $ROLE (expected: api | consumer | seed)" >&2
    exit 1
    ;;
esac
