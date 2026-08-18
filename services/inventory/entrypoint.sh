#!/usr/bin/env bash
# One image, several process roles. `command:` in docker-compose picks the role.
# Valid roles: api | consumer | outbox | sweeper | seed
#
# Consumers are separate PROCESSES from the API, not background tasks inside it.
# If a consumer ran in the API process, scaling the API to N replicas would put N
# members in the consumer group and rebalance on every deploy.

set -euo pipefail

ROLE="${1:-api}"

if [[ "${RUN_MIGRATIONS:-false}" == "true" ]]; then
  echo "[entrypoint] running alembic migrations"
  alembic upgrade head
fi

case "$ROLE" in
  api)      exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --no-access-log ;;
  consumer) exec python -m app.consumer ;;
  outbox)   exec python -m app.outbox_publisher ;;
  sweeper)  exec python -m app.sweeper ;;
  seed)     exec python -m app.seed ;;
  *)
    echo "[entrypoint] unknown role: $ROLE (expected: api | consumer | outbox | sweeper | seed)" >&2
    exit 1
    ;;
esac
