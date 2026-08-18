#!/usr/bin/env bash
# One image, several process roles. `command:` in docker-compose picks the role.
# Valid roles: consumer
#
# Consumers are separate PROCESSES from the API, not background tasks inside it.
# If a consumer ran in the API process, scaling the API to N replicas would put N
# members in the consumer group and rebalance on every deploy.

set -euo pipefail

ROLE="${1:-consumer}"

if [[ "${RUN_MIGRATIONS:-false}" == "true" ]]; then
  echo "[entrypoint] running alembic migrations"
  alembic upgrade head
fi

case "$ROLE" in
  consumer) exec python -m app.consumer ;;
  *)
    echo "[entrypoint] unknown role: $ROLE (expected: consumer)" >&2
    exit 1
    ;;
esac
