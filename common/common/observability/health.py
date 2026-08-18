"""Liveness and readiness endpoints.

The distinction matters for compose (and later k8s):

  /health/live   the process is running. Never touches dependencies - a failing
                 liveness check means "restart me", and restarting will not fix
                 a down database.
  /health/ready  dependencies are reachable, so it is safe to route traffic here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Response, status

from common.db.session import Database

ReadinessCheck = Callable[[], Awaitable[bool]]


def build_health_router(
    *,
    service_name: str,
    db: Database | None = None,
    extra_checks: dict[str, ReadinessCheck] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/health", tags=["observability"])

    @router.get("/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "alive", "service": service_name}

    @router.get("/ready", include_in_schema=False)
    async def ready(response: Response) -> dict[str, object]:
        checks: dict[str, bool] = {}

        if db is not None:
            checks["database"] = await db.ping()

        for name, check in (extra_checks or {}).items():
            try:
                checks[name] = await check()
            except Exception:
                checks[name] = False

        healthy = all(checks.values())
        if not healthy:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

        return {
            "status": "ready" if healthy else "not_ready",
            "service": service_name,
            "checks": checks,
        }

    return router
