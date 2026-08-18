"""Shared FastAPI application factory.

Every service gets identical middleware order, exception handling, CORS, metrics
and health endpoints. Wiring this per service is how six services end up with
five different error response shapes.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common.db.session import Database
from common.errors import register_exception_handlers
from common.observability.health import ReadinessCheck, build_health_router
from common.observability.metrics import metrics_router
from common.observability.middleware import CorrelationIdMiddleware

# The Vite dev server. Tightened via env in anything resembling production.
DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
]


def create_app(
    *,
    service_name: str,
    title: str,
    description: str = "",
    version: str = "0.1.0",
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[Any]] | None = None,
    db: Database | None = None,
    readiness_checks: dict[str, ReadinessCheck] | None = None,
    cors_origins: list[str] | None = None,
) -> FastAPI:
    app = FastAPI(
        title=title,
        description=description,
        version=version,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins or DEFAULT_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Correlation-Id"],
    )
    app.add_middleware(CorrelationIdMiddleware)

    register_exception_handlers(app)

    app.include_router(build_health_router(service_name=service_name, db=db,
                                          extra_checks=readiness_checks))
    app.include_router(metrics_router)

    return app
