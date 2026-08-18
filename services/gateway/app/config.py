from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "api-gateway"

    # ---- upstreams ----------------------------------------------------------
    auth_url: str = "http://auth-api:8000"
    catalog_url: str = "http://catalog-api:8000"
    order_url: str = "http://order-api:8000"
    inventory_url: str = "http://inventory-api:8000"
    payment_url: str = "http://payment-api:8000"

    # ---- proxy behaviour ----------------------------------------------------
    upstream_timeout_seconds: float = 30.0
    # Long, because the SSE order stream holds a connection open deliberately.
    stream_timeout_seconds: float = 360.0
    max_connections: int = 200
    max_keepalive_connections: int = 50

    # ---- rate limiting ------------------------------------------------------
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60
    # Writes are cheaper to abuse and more expensive to serve, so they get their
    # own tighter budget.
    write_rate_limit_requests: int = 30


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
