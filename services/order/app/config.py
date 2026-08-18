from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "order-service"

    # ---- limits -------------------------------------------------------------
    max_items_per_order: int = 50
    max_quantity_per_item: int = 100

    # ---- SSE ----------------------------------------------------------------
    # How often the /orders/{id}/stream generator re-reads the order. Polling the
    # database rather than subscribing to Redis keeps the mechanism to one moving
    # part; the frontend also polls as a fallback, so a dropped stream is not a
    # correctness problem.
    sse_poll_interval_seconds: float = 1.5
    sse_max_duration_seconds: int = 300
    sse_heartbeat_seconds: float = 15.0


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
