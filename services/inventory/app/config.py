from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "inventory-service"

    # How long stock is held for an unpaid order. Long enough for a payment to
    # complete, short enough that a crashed saga does not strand stock for hours.
    reservation_ttl_minutes: int = 15
    sweeper_interval_seconds: int = 60
    sweeper_batch_size: int = 100

    default_low_stock_threshold: int = 10


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
