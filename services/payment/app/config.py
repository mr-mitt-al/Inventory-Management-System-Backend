from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "payment-service"

    # ---- mock gateway behaviour ---------------------------------------------
    # Raise to demonstrate the compensation path without a specific test card.
    # Deterministic per order_id, not random - see mock_gateway.py.
    mock_failure_rate: float = 0.0
    mock_latency_ms: int = 500
    mock_timeout_delay_ms: int = 2000


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
