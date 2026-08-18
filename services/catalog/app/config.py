from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "catalog-service"

    # ---- caching ------------------------------------------------------------
    product_cache_ttl_seconds: int = 300  # product detail
    listing_cache_ttl_seconds: int = 60   # search/listing results
    cache_enabled: bool = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
