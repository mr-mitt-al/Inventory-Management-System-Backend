from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "notification-service"

    # Delivery is a logged stub. Swapping in real SMTP is a single function -
    # doing so adds nothing to the distributed-systems design, which is the point
    # of this project.
    delivery_mode: str = "log"  # log | smtp (not implemented)
    from_address: str = "no-reply@example.com"
    admin_alert_email: str = "admin@example.com"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
