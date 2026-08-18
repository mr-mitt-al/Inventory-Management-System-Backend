"""Settings shared by every service.

Each service subclasses ``BaseServiceSettings`` and adds its own fields. Values
come from environment variables, which docker-compose supplies.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- identity -----------------------------------------------------------
    service_name: str = "unnamed-service"
    env: str = "development"
    log_level: str = "INFO"

    # ---- postgres -----------------------------------------------------------
    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/postgres"
    db_pool_size: int = 10
    db_max_overflow: int = 5
    db_echo: bool = False

    # ---- kafka --------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:29092"
    consumer_max_retries: int = 3
    consumer_retry_backoff_ms: int = 1000
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100

    # ---- redis --------------------------------------------------------------
    redis_url: str = "redis://localhost:6379/0"

    # ---- jwt ----------------------------------------------------------------
    # Shared across all services: every service verifies tokens locally instead
    # of calling the auth service, so auth is never a runtime dependency.
    jwt_secret: str = "dev-only-secret-do-not-use-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    @property
    def is_production(self) -> bool:
        return self.env.lower() in {"production", "prod"}


class AuthSettings(BaseServiceSettings):
    """Slice of settings needed by the shared JWT dependencies.

    Lets ``common.auth`` validate tokens without importing any service's own
    config module.
    """


@lru_cache(maxsize=1)
def auth_settings() -> AuthSettings:
    return AuthSettings()
