from __future__ import annotations

from functools import lru_cache

from common.settings import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "auth-service"

    # ---- admin bootstrap ----------------------------------------------------
    # THIS is how the first admin exists. Signup can never create one.
    bootstrap_admin_email: str | None = None
    bootstrap_admin_password: str | None = None
    bootstrap_admin_name: str = "Bootstrap Admin"

    # ---- login throttling ---------------------------------------------------
    login_max_attempts: int = 5
    login_attempt_window_seconds: int = 900  # 15 minutes

    # ---- password policy ----------------------------------------------------
    password_min_length: int = 8


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
