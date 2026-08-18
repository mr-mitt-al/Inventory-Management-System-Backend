"""Redis-backed login throttle.

Keyed by email rather than IP: a credential-stuffing run against one account
comes from many IPs, and NAT means many legitimate users share one IP.
"""

from __future__ import annotations

import logging

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class LoginRateLimiter:
    def __init__(self, redis: Redis, *, max_attempts: int, window_seconds: int) -> None:
        self._redis = redis
        self._max = max_attempts
        self._window = window_seconds

    @staticmethod
    def _key(email: str) -> str:
        return f"login:attempts:{email.lower()}"

    async def check(self, email: str) -> None:
        """Raise if this email is currently locked out."""
        from common.errors import RateLimitedError

        try:
            count = await self._redis.get(self._key(email))
        except Exception:
            # Redis being down must not make login impossible. Fail open on the
            # throttle, since the password check itself is still enforced.
            logger.warning("rate limiter unavailable, allowing attempt", exc_info=True)
            return

        if count is not None and int(count) >= self._max:
            ttl = await self._redis.ttl(self._key(email))
            raise RateLimitedError(
                "too many failed login attempts, try again later",
                details={"retry_after_seconds": max(ttl, 0)},
            )

    async def record_failure(self, email: str) -> None:
        key = self._key(email)
        try:
            pipe = self._redis.pipeline()
            pipe.incr(key)
            # Set the TTL only when creating the counter, so the window is fixed
            # from the first failure instead of sliding forward on every attempt
            # (which would lock an account out indefinitely under a slow attack).
            pipe.expire(key, self._window, nx=True)
            await pipe.execute()
        except Exception:
            logger.warning("failed to record login failure", exc_info=True)

    async def reset(self, email: str) -> None:
        try:
            await self._redis.delete(self._key(email))
        except Exception:
            logger.warning("failed to reset login attempts", exc_info=True)
