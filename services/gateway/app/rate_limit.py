"""Redis sliding-window rate limiter.

Keyed by the authenticated user when a token is present, and by client IP
otherwise - so one abusive account cannot exhaust the budget for everyone behind
the same NAT, and an anonymous flood is still bounded.

Fails OPEN. A Redis outage must not take the whole API down; losing rate limiting
for a few minutes is strictly better than losing service.
"""

from __future__ import annotations

import logging
import time

from redis.asyncio import Redis

from common.errors import RateLimitedError
from app.config import settings

logger = logging.getLogger(__name__)

WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


class RateLimiter:
    def __init__(self, redis: Redis | None) -> None:
        self._redis = redis

    async def check(self, *, identity: str, method: str) -> tuple[int, int]:
        """Consume one unit of budget. Returns (limit, remaining).

        Uses a sorted set as a true sliding window rather than a fixed-window
        counter: a fixed window lets a caller send 2x the limit across a window
        boundary (all of it at 0:59, all of it again at 1:01).
        """
        if not settings.rate_limit_enabled or self._redis is None:
            return 0, 0

        limit = (
            settings.write_rate_limit_requests
            if method.upper() in WRITE_METHODS
            else settings.rate_limit_requests
        )
        window = settings.rate_limit_window_seconds
        bucket = "w" if method.upper() in WRITE_METHODS else "r"
        key = f"ratelimit:{bucket}:{identity}"
        now = time.time()

        try:
            pipe = self._redis.pipeline()
            pipe.zremrangebyscore(key, 0, now - window)  # drop what left the window
            pipe.zadd(key, {f"{now}:{id(self)}": now})
            pipe.zcard(key)
            pipe.expire(key, window + 1)
            results = await pipe.execute()
            used = int(results[2])
        except Exception:
            logger.warning("rate limiter unavailable, allowing request", exc_info=True)
            return 0, 0

        if used > limit:
            raise RateLimitedError(
                "rate limit exceeded",
                details={"limit": limit, "window_seconds": window, "scope": bucket},
            )

        return limit, max(limit - used, 0)
