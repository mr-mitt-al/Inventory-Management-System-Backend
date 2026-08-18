"""Redis cache for product reads.

Catalog is the read-heavy service in the system: every page view hits it, while
writes are occasional admin edits. Two cache layers:

  product:{id}      product detail, TTL 5 min
  listing:{hash}    a listing/search result page, TTL 60s

Invalidation happens on admin writes AND on `inventory.stock.changed`, so a
product page reflects a stock movement within one event rather than one TTL.

Every operation fails open. A Redis outage must degrade this service to "slower"
rather than "down" - it is a cache, not a database.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

PRODUCT_PREFIX = "product:"
LISTING_PREFIX = "listing:"


class CatalogCache:
    def __init__(
        self,
        redis: Redis | None,
        *,
        product_ttl: int = 300,
        listing_ttl: int = 60,
        enabled: bool = True,
    ) -> None:
        self._redis = redis
        self._product_ttl = product_ttl
        self._listing_ttl = listing_ttl
        self._enabled = enabled and redis is not None

    # ------------------------------------------------------------------ product
    async def get_product(self, product_id: str) -> dict[str, Any] | None:
        return await self._get(f"{PRODUCT_PREFIX}{product_id}")

    async def set_product(self, product_id: str, payload: dict[str, Any]) -> None:
        await self._set(f"{PRODUCT_PREFIX}{product_id}", payload, self._product_ttl)

    async def invalidate_product(self, product_id: str) -> None:
        """Called on admin edits and on stock changes."""
        await self._delete(f"{PRODUCT_PREFIX}{product_id}")
        # A product's price or stock changed, so cached listing pages containing
        # it are wrong too. Listings are cheap to rebuild and their TTL is short,
        # so dropping all of them beats tracking which pages held which product.
        await self.invalidate_listings()

    # ------------------------------------------------------------------ listing
    @staticmethod
    def listing_key(params: dict[str, Any]) -> str:
        """Stable key from query parameters.

        Sorted keys matter: ``?page=1&size=20`` and ``?size=20&page=1`` are the
        same query and must not occupy two cache entries.
        """
        canonical = json.dumps(params, sort_keys=True, default=str)
        digest = hashlib.sha256(canonical.encode()).hexdigest()[:24]
        return f"{LISTING_PREFIX}{digest}"

    async def get_listing(self, key: str) -> dict[str, Any] | None:
        return await self._get(key)

    async def set_listing(self, key: str, payload: dict[str, Any]) -> None:
        await self._set(key, payload, self._listing_ttl)

    async def invalidate_listings(self) -> None:
        if not self._enabled or self._redis is None:
            return
        try:
            # scan_iter, not KEYS: KEYS blocks the single-threaded server for the
            # whole scan, which on a large keyspace stalls every other client.
            keys = [key async for key in self._redis.scan_iter(match=f"{LISTING_PREFIX}*", count=200)]
            if keys:
                await self._redis.delete(*keys)
                logger.debug("listing cache invalidated", extra={"keys": len(keys)})
        except Exception:
            logger.warning("failed to invalidate listing cache", exc_info=True)

    # ----------------------------------------------------------------- internals
    async def _get(self, key: str) -> dict[str, Any] | None:
        if not self._enabled or self._redis is None:
            return None
        try:
            raw = await self._redis.get(key)
        except Exception:
            logger.warning("cache read failed, falling through to db", exc_info=True)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Corrupt or stale-format entry: drop it rather than 500.
            await self._delete(key)
            return None

    async def _set(self, key: str, payload: dict[str, Any], ttl: int) -> None:
        if not self._enabled or self._redis is None:
            return
        try:
            await self._redis.set(key, json.dumps(payload, default=str), ex=ttl)
        except Exception:
            logger.warning("cache write failed", exc_info=True)

    async def _delete(self, key: str) -> None:
        if not self._enabled or self._redis is None:
            return
        try:
            await self._redis.delete(key)
        except Exception:
            logger.warning("cache delete failed", exc_info=True)
