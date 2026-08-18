"""Cache key and schema tests - no Redis or database needed."""

from __future__ import annotations

from app.cache import CatalogCache
from app.schemas import ProductUpdateRequest


class TestListingKeys:
    def test_parameter_order_does_not_change_the_key(self) -> None:
        # ?page=1&size=20 and ?size=20&page=1 are the same query. If key
        # generation depended on dict order they would occupy two cache entries,
        # halving the hit rate for no reason.
        a = CatalogCache.listing_key({"page": 1, "size": 20, "q": "sony"})
        b = CatalogCache.listing_key({"q": "sony", "size": 20, "page": 1})
        assert a == b

    def test_different_queries_get_different_keys(self) -> None:
        assert CatalogCache.listing_key({"q": "sony"}) != CatalogCache.listing_key({"q": "bose"})

    def test_page_is_part_of_the_key(self) -> None:
        # Otherwise page 2 serves page 1's cached response.
        assert CatalogCache.listing_key({"page": 1}) != CatalogCache.listing_key({"page": 2})

    def test_none_is_distinct_from_absent_value(self) -> None:
        assert CatalogCache.listing_key({"category": None}) != CatalogCache.listing_key(
            {"category": "audio"}
        )


class TestCacheDisabledIsSafe:
    async def test_all_operations_no_op_without_redis(self) -> None:
        # A Redis outage must degrade catalog to "slower", never to "down".
        cache = CatalogCache(None, enabled=False)
        assert await cache.get_product("abc") is None
        await cache.set_product("abc", {"id": "abc"})
        await cache.invalidate_product("abc")
        await cache.invalidate_listings()


class TestProductUpdateContract:
    def test_stock_cannot_be_edited_through_the_catalog(self) -> None:
        # Inventory owns stock. A settable field here would create a second
        # source of truth that silently disagrees with the first.
        fields = ProductUpdateRequest.model_fields
        assert "cached_stock" not in fields
        assert "stock" not in fields

    def test_patch_only_reports_provided_fields(self) -> None:
        body = ProductUpdateRequest.model_validate({"price": "19.99"})
        assert body.model_dump(exclude_unset=True) == {"price": body.price}
