"""Catalog business logic: read-through caching and admin writes."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.outbox import enqueue_event
from common.errors import ConflictError, NotFoundError
from common.events.envelope import make_envelope
from common.events.schemas import ProductUpserted
from common.events.topics import Topics
from common.pagination import Page, PageParams
from app.cache import CatalogCache
from app.config import settings
from app.models import Outbox, Product
from app.repositories import CategoryRepository, ProductRepository
from app.schemas import (
    CategoryResponse,
    ProductCreateRequest,
    ProductResponse,
    ProductUpdateRequest,
    SortOption,
)

logger = logging.getLogger(__name__)


class CatalogService:
    def __init__(
        self,
        session: AsyncSession,
        cache: CatalogCache,
        *,
        correlation_id: UUID | None = None,
    ) -> None:
        self._session = session
        self._products = ProductRepository(session)
        self._categories = CategoryRepository(session)
        self._cache = cache
        self._correlation_id = correlation_id

    async def _publish_product_upserted(self, product: Product) -> None:
        """Announce a product change so the Order service can keep its local
        price read-model current.

        Enqueued in the caller's transaction, so the product row and its event
        commit together - the whole point of the outbox.
        """
        envelope = make_envelope(
            event_type=Topics.CATALOG_PRODUCT_UPSERTED,
            payload=ProductUpserted(
                product_id=product.id,
                sku=product.sku,
                name=product.name,
                price=product.price,
                currency=product.currency,
                is_active=product.is_active,
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.CATALOG_PRODUCT_UPSERTED,
            key=str(product.id),
            envelope=envelope,
            aggregate_id=product.id,
        )

    # ------------------------------------------------------------------- reads
    async def get_product(self, product_id: UUID) -> ProductResponse:
        cached = await self._cache.get_product(str(product_id))
        if cached is not None:
            return ProductResponse.model_validate(cached)

        product = await self._products.get_by_id(product_id)
        if product is None:
            raise NotFoundError("product not found")

        response = ProductResponse.model_validate(product)
        await self._cache.set_product(str(product_id), response.model_dump(mode="json"))
        return response

    async def list_products(
        self,
        *,
        params: PageParams,
        category: str | None = None,
        query: str | None = None,
        min_price: Decimal | None = None,
        max_price: Decimal | None = None,
        in_stock_only: bool = False,
        sort: SortOption = SortOption.NEWEST,
    ) -> Page[ProductResponse]:
        cache_key = self._cache.listing_key(
            {
                "page": params.page,
                "size": params.size,
                "category": category,
                "q": query,
                "min_price": min_price,
                "max_price": max_price,
                "in_stock": in_stock_only,
                "sort": sort.value,
            }
        )

        cached = await self._cache.get_listing(cache_key)
        if cached is not None:
            return Page[ProductResponse].model_validate(cached)

        products, total = await self._products.search(
            offset=params.offset,
            limit=params.limit,
            category_slug=category,
            query=query,
            min_price=min_price,
            max_price=max_price,
            in_stock_only=in_stock_only,
            sort=sort,
        )
        page = Page.build(
            [ProductResponse.model_validate(p) for p in products], total=total, params=params
        )
        await self._cache.set_listing(cache_key, page.model_dump(mode="json"))
        return page

    async def list_categories(self) -> list[CategoryResponse]:
        categories = await self._categories.list_all()
        return [CategoryResponse.model_validate(c) for c in categories]

    # ------------------------------------------------------------- admin writes
    async def create_product(self, body: ProductCreateRequest) -> ProductResponse:
        if await self._products.sku_exists(body.sku):
            raise ConflictError(f"a product with sku {body.sku} already exists")

        if body.category_id is not None:
            if await self._categories.get_by_id(body.category_id) is None:
                raise NotFoundError("category not found")

        product = await self._products.create(
            sku=body.sku,
            name=body.name,
            description=body.description,
            price=body.price,
            currency=body.currency,
            category_id=body.category_id,
            image_url=body.image_url,
            # Opening stock is displayed here but OWNED by the inventory service.
            # Phase 3 will publish a stock-seeding command so inventory records
            # it; until then this is a display-only starting value.
            cached_stock=body.initial_stock,
        )
        await self._publish_product_upserted(product)
        await self._cache.invalidate_listings()
        logger.info("product created", extra={"product_id": str(product.id), "sku": product.sku})
        return ProductResponse.model_validate(product)

    async def update_product(
        self, product_id: UUID, body: ProductUpdateRequest
    ) -> ProductResponse:
        product = await self._products.get_by_id(product_id, include_inactive=True)
        if product is None:
            raise NotFoundError("product not found")

        updates = body.model_dump(exclude_unset=True)
        if "category_id" in updates and updates["category_id"] is not None:
            if await self._categories.get_by_id(updates["category_id"]) is None:
                raise NotFoundError("category not found")

        product = await self._products.apply_updates(product, updates)
        # A price change must reach Order's read-model, or the next checkout
        # snapshots a stale price.
        await self._publish_product_upserted(product)
        await self._cache.invalidate_product(str(product_id))
        logger.info(
            "product updated",
            extra={"product_id": str(product_id), "fields": sorted(updates)},
        )
        return ProductResponse.model_validate(product)

    async def deactivate_product(self, product_id: UUID) -> None:
        """Soft delete.

        A hard delete would break every historical order that references this
        product - and orders live in a different database, so there is no foreign
        key to stop you.
        """
        product = await self._products.get_by_id(product_id, include_inactive=True)
        if product is None:
            raise NotFoundError("product not found")

        product.is_active = False
        await self._session.flush()
        # Order must stop accepting new orders for this product.
        await self._publish_product_upserted(product)
        await self._cache.invalidate_product(str(product_id))
        logger.info("product deactivated", extra={"product_id": str(product_id)})

    async def create_category(
        self, *, name: str, slug: str, description: str | None
    ) -> CategoryResponse:
        if await self._categories.get_by_slug(slug) is not None:
            raise ConflictError(f"a category with slug '{slug}' already exists")
        category = await self._categories.create(name=name, slug=slug, description=description)
        await self._cache.invalidate_listings()
        return CategoryResponse.model_validate(category)

    # -------------------------------------------------------- event application
    async def apply_stock_change(
        self, *, product_id: UUID, available_qty: int, reserved_qty: int
    ) -> Product | None:
        product = await self._products.update_cached_stock(
            product_id=product_id, available_qty=available_qty, reserved_qty=reserved_qty
        )
        if product is None:
            logger.warning(
                "stock change for unknown product, ignoring",
                extra={"product_id": str(product_id)},
            )
        return product

    async def invalidate_product_cache(self, product_id: UUID) -> None:
        await self._cache.invalidate_product(str(product_id))

    @staticmethod
    def serialize(product: Product) -> dict[str, Any]:
        return ProductResponse.model_validate(product).model_dump(mode="json")
