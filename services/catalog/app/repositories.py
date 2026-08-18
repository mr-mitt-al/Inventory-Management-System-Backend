from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Category, Product
from app.schemas import SortOption

_SORT_CLAUSES = {
    SortOption.NEWEST: Product.created_at.desc(),
    SortOption.PRICE_ASC: Product.price.asc(),
    SortOption.PRICE_DESC: Product.price.desc(),
    SortOption.NAME_ASC: Product.name.asc(),
}


class CategoryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[Category]:
        result = await self._session.execute(select(Category).order_by(Category.name))
        return list(result.scalars().all())

    async def get_by_id(self, category_id: UUID) -> Category | None:
        return await self._session.get(Category, category_id)

    async def get_by_slug(self, slug: str) -> Category | None:
        result = await self._session.execute(select(Category).where(Category.slug == slug))
        return result.scalar_one_or_none()

    async def create(self, *, name: str, slug: str, description: str | None) -> Category:
        category = Category(name=name, slug=slug, description=description)
        self._session.add(category)
        await self._session.flush()
        await self._session.refresh(category)
        return category


class ProductRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, product_id: UUID, *, include_inactive: bool = False) -> Product | None:
        product = await self._session.get(Product, product_id)
        if product is None:
            return None
        if not include_inactive and not product.is_active:
            return None
        return product

    async def get_by_sku(self, sku: str) -> Product | None:
        result = await self._session.execute(select(Product).where(Product.sku == sku.upper()))
        return result.scalar_one_or_none()

    async def sku_exists(self, sku: str) -> bool:
        result = await self._session.execute(select(Product.id).where(Product.sku == sku.upper()))
        return result.scalar_one_or_none() is not None

    async def search(
        self,
        *,
        offset: int,
        limit: int,
        category_slug: str | None = None,
        query: str | None = None,
        min_price: Decimal | None = None,
        max_price: Decimal | None = None,
        in_stock_only: bool = False,
        sort: SortOption = SortOption.NEWEST,
        include_inactive: bool = False,
    ) -> tuple[list[Product], int]:
        conditions = []
        if not include_inactive:
            conditions.append(Product.is_active.is_(True))
        if query:
            pattern = f"%{query.lower()}%"
            conditions.append(
                func.lower(Product.name).like(pattern)
                | func.lower(Product.sku).like(pattern)
                | func.lower(func.coalesce(Product.description, "")).like(pattern)
            )
        if min_price is not None:
            conditions.append(Product.price >= min_price)
        if max_price is not None:
            conditions.append(Product.price <= max_price)
        if in_stock_only:
            conditions.append(Product.cached_stock > 0)

        stmt = select(Product)
        count_stmt = select(func.count()).select_from(Product)

        if category_slug:
            stmt = stmt.join(Category, Product.category_id == Category.id)
            count_stmt = count_stmt.join(Category, Product.category_id == Category.id)
            conditions.append(Category.slug == category_slug)

        for condition in conditions:
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        total = int((await self._session.execute(count_stmt)).scalar_one())

        # Secondary sort on id: without a tiebreaker, rows with equal price can
        # come back in a different order per page and an item appears twice (or
        # never) while the user pages through.
        stmt = stmt.order_by(_SORT_CLAUSES[sort], Product.id).offset(offset).limit(limit)
        rows = (await self._session.execute(stmt)).scalars().all()
        return list(rows), total

    async def create(self, **fields) -> Product:
        product = Product(**fields)
        self._session.add(product)
        await self._session.flush()
        await self._session.refresh(product)
        return product

    async def apply_updates(self, product: Product, updates: dict) -> Product:
        for key, value in updates.items():
            setattr(product, key, value)
        await self._session.flush()
        await self._session.refresh(product)
        return product

    async def update_cached_stock(
        self, *, product_id: UUID, available_qty: int, reserved_qty: int
    ) -> Product | None:
        """Applies an `inventory.stock.changed` event to the denormalized copy.

        Returns None if the product is unknown here - a legitimate case during a
        replay of old events, not an error worth failing the handler over.
        """
        product = await self._session.get(Product, product_id)
        if product is None:
            return None
        product.cached_stock = available_qty
        product.cached_reserved = reserved_qty
        await self._session.flush()
        return product
