"""The oversell test - the one that actually proves `SELECT ... FOR UPDATE` works.

Needs a REAL postgres. Row-level locking cannot be tested against sqlite or a
mock: sqlite serialises everything anyway, so the test would pass whether or not
the lock existed, which is worse than having no test at all.

Run it with a database available:

    docker compose up -d postgres
    export TEST_DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/inventory_db
    pytest -m integration

Skipped automatically when TEST_DATABASE_URL is unset.
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from uuid import uuid4

import pytest

from common.db.base import Base
from common.db.session import Database
from common.events.schemas import PaymentMethod
from app.models import ReservationStatus, StockItem
from app.services import InventoryService

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not TEST_DATABASE_URL,
        reason="set TEST_DATABASE_URL to a real postgres to run oversell tests",
    ),
]

PAYMENT = PaymentMethod(type="CARD", token="tok_test_success", last4="4242")


@pytest.fixture
async def db():
    database = Database(TEST_DATABASE_URL, pool_size=20, max_overflow=10)
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield database
    await database.dispose()


@pytest.fixture
async def product(db):
    """A product with exactly 5 units available."""
    product_id = uuid4()
    async with db.transaction() as session:
        session.add(
            StockItem(
                product_id=product_id,
                sku=f"TEST-{product_id.hex[:8]}".upper(),
                available_qty=5,
                reserved_qty=0,
                low_stock_threshold=1,
            )
        )
    return product_id


async def _try_reserve(db, product_id, quantity: int) -> bool:
    """One independent reservation attempt in its own transaction."""
    try:
        async with db.transaction() as session:
            service = InventoryService(session)
            reservation = await service.reserve(
                order_id=uuid4(),
                user_id=uuid4(),
                items=[(product_id, quantity)],
                total_amount=Decimal("100.00"),
                currency="INR",
                payment_method=PAYMENT,
            )
            return reservation is not None
    except Exception:
        return False


class TestNoOversellUnderConcurrency:
    async def test_ten_concurrent_orders_for_five_units(self, db, product) -> None:
        """THE test. 10 simultaneous single-unit orders against 5 units of stock.

        Exactly 5 must succeed. Without the row lock, several transactions read
        available_qty=5 at the same instant, each decides "yes", and stock goes
        negative - or the CHECK constraint fires and the customer sees a 500.
        """
        results = await asyncio.gather(
            *[_try_reserve(db, product, 1) for _ in range(10)]
        )

        assert sum(results) == 5, f"expected exactly 5 winners, got {sum(results)}"

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert stock.available_qty == 0
            assert stock.reserved_qty == 5
            assert stock.available_qty + stock.reserved_qty == 5  # nothing invented

    async def test_concurrent_multi_unit_orders_do_not_oversell(self, db, product) -> None:
        # Three orders of 2 units against 5 available: two win, one loses.
        results = await asyncio.gather(*[_try_reserve(db, product, 2) for _ in range(3)])
        assert sum(results) == 2

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert stock.available_qty == 1
            assert stock.reserved_qty == 4

    async def test_stock_never_goes_negative(self, db, product) -> None:
        await asyncio.gather(*[_try_reserve(db, product, 3) for _ in range(8)])
        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert stock.available_qty >= 0
            assert stock.reserved_qty >= 0


class TestIdempotentReservation:
    async def test_same_order_reserved_twice_only_holds_stock_once(self, db, product) -> None:
        """A duplicate `order.created` must not reserve twice.

        `reservations.order_id UNIQUE` is the guarantee; this proves it holds even
        when consumer-side dedup is bypassed entirely, as it is here.
        """
        order_id = uuid4()
        user_id = uuid4()

        async def reserve_once():
            async with db.transaction() as session:
                return await InventoryService(session).reserve(
                    order_id=order_id,
                    user_id=user_id,
                    items=[(product, 2)],
                    total_amount=Decimal("100.00"),
                    currency="INR",
                    payment_method=PAYMENT,
                )

        first = await reserve_once()
        assert first is not None

        second = await reserve_once()
        assert second is None, "the duplicate must be detected, not reserved again"

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert stock.reserved_qty == 2, "stock was reserved twice"
            assert stock.available_qty == 3


class TestCompensation:
    async def test_release_returns_stock_exactly(self, db, product) -> None:
        """The compensating transaction, end to end.

        Reserve, then release as `payment.failed` would - stock must return to
        exactly its starting values, with nothing lost or invented.
        """
        order_id = uuid4()

        async with db.transaction() as session:
            await InventoryService(session).reserve(
                order_id=order_id,
                user_id=uuid4(),
                items=[(product, 3)],
                total_amount=Decimal("100.00"),
                currency="INR",
                payment_method=PAYMENT,
            )

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert (stock.available_qty, stock.reserved_qty) == (2, 3)

        async with db.transaction() as session:
            reservation = await InventoryService(session).release_reservation(
                order_id=order_id, reason="payment failed: card_declined"
            )
            assert reservation.status == ReservationStatus.RELEASED.value

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert (stock.available_qty, stock.reserved_qty) == (5, 0)

    async def test_commit_deducts_permanently(self, db, product) -> None:
        order_id = uuid4()
        async with db.transaction() as session:
            await InventoryService(session).reserve(
                order_id=order_id,
                user_id=uuid4(),
                items=[(product, 3)],
                total_amount=Decimal("100.00"),
                currency="INR",
                payment_method=PAYMENT,
            )

        async with db.transaction() as session:
            await InventoryService(session).commit_reservation(order_id=order_id)

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            # Units are gone: available stays at 2, reserved drops to 0.
            assert (stock.available_qty, stock.reserved_qty) == (2, 0)

    async def test_release_is_idempotent(self, db, product) -> None:
        # payment.failed and order.cancelled can both arrive for one order; the
        # second release must not return the stock twice.
        order_id = uuid4()
        async with db.transaction() as session:
            await InventoryService(session).reserve(
                order_id=order_id,
                user_id=uuid4(),
                items=[(product, 2)],
                total_amount=Decimal("100.00"),
                currency="INR",
                payment_method=PAYMENT,
            )

        for _ in range(3):
            async with db.transaction() as session:
                await InventoryService(session).release_reservation(
                    order_id=order_id, reason="repeated"
                )

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert stock.available_qty == 5, "stock was returned more than once"


class TestReservationExpiry:
    async def test_sweeper_reclaims_expired_reservations(self, db, product) -> None:
        """A saga that died mid-flight must not strand stock forever."""
        from datetime import timedelta

        from common.db.base import utcnow
        from app.models import Reservation

        order_id = uuid4()
        async with db.transaction() as session:
            await InventoryService(session).reserve(
                order_id=order_id,
                user_id=uuid4(),
                items=[(product, 4)],
                total_amount=Decimal("100.00"),
                currency="INR",
                payment_method=PAYMENT,
            )

        # Backdate the TTL rather than waiting 15 minutes.
        async with db.transaction() as session:
            from sqlalchemy import select

            reservation = (
                await session.execute(
                    select(Reservation).where(Reservation.order_id == order_id)
                )
            ).scalar_one()
            reservation.expires_at = utcnow() - timedelta(minutes=1)

        async with db.transaction() as session:
            expired = await InventoryService(session).expire_stale_reservations()
            assert expired == 1

        async with db.session_factory() as session:
            stock = await session.get(StockItem, product)
            assert (stock.available_qty, stock.reserved_qty) == (5, 0)
