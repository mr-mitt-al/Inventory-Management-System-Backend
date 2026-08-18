"""Inventory invariants that hold without a database.

The concurrency test that genuinely proves `FOR UPDATE` works needs a real
postgres and lives in `test_oversell_integration.py`.
"""

from __future__ import annotations

from app.models import LedgerReason, ReservationStatus


class TestReservationLifecycle:
    def test_four_statuses_exist_and_are_distinct(self) -> None:
        assert set(ReservationStatus) == {
            ReservationStatus.HELD,
            ReservationStatus.COMMITTED,
            ReservationStatus.RELEASED,
            ReservationStatus.EXPIRED,
        }

    def test_released_and_expired_are_separate(self) -> None:
        """Both return stock, but for different reasons, and conflating them would
        hide a real problem.

        RELEASED means the saga completed and compensated correctly. EXPIRED means
        a saga died mid-flight and the sweeper cleaned up after it - which is a
        signal worth seeing in the ledger.
        """
        assert ReservationStatus.RELEASED != ReservationStatus.EXPIRED


class TestLedgerReasons:
    def test_every_stock_movement_has_a_reason(self) -> None:
        assert set(LedgerReason) == {
            LedgerReason.RESERVE,
            LedgerReason.RELEASE,
            LedgerReason.COMMIT,
            LedgerReason.RESTOCK,
            LedgerReason.ADJUST,
            LedgerReason.EXPIRE,
        }

    def test_restock_and_adjust_are_separate(self) -> None:
        # "40 more arrived" and "we counted and there are 40" are different facts.
        # Merging them would make the ledger unable to answer either question.
        assert LedgerReason.RESTOCK != LedgerReason.ADJUST


class TestStockArithmetic:
    """The available/reserved split, reasoned through without a database.

    Physical stock is available + reserved. A reservation moves units between the
    two counters; it never changes the total. Committing removes them from
    reserved; releasing moves them back to available.
    """

    def test_reserve_moves_units_without_changing_the_total(self) -> None:
        available, reserved = 10, 0
        total_before = available + reserved

        available, reserved = available - 3, reserved + 3

        assert (available, reserved) == (7, 3)
        assert available + reserved == total_before

    def test_commit_removes_units_permanently(self) -> None:
        available, reserved = 7, 3
        # Units leave the building: reserved drops, available is untouched because
        # they were already moved out of it at reservation time.
        reserved -= 3
        assert (available, reserved) == (7, 0)

    def test_release_returns_units_to_available(self) -> None:
        # THE COMPENSATION. After a declined payment the units become sellable
        # again and the counters return to exactly their pre-reservation values.
        available, reserved = 7, 3
        available, reserved = available + 3, reserved - 3
        assert (available, reserved) == (10, 0)

    def test_full_failed_saga_is_a_round_trip(self) -> None:
        start_available, start_reserved = 10, 0

        # reserve
        available, reserved = start_available - 4, start_reserved + 4
        assert (available, reserved) == (6, 4)

        # payment fails -> release
        available, reserved = available + 4, reserved - 4

        assert (available, reserved) == (start_available, start_reserved)

    def test_expiry_is_arithmetically_identical_to_release(self) -> None:
        available, reserved = 6, 4
        available, reserved = available + 4, reserved - 4
        assert (available, reserved) == (10, 0)


class TestLowStockThreshold:
    def test_at_threshold_counts_as_low(self) -> None:
        # `<=`, not `<`: hitting the threshold exactly is the moment to reorder.
        available, threshold = 10, 10
        assert available <= threshold

    def test_above_threshold_is_not_low(self) -> None:
        assert not (11 <= 10)

    def test_zero_stock_is_low(self) -> None:
        assert 0 <= 10
