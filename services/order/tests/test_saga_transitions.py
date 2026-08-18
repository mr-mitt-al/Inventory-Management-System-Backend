"""The order state machine, asserted rather than assumed.

No database or broker needed - this is pure logic, which is exactly why it is worth
testing exhaustively. Every rule here is one a wrong event ordering could otherwise
violate silently.
"""

from __future__ import annotations

import pytest

from common.order_status import (
    ALLOWED_TRANSITIONS,
    CUSTOMER_CANCELLABLE,
    PAID_STATUSES,
    TERMINAL_STATUSES,
    OrderStatus,
    can_transition,
    is_terminal,
)


class TestHappyPath:
    def test_full_forward_path_is_legal(self) -> None:
        path = [
            OrderStatus.PENDING,
            OrderStatus.INVENTORY_RESERVED,
            OrderStatus.PAID,
            OrderStatus.CONFIRMED,
            OrderStatus.SHIPPED,
            OrderStatus.DELIVERED,
        ]
        # Pairwise over consecutive states. No strict=True: the offset slice is
        # one shorter by construction, which is the whole point of pairing.
        for current, target in zip(path, path[1:]):
            assert can_transition(current, target), f"{current} -> {target} should be legal"


class TestForbiddenShortcuts:
    def test_cannot_skip_reservation(self) -> None:
        # Paying before stock is held is the bug the whole reserve-then-pay
        # ordering exists to prevent.
        assert not can_transition(OrderStatus.PENDING, OrderStatus.PAID)

    def test_cannot_confirm_without_paying(self) -> None:
        assert not can_transition(OrderStatus.INVENTORY_RESERVED, OrderStatus.CONFIRMED)

    def test_cannot_ship_before_confirming(self) -> None:
        assert not can_transition(OrderStatus.PAID, OrderStatus.SHIPPED)

    def test_cannot_deliver_without_shipping(self) -> None:
        assert not can_transition(OrderStatus.CONFIRMED, OrderStatus.DELIVERED)

    def test_cannot_go_backwards(self) -> None:
        assert not can_transition(OrderStatus.PAID, OrderStatus.INVENTORY_RESERVED)
        assert not can_transition(OrderStatus.CONFIRMED, OrderStatus.PAID)
        assert not can_transition(OrderStatus.SHIPPED, OrderStatus.CONFIRMED)


class TestTerminalStates:
    @pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
    def test_nothing_leaves_a_terminal_state(self, status: OrderStatus) -> None:
        # A late-arriving duplicate event must never resurrect a finished order.
        assert ALLOWED_TRANSITIONS[status] == frozenset()
        assert is_terminal(status)

    def test_terminal_set_is_exactly_these_three(self) -> None:
        assert TERMINAL_STATUSES == {
            OrderStatus.DELIVERED,
            OrderStatus.CANCELLED,
            OrderStatus.FAILED,
        }

    def test_in_flight_states_are_not_terminal(self) -> None:
        for status in (
            OrderStatus.PENDING,
            OrderStatus.INVENTORY_RESERVED,
            OrderStatus.PAID,
            OrderStatus.CONFIRMED,
            OrderStatus.SHIPPED,
        ):
            assert not is_terminal(status)


class TestFailurePaths:
    def test_can_fail_from_pending(self) -> None:
        # Out of stock: nothing was done, so nothing needs compensating.
        assert can_transition(OrderStatus.PENDING, OrderStatus.FAILED)

    def test_can_fail_from_reserved(self) -> None:
        # Payment declined after stock was held - the compensation case.
        assert can_transition(OrderStatus.INVENTORY_RESERVED, OrderStatus.FAILED)

    def test_cannot_fail_after_payment_captured(self) -> None:
        # Money has been taken. The exit is CANCELLED with a refund, not FAILED,
        # because FAILED implies nothing happened.
        assert not can_transition(OrderStatus.PAID, OrderStatus.FAILED)
        assert not can_transition(OrderStatus.CONFIRMED, OrderStatus.FAILED)


class TestCancellation:
    @pytest.mark.parametrize("status", sorted(CUSTOMER_CANCELLABLE))
    def test_cancellable_states_can_reach_cancelled(self, status: OrderStatus) -> None:
        assert can_transition(status, OrderStatus.CANCELLED)

    def test_shipped_orders_cannot_be_cancelled(self) -> None:
        # Once it is with the courier it is a returns problem, not an order problem.
        assert OrderStatus.SHIPPED not in CUSTOMER_CANCELLABLE
        assert not can_transition(OrderStatus.SHIPPED, OrderStatus.CANCELLED)

    def test_delivered_orders_cannot_be_cancelled(self) -> None:
        assert not can_transition(OrderStatus.DELIVERED, OrderStatus.CANCELLED)


class TestPaidStatuses:
    def test_paid_set_drives_refund_decisions(self) -> None:
        # `was_paid` in order.cancelled is computed from this set, and it is what
        # tells Payment whether to refund and Inventory whether to restock rather
        # than release.
        assert PAID_STATUSES == {
            OrderStatus.PAID,
            OrderStatus.CONFIRMED,
            OrderStatus.SHIPPED,
            OrderStatus.DELIVERED,
        }

    def test_pre_payment_states_are_not_paid(self) -> None:
        assert OrderStatus.PENDING not in PAID_STATUSES
        assert OrderStatus.INVENTORY_RESERVED not in PAID_STATUSES
        assert OrderStatus.FAILED not in PAID_STATUSES
        assert OrderStatus.CANCELLED not in PAID_STATUSES


class TestMachineIntegrity:
    def test_every_status_has_a_transition_rule(self) -> None:
        # A status missing from the table would silently reject every transition.
        for status in OrderStatus:
            assert status in ALLOWED_TRANSITIONS, f"{status} has no rule"

    def test_no_transition_targets_an_unknown_status(self) -> None:
        for source, targets in ALLOWED_TRANSITIONS.items():
            for target in targets:
                assert isinstance(target, OrderStatus), f"{source} -> {target}"

    def test_no_self_transitions(self) -> None:
        # Handled as a no-op in OrderSaga.transition, never as a state change - a
        # self-transition would write a misleading history row.
        for status, targets in ALLOWED_TRANSITIONS.items():
            assert status not in targets

    def test_every_non_terminal_state_can_reach_a_terminal_one(self) -> None:
        """No order can get permanently stuck.

        Walks the graph from each state; if some state could not reach DELIVERED,
        CANCELLED or FAILED, an order there would hang forever with no way out.
        """
        for start in OrderStatus:
            if is_terminal(start):
                continue
            seen, frontier = {start}, [start]
            while frontier:
                current = frontier.pop()
                if is_terminal(current):
                    break
                for nxt in ALLOWED_TRANSITIONS[current]:
                    if nxt not in seen:
                        seen.add(nxt)
                        frontier.append(nxt)
            else:
                pytest.fail(f"{start} cannot reach any terminal state")
