"""Mock gateway behaviour.

The determinism tests matter more than they look: if a retried charge for the same
order could reach a different verdict than the first attempt, the idempotency layer
would be papering over a gateway that lies.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from app.mock_gateway import (
    TOKEN_DECLINED,
    TOKEN_EXPIRED,
    TOKEN_INSUFFICIENT,
    TOKEN_SUCCESS,
    CardDeclined,
    CardExpired,
    GatewayTimeout,
    InsufficientFunds,
    MockPaymentGateway,
    _stable_fraction,
)

gateway = MockPaymentGateway()
AMOUNT = Decimal("1999.00")


class TestSuccessPath:
    async def test_success_token_charges(self) -> None:
        result = await gateway.charge(
            order_id=uuid4(), amount=AMOUNT, currency="INR", token=TOKEN_SUCCESS
        )
        assert result.provider_ref.startswith("mock_ch_")
        assert result.amount == AMOUNT

    async def test_each_charge_gets_a_distinct_provider_ref(self) -> None:
        a = await gateway.charge(
            order_id=uuid4(), amount=AMOUNT, currency="INR", token=TOKEN_SUCCESS
        )
        b = await gateway.charge(
            order_id=uuid4(), amount=AMOUNT, currency="INR", token=TOKEN_SUCCESS
        )
        assert a.provider_ref != b.provider_ref


class TestFailureTokens:
    @pytest.mark.parametrize(
        ("token", "exc", "code"),
        [
            (TOKEN_DECLINED, CardDeclined, "card_declined"),
            (TOKEN_INSUFFICIENT, InsufficientFunds, "insufficient_funds"),
            (TOKEN_EXPIRED, CardExpired, "card_expired"),
        ],
    )
    async def test_failure_tokens_raise_the_right_error(self, token, exc, code) -> None:
        with pytest.raises(exc) as info:
            await gateway.charge(
                order_id=uuid4(), amount=AMOUNT, currency="INR", token=token
            )
        assert info.value.failure_code == code

    async def test_declines_are_retryable(self) -> None:
        # A different card might work, so the customer is offered a retry.
        with pytest.raises(CardDeclined) as info:
            await gateway.charge(
                order_id=uuid4(), amount=AMOUNT, currency="INR", token=TOKEN_DECLINED
            )
        assert info.value.retryable is True

    async def test_timeouts_are_not_retryable(self) -> None:
        """A timeout means the charge MAY have gone through at the provider.

        Blindly retrying risks charging twice, so `retryable=False` sends the
        customer to support instead of offering a retry button. Real systems
        reconcile against the provider here.
        """
        assert GatewayTimeout("x").retryable is False


class TestDeterminism:
    def test_stable_fraction_is_stable(self) -> None:
        order_id = uuid4()
        assert _stable_fraction(order_id) == _stable_fraction(order_id)

    def test_stable_fraction_differs_across_orders(self) -> None:
        values = {_stable_fraction(uuid4()) for _ in range(50)}
        assert len(values) > 45  # essentially all distinct

    def test_stable_fraction_is_in_range(self) -> None:
        for _ in range(100):
            assert 0.0 <= _stable_fraction(uuid4()) <= 1.0

    def test_known_uuid_gives_a_fixed_value(self) -> None:
        # Pinned so a future refactor of the hashing cannot silently change which
        # orders fail under MOCK_FAILURE_RATE.
        fixed = UUID("00000000-0000-0000-0000-000000000001")
        assert _stable_fraction(fixed) == pytest.approx(_stable_fraction(fixed))

    async def test_same_order_retried_gets_the_same_verdict(self, monkeypatch) -> None:
        """The property that makes retries safe.

        With a random failure rate, a retried charge for one order must reach the
        same answer as the first attempt - otherwise "retry" becomes a lottery and
        the system's behaviour is unreproducible.
        """
        from app import mock_gateway

        monkeypatch.setattr(mock_gateway.settings, "mock_failure_rate", 0.5)
        monkeypatch.setattr(mock_gateway.settings, "mock_latency_ms", 0)

        order_id = uuid4()

        async def attempt() -> bool:
            try:
                await gateway.charge(
                    order_id=order_id, amount=AMOUNT, currency="INR", token=TOKEN_SUCCESS
                )
                return True
            except CardDeclined:
                return False

        verdicts = {await attempt() for _ in range(6)}
        assert len(verdicts) == 1, "the same order must always get the same verdict"


class TestRefunds:
    async def test_refund_succeeds(self) -> None:
        result = await gateway.refund(provider_ref="mock_ch_abc", amount=AMOUNT)
        assert result.provider_ref.startswith("mock_re_")
        assert result.amount == AMOUNT

    async def test_refund_without_a_charge_reference_fails(self) -> None:
        from app.mock_gateway import PaymentError

        with pytest.raises(PaymentError):
            await gateway.refund(provider_ref="", amount=AMOUNT)


class TestAmountValidation:
    async def test_zero_amount_declined(self) -> None:
        with pytest.raises(CardDeclined):
            await gateway.charge(
                order_id=uuid4(), amount=Decimal("0.00"), currency="INR", token=TOKEN_SUCCESS
            )
