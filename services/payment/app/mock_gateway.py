"""Mock payment provider.

Deterministic on purpose. `Math.random()`-style failure makes demos unreproducible
and tests flaky, and - worse - a retried charge for the same order could reach a
different verdict than the first attempt, which is exactly the bug the whole
idempotency layer exists to prevent.

So outcomes are decided by the payment TOKEN, and the optional random-failure
knob hashes the `order_id` rather than calling a random number generator: the same
order always gets the same answer, however many times it is retried.

The frontend maps its test cards to these tokens:

    4242 4242 4242 4242 -> tok_test_success
    4000 0000 0000 0002 -> tok_test_declined
    4000 0000 0000 9995 -> tok_test_insufficient
    4000 0000 0000 0127 -> tok_test_timeout
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid4

from app.config import settings

logger = logging.getLogger(__name__)

TOKEN_SUCCESS = "tok_test_success"
TOKEN_DECLINED = "tok_test_declined"
TOKEN_INSUFFICIENT = "tok_test_insufficient"
TOKEN_TIMEOUT = "tok_test_timeout"
TOKEN_EXPIRED = "tok_test_expired"

TEST_TOKENS = {
    TOKEN_SUCCESS: "always succeeds",
    TOKEN_DECLINED: "always declined by the issuer",
    TOKEN_INSUFFICIENT: "always insufficient funds",
    TOKEN_TIMEOUT: "always times out",
    TOKEN_EXPIRED: "always reports an expired card",
}


class PaymentError(Exception):
    """Base for gateway rejections. `retryable` tells the saga whether a customer
    retry could plausibly succeed."""

    failure_code = "payment_error"
    retryable = True

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class CardDeclined(PaymentError):
    failure_code = "card_declined"
    retryable = True  # a different card might work


class InsufficientFunds(PaymentError):
    failure_code = "insufficient_funds"
    retryable = True


class CardExpired(PaymentError):
    failure_code = "card_expired"
    retryable = True


class GatewayTimeout(PaymentError):
    failure_code = "gateway_timeout"
    # NOT retryable automatically. A timeout means the charge may or may not have
    # gone through at the provider; blindly retrying risks charging twice. Real
    # systems reconcile against the provider instead.
    retryable = False


@dataclass(frozen=True)
class ChargeResult:
    provider_ref: str
    amount: Decimal
    currency: str


@dataclass(frozen=True)
class RefundResult:
    provider_ref: str
    amount: Decimal


def _stable_fraction(order_id: UUID) -> float:
    """Deterministic 0.0-1.0 value from an order id.

    Same order, same number, forever - so a retry agrees with the first attempt
    instead of flip-flopping.
    """
    digest = hashlib.sha256(str(order_id).encode()).digest()
    return int.from_bytes(digest[:4], "big") / 0xFFFFFFFF


class MockPaymentGateway:
    async def charge(
        self, *, order_id: UUID, amount: Decimal, currency: str, token: str
    ) -> ChargeResult:
        await asyncio.sleep(settings.mock_latency_ms / 1000)

        if token == TOKEN_DECLINED:
            raise CardDeclined("the card was declined by the issuing bank")
        if token == TOKEN_INSUFFICIENT:
            raise InsufficientFunds("the card has insufficient funds")
        if token == TOKEN_EXPIRED:
            raise CardExpired("the card has expired")
        if token == TOKEN_TIMEOUT:
            await asyncio.sleep(settings.mock_timeout_delay_ms / 1000)
            raise GatewayTimeout("the payment provider did not respond in time")

        # Optional chaos, still deterministic per order.
        if settings.mock_failure_rate > 0 and _stable_fraction(order_id) < settings.mock_failure_rate:
            logger.info(
                "injected failure per MOCK_FAILURE_RATE",
                extra={"order_id": str(order_id), "rate": settings.mock_failure_rate},
            )
            raise CardDeclined("randomly injected decline (MOCK_FAILURE_RATE)")

        if amount <= 0:
            raise CardDeclined("amount must be positive")

        return ChargeResult(
            provider_ref=f"mock_ch_{uuid4().hex[:16]}", amount=amount, currency=currency
        )

    async def refund(
        self, *, provider_ref: str, amount: Decimal
    ) -> RefundResult:
        await asyncio.sleep(settings.mock_latency_ms / 1000)
        if not provider_ref:
            raise PaymentError("cannot refund a charge with no provider reference")
        return RefundResult(provider_ref=f"mock_re_{uuid4().hex[:16]}", amount=amount)


gateway = MockPaymentGateway()
