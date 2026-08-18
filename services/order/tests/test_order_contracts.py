"""Request/response contract guards.

Each of these encodes a decision that is easy to undo by accident later.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.schemas import CreateOrderRequest, OrderItemRequest, PaymentMethodRequest

VALID_ADDRESS = {
    "line1": "1 Test Road",
    "city": "Pune",
    "state": "MH",
    "postal_code": "411001",
    "country": "IN",
}
VALID_PAYMENT = {"type": "CARD", "token": "tok_test_success", "last4": "4242"}


def _body(**overrides) -> dict:
    body = {
        "items": [{"product_id": str(uuid4()), "quantity": 2}],
        "shipping_address": VALID_ADDRESS,
        "payment_method": VALID_PAYMENT,
    }
    body.update(overrides)
    return body


class TestClientCannotSetPrices:
    def test_order_item_has_no_price_field(self) -> None:
        # If a client could send a price, anyone could buy a laptop for 1 rupee.
        # Order reads prices from its own product_snapshots read-model instead.
        fields = OrderItemRequest.model_fields
        assert "unit_price" not in fields
        assert "price" not in fields
        assert set(fields) == {"product_id", "quantity"}

    def test_price_in_body_is_dropped_not_honoured(self) -> None:
        item = OrderItemRequest.model_validate(
            {"product_id": str(uuid4()), "quantity": 1, "unit_price": "0.01"}
        )
        assert not hasattr(item, "unit_price")

    def test_create_request_has_no_total(self) -> None:
        # The total is computed from snapshot prices server-side.
        assert "total_amount" not in CreateOrderRequest.model_fields
        assert "total" not in CreateOrderRequest.model_fields


class TestCardNumbersNeverEnterTheSystem:
    def test_payment_method_takes_a_token(self) -> None:
        # Card numbers must not reach a backend that copies this straight into a
        # Kafka event retained for seven days and visible in Kafka UI.
        fields = PaymentMethodRequest.model_fields
        assert "token" in fields
        assert "card_number" not in fields
        assert "cvv" not in fields
        assert "expiry" not in fields

    def test_last4_must_be_exactly_four_digits(self) -> None:
        with pytest.raises(ValidationError):
            PaymentMethodRequest.model_validate(
                {"token": "tok_x", "last4": "4242424242424242"}
            )

    def test_unknown_payment_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            PaymentMethodRequest.model_validate({"type": "BITCOIN", "token": "tok_x"})


class TestQuantityAndItemLimits:
    def test_zero_quantity_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CreateOrderRequest.model_validate(
                _body(items=[{"product_id": str(uuid4()), "quantity": 0}])
            )

    def test_negative_quantity_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CreateOrderRequest.model_validate(
                _body(items=[{"product_id": str(uuid4()), "quantity": -5}])
            )

    def test_empty_order_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CreateOrderRequest.model_validate(_body(items=[]))

    def test_absurd_quantity_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CreateOrderRequest.model_validate(
                _body(items=[{"product_id": str(uuid4()), "quantity": 10_000}])
            )

    def test_duplicate_product_lines_rejected(self) -> None:
        """Two lines for one product must be combined by the client.

        Inventory aggregates duplicates before checking stock, so accepting them
        would work - but the order would then show the same product twice, which
        confuses both the customer and any later reconciliation.
        """
        product_id = str(uuid4())
        with pytest.raises(ValidationError, match="one line per product"):
            CreateOrderRequest.model_validate(
                _body(
                    items=[
                        {"product_id": product_id, "quantity": 1},
                        {"product_id": product_id, "quantity": 2},
                    ]
                )
            )


class TestValidRequestPasses:
    def test_minimal_valid_order(self) -> None:
        body = CreateOrderRequest.model_validate(_body())
        assert len(body.items) == 1
        assert body.items[0].quantity == 2
        assert body.payment_method.token == "tok_test_success"
        assert body.shipping_address.city == "Pune"

    def test_country_defaults_to_in(self) -> None:
        address = dict(VALID_ADDRESS)
        address.pop("country")
        body = CreateOrderRequest.model_validate(_body(shipping_address=address))
        assert body.shipping_address.country == "IN"
