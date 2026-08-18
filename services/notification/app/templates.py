"""Message templates.

Plain functions returning (subject, body). No template engine: five messages do
not justify Jinja, and a function is easier to test than a file.
"""

from __future__ import annotations

from decimal import Decimal

CURRENCY_SYMBOLS = {"INR": "₹", "USD": "$", "EUR": "€", "GBP": "£"}


def money(amount: Decimal | float | str, currency: str = "INR") -> str:
    symbol = CURRENCY_SYMBOLS.get(currency.upper(), f"{currency.upper()} ")
    return f"{symbol}{Decimal(str(amount)):,.2f}"


def welcome(*, full_name: str) -> tuple[str, str]:
    return (
        "Welcome aboard",
        f"Hi {full_name},\n\n"
        "Your account is ready. Happy shopping.\n",
    )


def order_confirmed(
    *, order_id, total_amount, currency: str, items: list[dict]
) -> tuple[str, str]:
    lines = "\n".join(
        f"  - {item['name']} x{item['quantity']}  "
        f"{money(item['unit_price'], currency)}"
        for item in items
    )
    return (
        f"Order confirmed - {str(order_id)[:8]}",
        f"Your order is confirmed.\n\n"
        f"Order: {order_id}\n"
        f"{lines}\n\n"
        f"Total: {money(total_amount, currency)}\n\n"
        "We will let you know when it ships.\n",
    )


def payment_failed(
    *, order_id, amount, currency: str, failure_code: str, failure_message: str, retryable: bool
) -> tuple[str, str]:
    """The most important message in the system.

    It explains the compensation to the customer: the money was not taken AND the
    items are no longer being held for them. Saying only "payment failed" would
    leave them wondering whether their items are still reserved.
    """
    next_step = (
        "You can retry with a different payment method from your order page.\n"
        if retryable
        else "Please contact support before trying again.\n"
    )
    return (
        f"Payment declined - order {str(order_id)[:8]}",
        f"We could not take payment of {money(amount, currency)} for order {order_id}.\n\n"
        f"Reason: {failure_message} ({failure_code})\n\n"
        "You have not been charged, and the items reserved for this order have been "
        "returned to stock.\n\n" + next_step,
    )


def order_out_of_stock(*, order_id, reason: str) -> tuple[str, str]:
    return (
        f"Order could not be fulfilled - {str(order_id)[:8]}",
        f"We could not fulfil order {order_id}.\n\n"
        f"Reason: {reason}\n\n"
        "You have not been charged.\n",
    )


def order_cancelled(*, order_id, reason: str, was_paid: bool) -> tuple[str, str]:
    refund_line = (
        "A refund has been issued and should reach your account within a few "
        "business days.\n"
        if was_paid
        else "No payment was taken.\n"
    )
    return (
        f"Order cancelled - {str(order_id)[:8]}",
        f"Order {order_id} has been cancelled.\n\nReason: {reason}\n\n" + refund_line,
    )


def payment_refunded(*, order_id, amount, currency: str, reason: str) -> tuple[str, str]:
    return (
        f"Refund issued - order {str(order_id)[:8]}",
        f"We have refunded {money(amount, currency)} for order {order_id}.\n\n"
        f"Reason: {reason}\n\n"
        "It should appear in your account within a few business days.\n",
    )


def low_stock_alert(*, sku: str, available_qty: int, threshold: int) -> tuple[str, str]:
    return (
        f"Low stock: {sku} ({available_qty} left)",
        f"Stock for {sku} has fallen to {available_qty}, at or below the "
        f"threshold of {threshold}.\n\nRestock from the admin inventory screen.\n",
    )
