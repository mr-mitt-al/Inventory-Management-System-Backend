"""Delivery. A logged stub, isolated behind one function.

Swapping in real SMTP means changing `deliver` and nothing else. It is not
implemented because it would add credentials handling and retry semantics without
teaching anything about the distributed system, which is what this project is for.
"""

from __future__ import annotations

import logging

from app.config import settings
from app.models import Channel

logger = logging.getLogger(__name__)


class DeliveryError(Exception):
    pass


async def deliver(*, channel: Channel, recipient: str, subject: str | None, body: str) -> None:
    if not recipient or "@" not in recipient:
        # Raised rather than silently skipped: a missing address means someone
        # was not told something they should have been told.
        raise DeliveryError(f"cannot deliver to {recipient!r}")

    if settings.delivery_mode == "log":
        logger.info(
            "NOTIFICATION SENT",
            extra={
                "channel": channel.value,
                "to": recipient,
                "subject": subject,
                # Truncated so a long order confirmation does not dominate the log
                # stream; the full text is in the notifications table.
                "preview": body.strip().splitlines()[0][:120] if body.strip() else "",
            },
        )
        return

    raise DeliveryError(f"delivery mode {settings.delivery_mode!r} is not implemented")
