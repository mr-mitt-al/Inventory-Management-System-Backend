"""Kafka producer wrapper.

Two settings that matter:

``acks="all"``          wait for all in-sync replicas before considering a send
                       durable. With acks=1 a broker failure right after the
                       leader ack loses the event silently.
``enable_idempotence``  the broker deduplicates producer-side retries, so a
                       retried send does not create a second copy of the event.
                       Consumers still dedupe, because the outbox can also
                       resend after a crash - two independent layers.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from common.events.envelope import EventEnvelope

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        producer_name: str,
        linger_ms: int = 10,
        request_timeout_ms: int = 20_000,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._name = producer_name
        self._linger_ms = linger_ms
        self._request_timeout_ms = request_timeout_ms
        self._producer: AIOKafkaProducer | None = None

    async def start(self, *, max_attempts: int = 30, backoff_s: float = 2.0) -> None:
        """Connect, retrying while the broker boots.

        Compose healthchecks help but do not eliminate the race - a broker can
        answer the API-versions probe and still be electing partition leaders.
        Retry rather than crash-loop.
        """
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            client_id=self._name,
            acks="all",
            enable_idempotence=True,
            linger_ms=self._linger_ms,
            request_timeout_ms=self._request_timeout_ms,
            value_serializer=lambda v: v,  # already bytes
            key_serializer=lambda k: k if k is None else k.encode("utf-8"),
        )
        for attempt in range(1, max_attempts + 1):
            try:
                await self._producer.start()
                logger.info("kafka producer connected", extra={"producer": self._name})
                return
            except KafkaConnectionError:
                if attempt == max_attempts:
                    raise
                logger.warning(
                    "kafka not reachable, retrying",
                    extra={"attempt": attempt, "of": max_attempts},
                )
                await asyncio.sleep(backoff_s)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            logger.info("kafka producer stopped", extra={"producer": self._name})

    async def send(self, *, topic: str, key: str | None, envelope: EventEnvelope) -> None:
        """Publish and wait for the broker ack.

        Awaiting the ack is deliberate: fire-and-forget would let the outbox mark
        a row published before the broker actually has it.
        """
        if self._producer is None:
            raise RuntimeError("producer not started")

        await self._producer.send_and_wait(
            topic,
            value=envelope.to_bytes(),
            key=key,
            headers=[
                ("event_id", str(envelope.event_id).encode()),
                ("event_type", envelope.event_type.encode()),
                ("correlation_id", str(envelope.correlation_id).encode()),
                ("producer", envelope.producer.encode()),
            ],
        )
        logger.debug(
            "event published",
            extra={
                "topic": topic,
                "key": key,
                "event_id": str(envelope.event_id),
                "event_type": envelope.event_type,
            },
        )
