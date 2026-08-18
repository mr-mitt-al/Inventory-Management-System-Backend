"""Base consumer. Every consumer in the system inherits this behaviour.

The reliability rules live here, once, instead of being re-implemented (and
half-forgotten) in six services:

1. ``enable_auto_commit=False``. Offsets are committed only after a message has
   been successfully handled. Auto-commit acknowledges messages the handler
   never processed - silent data loss, and the worst kind of bug because nothing
   errors.

2. Deduplication against ``processed_events`` before dispatch, with the marker
   row written in the SAME transaction as the handler's business writes.

3. Retry with exponential backoff, then dead-letter. After the final failed
   attempt the message is published to ``<topic>.DLQ`` and the offset IS
   committed. That last part looks wrong and is essential: without it a single
   poison message blocks its partition forever and every order queued behind it
   stalls. Park the bad message, keep the line moving.

4. Unparseable messages go straight to the DLQ. Retrying a message that cannot
   be deserialized just burns time - it will never parse.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.errors import KafkaConnectionError
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.idempotency import ProcessedEventMixin, already_processed, mark_processed
from common.db.session import Database
from common.events.envelope import EventEnvelope, make_envelope
from common.events.topics import dlq_topic
from common.kafka.producer import EventProducer
from common.observability.logging import correlation_id_var

logger = logging.getLogger(__name__)

EventHandler = Callable[[AsyncSession, EventEnvelope], Awaitable[None]]
HandlerRegistry = dict[str, EventHandler]


class DeadLetterPayload(BaseModel):
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: str | None
    error_type: str
    error_message: str
    stack_trace: str
    attempts: int
    original_event: dict[str, Any] | None = None
    raw_message: str | None = None


class BaseConsumer:
    """Subclass and provide ``handlers``, or pass them to the constructor.

    ``handlers`` maps ``event_type`` -> coroutine. Routing on ``event_type``
    rather than topic means one consumer can subscribe to several topics and
    stay readable.
    """

    handlers: HandlerRegistry = {}

    def __init__(
        self,
        *,
        name: str,
        topics: list[str],
        group_id: str,
        bootstrap_servers: str,
        db: Database,
        producer: EventProducer,
        processed_event_model: type[ProcessedEventMixin],
        handlers: HandlerRegistry | None = None,
        max_retries: int = 3,
        retry_backoff_ms: int = 1000,
    ) -> None:
        self.name = name
        self.topics = topics
        self.group_id = group_id
        self._bootstrap = bootstrap_servers
        self._db = db
        self._producer = producer
        self._processed_model = processed_event_model
        self._max_retries = max_retries
        self._backoff_s = retry_backoff_ms / 1000
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = asyncio.Event()
        if handlers is not None:
            self.handlers = handlers

    # ------------------------------------------------------------------ lifecycle
    async def start(self, *, max_attempts: int = 30, backoff_s: float = 2.0) -> None:
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self._bootstrap,
            group_id=self.group_id,
            client_id=self.name,
            enable_auto_commit=False,  # rule 1 - non-negotiable
            auto_offset_reset="earliest",
            max_poll_interval_ms=300_000,
        )
        for attempt in range(1, max_attempts + 1):
            try:
                await self._consumer.start()
                logger.info(
                    "consumer started",
                    extra={
                        "consumer": self.name,
                        "group_id": self.group_id,
                        "topics": self.topics,
                    },
                )
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
        self._stopping.set()
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
            logger.info("consumer stopped", extra={"consumer": self.name})

    async def run(self) -> None:
        if self._consumer is None:
            raise RuntimeError("consumer not started")

        while not self._stopping.is_set():
            try:
                record = await self._consumer.getone()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stopping.is_set():
                    break
                logger.exception("consumer poll failed", extra={"consumer": self.name})
                await asyncio.sleep(1)
                continue

            try:
                await self._handle_record(record)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Only reachable if dead-lettering itself failed (broker down).
                # Rewind so the message is redelivered instead of skipped -
                # leaving the offset uncommitted is not enough on its own,
                # because getone() has already advanced this consumer's
                # position past it.
                logger.exception(
                    "record handling aborted, rewinding for redelivery",
                    extra={
                        "consumer": self.name,
                        "topic": record.topic,
                        "offset": record.offset,
                    },
                )
                self._consumer.seek(
                    TopicPartition(record.topic, record.partition), record.offset
                )
                await asyncio.sleep(self._backoff_s)
                continue

            # Offset commits only after _handle_record resolves - either the
            # handler succeeded, the message was a known duplicate, or it was
            # dead-lettered.
            await self._consumer.commit()

    # ------------------------------------------------------------------ internals
    async def _handle_record(self, record: ConsumerRecord) -> None:
        try:
            envelope = EventEnvelope.from_bytes(record.value)
        except Exception as exc:
            logger.error(
                "undeserializable message, dead-lettering immediately",
                extra={"topic": record.topic, "offset": record.offset},
                exc_info=True,
            )
            await self._dead_letter(record, exc, attempts=0, envelope=None)
            return

        token = correlation_id_var.set(str(envelope.correlation_id))
        try:
            handler = self.handlers.get(envelope.event_type)
            if handler is None:
                # Subscribed to a topic with no handler: a config mistake worth
                # seeing, but not worth dead-lettering a valid event over.
                logger.warning(
                    "no handler registered for event type",
                    extra={"consumer": self.name, "event_type": envelope.event_type},
                )
                return

            for attempt in range(1, self._max_retries + 1):
                try:
                    handled = await self._dispatch(handler, envelope)
                    if handled:
                        logger.info(
                            "event handled",
                            extra={
                                "consumer": self.name,
                                "event_type": envelope.event_type,
                                "event_id": str(envelope.event_id),
                                "attempt": attempt,
                            },
                        )
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if attempt >= self._max_retries:
                        logger.error(
                            "handler failed permanently, dead-lettering",
                            extra={
                                "consumer": self.name,
                                "event_type": envelope.event_type,
                                "event_id": str(envelope.event_id),
                                "attempts": attempt,
                            },
                            exc_info=True,
                        )
                        await self._dead_letter(record, exc, attempt, envelope)
                        return

                    delay = self._backoff_s * (2 ** (attempt - 1))
                    logger.warning(
                        "handler failed, retrying",
                        extra={
                            "consumer": self.name,
                            "event_type": envelope.event_type,
                            "event_id": str(envelope.event_id),
                            "attempt": attempt,
                            "retry_in_s": delay,
                        },
                        exc_info=True,
                    )
                    await asyncio.sleep(delay)
        finally:
            correlation_id_var.reset(token)

    async def _dispatch(self, handler: EventHandler, envelope: EventEnvelope) -> bool:
        """Run one handler in one transaction.

        Returns False if the event was a duplicate and skipped.

        The dedup check, the handler's writes and the ``processed_events`` insert
        all share this transaction. A fresh session per attempt matters - a
        rolled-back session is unusable, so retrying inside the old one fails
        for the wrong reason.
        """
        async with self._db.transaction() as session:
            if await already_processed(session, self._processed_model, envelope.event_id):
                logger.info(
                    "duplicate event skipped",
                    extra={
                        "consumer": self.name,
                        "event_id": str(envelope.event_id),
                        "event_type": envelope.event_type,
                    },
                )
                return False

            await handler(session, envelope)
            await mark_processed(
                session,
                self._processed_model,
                event_id=envelope.event_id,
                event_type=envelope.event_type,
                consumer=self.name,
            )
        return True

    async def _dead_letter(
        self,
        record: ConsumerRecord,
        exc: Exception,
        attempts: int,
        envelope: EventEnvelope | None,
    ) -> None:
        payload = DeadLetterPayload(
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            original_key=record.key.decode() if record.key else None,
            error_type=type(exc).__name__,
            error_message=str(exc)[:2000],
            stack_trace="".join(traceback.format_exception(exc))[:8000],
            attempts=attempts,
            original_event=envelope.model_dump(mode="json") if envelope else None,
            raw_message=None if envelope else _safe_decode(record.value),
        )

        dlq_envelope = make_envelope(
            event_type=f"{record.topic}.dead_letter",
            payload=payload,
            producer=self.name,
            correlation_id=envelope.correlation_id if envelope else None,
            causation_id=envelope.event_id if envelope else None,
        )

        try:
            await self._producer.send(
                topic=dlq_topic(record.topic),
                key=record.key.decode() if record.key else None,
                envelope=dlq_envelope,
            )
        except Exception:
            # If even the DLQ send fails, do not swallow it - re-raise so run()
            # skips the offset commit and the message is redelivered rather than
            # vanishing.
            logger.exception("failed to publish to DLQ", extra={"topic": record.topic})
            raise


def _safe_decode(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8", errors="replace")[:4000]
    except Exception:
        return "<undecodable>"
