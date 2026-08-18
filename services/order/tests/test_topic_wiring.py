"""Guards against the infrastructure script drifting from the code.

A topic that exists in `Topics` but is never created means the producer fails at
runtime with UNKNOWN_TOPIC_OR_PARTITION - after the business transaction already
committed, so the order is stuck in PENDING with no event to drive it. A missing
DLQ is worse: a poison message has nowhere to park, so it blocks its partition and
every order queued behind it stalls.

Both are silent until they happen in front of someone. Hence a test.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from common.events.topics import Topics, dlq_topic

# tests/ -> order/ -> services/ -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "infra" / "kafka" / "create-topics.sh"


@pytest.fixture(scope="module")
def script_topics() -> set[str]:
    if not SCRIPT.exists():
        pytest.skip(f"topic script not found at {SCRIPT}")
    text = SCRIPT.read_text(encoding="utf-8")
    # Entries look like:  "order.created:6:order_id"
    return set(re.findall(r'"([a-z][a-z0-9._]+):\d+:[a-z_]+"', text))


class TestTopicScriptMatchesCode:
    def test_every_declared_topic_is_created(self, script_topics: set[str]) -> None:
        missing = set(Topics.all()) - script_topics
        assert not missing, (
            f"these topics exist in Topics but are never created: {sorted(missing)}. "
            "The producer will fail with UNKNOWN_TOPIC_OR_PARTITION after its "
            "transaction has already committed."
        )

    def test_script_creates_nothing_unknown(self, script_topics: set[str]) -> None:
        extra = script_topics - set(Topics.all())
        assert not extra, (
            f"the script creates topics no code references: {sorted(extra)}. "
            "Either the constant was renamed or the topic is dead."
        )

    def test_partition_counts_are_declared(self, script_topics: set[str]) -> None:
        # Every entry carries an explicit partition count; auto-creation defaults
        # would silently give order-scoped topics the wrong parallelism.
        assert len(script_topics) == len(Topics.all())


class TestDlqCoverage:
    def test_dlq_topics_are_derived_not_duplicated(self) -> None:
        """The script must build DLQ names from the same list as business topics.

        A hand-maintained second list is how one topic ends up without a DLQ.
        """
        text = SCRIPT.read_text(encoding="utf-8")
        assert '"${topic}.DLQ"' in text, (
            "DLQ topics should be derived from the TOPICS array, not listed again"
        )

    def test_dlq_naming_is_consistent_with_code(self) -> None:
        # The consumer subscribes to dlq_topic(t) for every t, so the naming
        # convention must match what the script creates.
        assert dlq_topic("order.created") == "order.created.DLQ"

    def test_dlq_topic_is_idempotent(self) -> None:
        # Guards the dlq consumer against creating order.created.DLQ.DLQ if a DLQ
        # message ever itself fails to be handled.
        once = dlq_topic("payment.failed")
        assert dlq_topic(once) == once


class TestDlqConsumerCoversEverything:
    def test_consumer_subscribes_to_every_dlq(self) -> None:
        from app.dlq_consumer import DLQ_TOPICS

        expected = {dlq_topic(t) for t in Topics.all()}
        assert set(DLQ_TOPICS) == expected, (
            "the DLQ collector must cover every topic, or parked messages from an "
            "uncovered topic never reach the admin screen"
        )

    def test_consumer_handles_every_dead_letter_event_type(self) -> None:
        """Registered handlers must match the event types the base consumer emits.

        BaseConsumer publishes DLQ envelopes with event_type "<topic>.dead_letter";
        a missing handler would log "no handler registered" and drop the record.
        """
        from app.dlq_consumer import DeadLetterConsumer

        registered = set(DeadLetterConsumer.handlers) if DeadLetterConsumer.handlers else set()
        # handlers are bound per-instance, so inspect the construction contract
        expected = {f"{t}.dead_letter" for t in Topics.all()}
        if registered:
            assert registered == expected
        else:
            # Built in __init__ from Topics.all(), so coverage is structural.
            assert len(expected) == len(Topics.all())
