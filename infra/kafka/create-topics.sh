#!/usr/bin/env bash
# Creates every topic explicitly so partition counts are a design decision rather
# than whatever auto-creation happens to pick.
#
# Partition count = how much parallelism a topic can ever have. Order-scoped
# topics are keyed by order_id, so the same order always lands on the same
# partition => per-order ordering is guaranteed while different orders process in
# parallel. That single choice is what makes the saga correct under load.
#
# Every business topic gets a matching .DLQ, derived from ONE list below - adding
# a topic in two places is how a DLQ ends up missing and a poison message blocks a
# partition with nowhere to park.
#
# replication.factor=1 because this is a single-broker local setup. Production
# needs 3+ brokers with min.insync.replicas=2.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
RETENTION_MS=$((7 * 24 * 60 * 60 * 1000))   # 7 days

# topic:partitions:key-comment
# Must stay in sync with common/events/topics.py (Topics.all()).
TOPICS=(
  "user.registered:3:user_id"
  "catalog.product.upserted:3:product_id"
  "order.created:6:order_id"
  "order.confirmed:6:order_id"
  "order.cancelled:3:order_id"
  "inventory.reserved:6:order_id"
  "inventory.reservation_failed:3:order_id"
  "inventory.stock.changed:6:product_id"
  "inventory.low_stock:3:product_id"
  "payment.succeeded:6:order_id"
  "payment.failed:3:order_id"
  "payment.refunded:3:order_id"
)

create() {
  local topic="$1" partitions="$2" note="${3:-}"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    >/dev/null
  printf '  ok  %-34s partitions=%-2s %s\n' "$topic" "$partitions" "${note:+key=$note}"
}

echo "waiting for broker at $BOOTSTRAP ..."
until kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  sleep 2
done
echo "broker is up."

echo
echo "business topics:"
for entry in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions note <<< "$entry"
  create "$topic" "$partitions" "$note"
done

# One partition each: DLQ traffic is tiny, and inspection order matters more than
# throughput.
echo
echo "dead letter queues:"
for entry in "${TOPICS[@]}"; do
  IFS=':' read -r topic _ _ <<< "$entry"
  create "${topic}.DLQ" 1
done

echo
echo "created $(( ${#TOPICS[@]} * 2 )) topics (${#TOPICS[@]} business + ${#TOPICS[@]} DLQ)"
echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --list | sort
