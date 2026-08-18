# E-Commerce Order Processing — Backend

Event-driven microservices in Python: FastAPI + Kafka + PostgreSQL + Redis.
Architecture and rationale live in [`../DESIGN.md`](../DESIGN.md).

**Status: complete.** Six services, an API gateway, 18 application containers,
135 passing tests.

## Prerequisites

**Docker Desktop is required and was not installed when this was built.**
Get it from https://www.docker.com/products/docker-desktop, then reopen your
terminal so `docker` is on `PATH`. Kafka and Postgres both run as containers;
there is no supported path that skips Docker.

## Running it

```powershell
cd "E:\E-Commerce Order Processing System\E-Commerce Backend"
Copy-Item .env.example .env
.\dev.ps1 up                                     # build + start all 23 containers
docker compose exec catalog-api  python -m app.seed   # 20 products + price events
docker compose exec inventory-api python -m app.seed  # real stock records
```

Both seeds are needed and the order matters: the catalog seed publishes
`catalog.product.upserted`, which Order consumes to learn prices, and the
inventory seed reads the catalog to create stock rows.

| What | Where |
| --- | --- |
| **API gateway** (use this) | http://localhost:8000 |
| Auth · Catalog · Order · Inventory · Payment docs | :8001 · :8002 · :8003 · :8004 · :8005 → `/docs` |
| Kafka UI | http://localhost:8080 |
| Postgres | `localhost:5432` (`app` / `app`) |

## Demo: watch the saga run

```bash
# 1. log in as the bootstrap admin (see "Becoming admin")
TOKEN=$(curl -s -X POST localhost:8000/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"Admin@12345"}' | jq -r .access_token)

# 2. pick a product
PID=$(curl -s "localhost:8000/products?size=1" | jq -r '.items[0].id')

# 3. HAPPY PATH - tok_test_success
curl -s -X POST localhost:8000/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d "{\"items\":[{\"product_id\":\"$PID\",\"quantity\":1}],
       \"shipping_address\":{\"line1\":\"1 Road\",\"city\":\"Pune\",\"state\":\"MH\",\"postal_code\":\"411001\"},
       \"payment_method\":{\"type\":\"CARD\",\"token\":\"tok_test_success\",\"last4\":\"4242\"}}"
# -> 202 Accepted, status PENDING

# 4. watch it progress: PENDING -> INVENTORY_RESERVED -> PAID -> CONFIRMED
curl -N -H "Authorization: Bearer $TOKEN" localhost:8000/orders/<order_id>/stream

# 5. COMPENSATION - same call with tok_test_declined
#    -> FAILED, and the reserved stock returns to available
```

Step 5 is the point of the project. Check the stock came back:

```bash
curl -s localhost:8000/stock/$PID | jq '{available_qty, reserved_qty}'
```

## Becoming admin

Signup **cannot** create an admin. `RegisterRequest` has no `role` field and
`AuthService.register` hardcodes `Role.CUSTOMER`; a test asserts both, including
that nobody later adds a `role=role` parameter "for convenience".

The first admin comes from `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_PASSWORD`
in `.env`. On startup Auth creates that user with `role=admin` if absent —
idempotent, and it survives `docker compose down -v`. Promote others with
`PATCH /auth/users/{id}/role`.

## The services

| Service | Port | Owns | Processes |
| --- | --- | --- | --- |
| **gateway** | 8000 | nothing | 1 |
| **auth** | 8001 | users, credentials, roles | api, outbox |
| **catalog** | 8002 | products, categories | api, consumer, outbox |
| **order** | 8003 | orders, the saga state machine | api, consumer, outbox, **dlq** |
| **inventory** | 8004 | **stock (source of truth)** | api, consumer, outbox, **sweeper** |
| **payment** | 8005 | payments, refunds | api, consumer, outbox |
| **notification** | — | notification log | consumer only |

Consumers are separate **processes** from the APIs. If a consumer ran inside an
API process, scaling that API to N replicas would put N members in the consumer
group and trigger a rebalance on every deploy.

## Event flow

```
order.created ─────────────▶ inventory reserves (FOR UPDATE, TTL 15m)
                                    │
inventory.reserved ◀────────────────┘
        ├──▶ order: INVENTORY_RESERVED
        └──▶ payment: charge
                 │
    ┌────────────┴─────────────┐
    ▼                          ▼
payment.succeeded          payment.failed
 ├─ order: PAID→CONFIRMED   ├─ order: FAILED
 └─ inventory: commit       ├─ inventory: RELEASE ← compensation
                            └─ notification: "declined, items returned"
```

12 topics, each with a `.DLQ`. Order-scoped topics are keyed by `order_id`, so
one order's events stay ordered while different orders process in parallel.

Nobody instructs the compensation. Payment publishes a fact; Inventory decides
on its own that the fact voids its reservation.

## Tests

```powershell
.\dev.ps1 test
```

135 passing, 8 skipped. No database or broker needed:

| Suite | Covers |
| --- | --- |
| auth (22) | bcrypt, JWT, timing-safe login, **privilege-escalation guard** |
| catalog (7) | cache keys, stock not editable via catalog |
| inventory (12) | reservation arithmetic, ledger reasons, thresholds |
| order (47) | **saga state machine**, client can't set prices, topic wiring |
| payment (15) | **gateway determinism**, failure codes, retryability |
| gateway (32) | prefix routing, `/admin/*` split across four services |

The 8 skipped are the ones that need real Postgres, because row-level locking
cannot be tested against sqlite or a mock — sqlite serialises everything, so the
test would pass whether or not the lock existed:

```powershell
docker compose up -d postgres
$env:TEST_DATABASE_URL="postgresql+asyncpg://app:app@localhost:5432/inventory_db"
cd services\inventory; pytest -m integration
```

They prove the important things: **10 concurrent orders against 5 units yield
exactly 5 winners**, a duplicate `order.created` reserves once, release restores
stock exactly, and the sweeper reclaims expired reservations.

## Operating it

- **`GET /admin/dlq`** — parked messages with stack traces, plus replay and
  discard. A non-zero `PARKED` count means a consumer is failing permanently.
- **`GET /admin/orders/stats`** — counts by status, 24h revenue, DLQ depth.
- **`/metrics`** on every service. The one to alert on is `outbox_pending`: if it
  climbs, the publisher is stuck and orders are silently stalling in `PENDING`
  with no error raised anywhere.
- **Correlation IDs** — one id per checkout, threaded through every event and log
  line: `docker compose logs --no-log-prefix | jq 'select(.correlation_id=="…")'`

## Two things worth knowing

**Order prices from its own read-model.** Order must snapshot the price at order
time, but it cannot call Catalog (that would make Catalog a synchronous dependency
of checkout) and cannot trust a price from the browser. So Catalog publishes
`catalog.product.upserted` and Order keeps a local `product_snapshots` table.
This was not in the original design — it is a gap found while building.

**Payment travels as a token, never a card number.** Kafka topics are retained for
seven days, replayed into DLQs, and readable in Kafka UI. The cost is that
Inventory relays payment data it has no interest in, documented in the schema.

## Known limitations

1. `products.cached_stock` is a display copy and eventually consistent. Checkout
   re-validates against Inventory, so it can never cause an oversell — only an
   occasional "just sold out" at checkout, which real stores do too.
2. Role changes take effect when the access token expires (15 min), the standard
   cost of stateless auth. Fix would be a Redis denylist of revoked `jti`s.
3. SSE polls the database per connected client. Fine at this scale; Redis pub/sub
   is the upgrade.
4. DLQ replay is manual by design — auto-replaying a deterministically failing
   message is a self-inflicted denial of service.
5. Migrations run from `entrypoint.sh`; concurrent API replicas would race.
   Production should run them as a separate step.
6. Single Kafka broker, so `replication.factor=1`.
7. Events are JSON. Avro + Schema Registry is the schema-evolution upgrade.

## Layout

```
common/                shared library, pip-installed into every image
  events/              envelope + all 12 event payloads
  kafka/               producer, BaseConsumer (retry → DLQ, dedup, manual commit)
  db/                  session, outbox, idempotency mixins
  auth/                JWT creation + local verification, require_role
  order_status.py      the state machine, shared by three services
  observability/       JSON logging, correlation ids, metrics, health
services/{auth,catalog,order,inventory,payment,notification,gateway}/
infra/                 postgres init SQL, kafka topic script
```
