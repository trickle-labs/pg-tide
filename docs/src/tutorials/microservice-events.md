# Tutorial: Microservice Event Bus

This tutorial shows how to use pg_tide as a reliable event bus between microservices. Instead of direct service-to-service HTTP calls (which create tight coupling and cascade failures), services publish events to their local outbox and subscribe to events from other services via inboxes.

## Architecture

```
┌─────────────────┐         ┌──────────────────┐         ┌───────────────────┐
│  Order Service  │         │  Payment Service │         │  Shipping Service │
│                 │         │                  │         │                   │
│  outbox: orders │──┐  ┌──→│  inbox: payments │    ┌──→│  inbox: shipping  │
└─────────────────┘  │  │   └──────────────────┘    │   └───────────────────┘
                     │  │                            │
                     ↓  │                            │
              ┌─────────────┐                        │
              │    NATS     │────────────────────────┘
              │  (or Kafka) │
              └─────────────┘
                     ↑
                     │
┌─────────────────┐  │
│ Inventory Svc   │  │
│                 │  │
│ outbox: stock   │──┘
└─────────────────┘
```

## What You'll Build

- Order Service publishes `order.created`, `order.cancelled` events
- Payment Service subscribes to order events and processes payments
- Shipping Service subscribes to order events and initiates fulfillment
- Each service has its own PostgreSQL database with pg_tide

## Step 1: Order Service Setup

```sql
-- In the order service's database
CREATE EXTENSION pg_tide;
SELECT tide.outbox_create('order_events');
```

Application code publishes events within the order transaction:

```sql
BEGIN;
INSERT INTO orders (id, customer_id, total, status)
VALUES ('ORD-001', 'CUST-42', 149.99, 'created');

SELECT tide.outbox_publish('order_events', 'orders', jsonb_build_object(
    'event_type', 'order.created',
    'order_id', 'ORD-001',
    'customer_id', 'CUST-42',
    'total', 149.99,
    'items', jsonb_build_array(
        jsonb_build_object('sku', 'WIDGET-A', 'qty', 2),
        jsonb_build_object('sku', 'GADGET-B', 'qty', 1)
    )
));
COMMIT;
```

Configure the relay to publish to NATS:

```sql
SELECT tide.relay_set_outbox(
    'orders-to-nats',
    'order_events',
    '{
        "sink_type": "nats",
        "url": "nats://nats:4222",
        "subject_template": "events.orders.{op}"
    }'::jsonb
);
```

## Step 2: Payment Service Setup

```sql
-- In the payment service's database
CREATE EXTENSION pg_tide;
SELECT tide.inbox_create('payment_triggers');
```

Configure an inbox pipeline that subscribes to order events:

```sql
SELECT tide.relay_set_inbox(
    'orders-for-payments',
    'payment_triggers',
    '{
        "source_type": "nats",
        "url": "nats://nats:4222",
        "subject": "events.orders.>",
        "consumer_group": "payment-service",
        "durable_name": "payment-service"
    }'::jsonb
);
```

Process incoming events:

```sql
-- Payment service worker queries pending inbox messages
SELECT id, payload
FROM tide.inbox_pending('payment_triggers')
LIMIT 10;

-- After processing, mark as done
SELECT tide.inbox_mark_processed('payment_triggers', 42);
```

## Step 3: Shipping Service Setup

```sql
-- In the shipping service's database
CREATE EXTENSION pg_tide;
SELECT tide.inbox_create('shipping_triggers');

SELECT tide.relay_set_inbox(
    'orders-for-shipping',
    'shipping_triggers',
    '{
        "source_type": "nats",
        "url": "nats://nats:4222",
        "subject": "events.orders.>",
        "consumer_group": "shipping-service",
        "durable_name": "shipping-service",
        "transform": {
            "filter": "payload.event_type == '"'"'order.created'"'"'"
        }
    }'::jsonb
);
```

The shipping service only processes `order.created` events (not cancellations) thanks to the transform filter.

## Step 4: Start Relays

Each service runs its own relay instance:

```bash
# Order service relay
pg-tide --postgres-url "postgres://user:pass@orders-db/orders"

# Payment service relay
pg-tide --postgres-url "postgres://user:pass@payments-db/payments"

# Shipping service relay
pg-tide --postgres-url "postgres://user:pass@shipping-db/shipping"
```

## Benefits of This Architecture

**Loose coupling:** Services don't know about each other. The order service publishes events without knowing who consumes them. New consumers can subscribe without changing the producer.

**Reliability:** The transactional outbox guarantees events are published if and only if the business transaction commits. No dual-write problems.

**Independent scaling:** Each service scales independently. The payment service can process events at its own pace without affecting the order service.

**Resilience:** If the payment service is down, events queue in NATS (with durable consumers) and are delivered when it recovers. No lost events, no cascading failures.

## Further Reading

- [Sinks: NATS](../sinks/nats.md) — NATS JetStream configuration
- [Sources: NATS](../sources/nats.md) — NATS subscription configuration
- [Concepts: Message Guarantees](../concepts/message-guarantees.md) — Delivery semantics
- [Fan-Out Pattern](fan-out-pattern.md) — Multiple consumers for the same event stream
