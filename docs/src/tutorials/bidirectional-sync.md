# Bidirectional Sync

Some systems need two-way data flow: events flow *out* of PostgreSQL to a
message broker (forward relay), and events from that same broker flow *back
into* PostgreSQL (reverse relay). pg_tide handles this without any external
coordinator — you simply configure both directions as separate pipelines.

## When to use bidirectional sync

- **Microservice choreography:** Service A writes orders; Service B processes
  them and writes fulfilments back. Both share the same broker topic but
  different outboxes and inboxes.
- **Read model synchronisation:** Keep an Elasticsearch index or Redis cache
  updated by streaming writes out of PostgreSQL and projecting them back
  into a read database via the inbox.
- **Event sourcing with CQRS:** The write-side emits domain events to the
  outbox; the read-side rebuilds its projection from the inbox.

## Architecture

```
┌───────────────────────────────┐
│         PostgreSQL             │
│                               │
│  tide.outbox_messages         │  ──(forward)──►  NATS / Kafka
│                               │
│  tide.<name>_inbox            │  ◄──(reverse)──  NATS / Kafka
└───────────────────────────────┘
          ▲           │
          │   pg-tide relay
          └──────────┘
         (single process, two pipelines)
```

The relay reads both pipeline configurations from the `tide` schema on startup.
A single `pg-tide` process can run dozens of forward and reverse pipelines
simultaneously — no separate instances are required.

## Step-by-step example

### 1. Set up the outbox and inbox

```sql
-- Forward: orders flow out.
SELECT tide.outbox_create('orders');

-- Reverse: fulfilments flow in.
SELECT tide.inbox_create('fulfilments');
```

### 2. Configure the relay pipelines

```sql
-- Forward pipeline: outbox → NATS subject "orders.events"
SELECT tide.relay_set_outbox(
    'forward-orders',          -- pipeline name
    'orders',                  -- source outbox
    'nats',                    -- sink type
    '{"url":"nats://broker:4222","subject":"orders.events"}'::jsonb
);

-- Reverse pipeline: NATS subject "fulfilments.events" → inbox
SELECT tide.relay_set_inbox(
    'reverse-fulfilments',     -- pipeline name
    'nats',                    -- source type
    'fulfilments',             -- target inbox
    '{"url":"nats://broker:4222","subject":"fulfilments.events"}'::jsonb
);
```

### 3. Start the relay

```bash
pg-tide --postgres-url "$DATABASE_URL"
```

Both pipelines start automatically. The relay logs each pipeline direction on
startup:

```
INFO pipeline name=forward-orders  direction=Forward
INFO pipeline name=reverse-fulfilments direction=Reverse
```

### 4. Publish an order and receive the fulfilment

```sql
-- Application publishes an order:
SELECT tide.outbox_publish(
    'orders',
    '{"order_id": 1001, "item": "widget", "qty": 3}'::jsonb,
    '{}'::jsonb
);
```

The relay picks this up and publishes it to `orders.events`. The fulfilment
service consumes it, processes it, and publishes to `fulfilments.events`. The
relay's reverse pipeline writes the result into the inbox:

```sql
SELECT event_id, source, payload, received_at
FROM tide.fulfilments_inbox
ORDER BY received_at DESC
LIMIT 5;
```

## Preventing loops

Bidirectional sync carries the risk of infinite feedback loops if both sides
subscribe to the same topic. Prevent this with:

1. **Separate subjects/topics** for each direction (recommended).
2. **Event filtering**: check a custom header (e.g. `x-source: service-a`) in
   the relay's transform config and drop events originating from self.
3. **Inbox idempotency**: the inbox's `UNIQUE(event_id)` constraint silently
   ignores messages it has already processed.

## Monitoring

Both pipelines emit independent metrics:

| Metric | Labels |
|--------|--------|
| `pg_tide_relay_messages_consumed_total` | `pipeline=forward-orders, direction=forward` |
| `pg_tide_relay_messages_published_total` | `pipeline=forward-orders, direction=forward` |
| `pg_tide_relay_messages_consumed_total` | `pipeline=reverse-fulfilments, direction=reverse` |
| `pg_tide_relay_pipeline_healthy` | `pipeline=forward-orders` / `pipeline=reverse-fulfilments` |

Check consumer lag to verify neither side is falling behind:

```sql
SELECT group_name, consumer_id, lag, last_heartbeat
FROM tide.consumer_lag
ORDER BY lag DESC;
```
