# Event Sourcing with DuckLake as the Event Store

This tutorial shows how to use the pg-tide + DuckLake combination as an
event store for event sourcing: every domain event is written to PostgreSQL
transactionally, streamed to the lake, and queryable with time-travel for
projection rebuilds, audit, and debugging.

## What You'll Build

- An event-sourced order aggregate with state transitions captured as events
- A DuckLake event store with full time-travel support
- Projection rebuilds by replaying events from a specific offset or snapshot

## The Event Sourcing Pattern with pg-tide

```
Application code
    │  writes event inside transaction
    ▼
PostgreSQL (pg_tide outbox + your tables)
    │  relay polls outbox
    ▼
DuckLake (append-only event archive)
    │
    ├─► Projections: query latest state
    ├─► Time-travel: rebuild projections at any point in time
    └─► Audit: every state transition recorded with causal chain
```

## Step 1: Define Your Events

```sql
-- Create an event-sourced orders outbox.
SELECT tide.outbox_create(
    'order_events',
    'payload JSONB NOT NULL'
);

-- Configure DuckLake sink.
SELECT tide.relay_set_outbox_v2(jsonb_build_object(
    'name',       'order-events-lake',
    'outbox',     'order_events',
    'sink_type',  'ducklake',
    'batch_size', 25,
    'enabled',    true,
    'config', jsonb_build_object(
        'data_path',          's3://pg-tide-lake/order-events/',
        'namespace',          'pgtide',
        'table_template',     'order_events',
        'atomic_lake_writes', true,    -- exactly-once delivery
        'partition',          'daily'
    )
));
```

## Step 2: Publish Events Transactionally

```sql
-- Within your application transaction:
BEGIN;

-- Your business logic.
UPDATE orders SET status = 'shipped', shipped_at = now() WHERE order_id = 42;

-- Publish the domain event (same transaction = atomic).
PERFORM tide.outbox_publish('order_events', jsonb_build_object(
    'event_type',    'OrderShipped',
    'order_id',      42,
    'aggregate_id',  'order-42',
    'occurred_at',   now(),
    'payload', jsonb_build_object(
        'carrier',         'FedEx',
        'tracking_number', 'FX123456789'
    )
));

COMMIT;
```

## Step 3: Build Projections from DuckDB

```sql
-- Current state of all orders.
SELECT DISTINCT ON (order_id)
    order_id,
    event_type AS last_event,
    occurred_at,
    payload
FROM lake.pgtide.order_events
ORDER BY order_id, occurred_at DESC;

-- Full event history for order 42.
SELECT event_type, occurred_at, payload
FROM lake.pgtide.order_events
WHERE payload->>'aggregate_id' = 'order-42'
ORDER BY occurred_at ASC;
```

## Step 4: Replay from a Specific Point in Time

```sql
-- Get the time-travel expression for replaying from offset 100.
SELECT tide.ducklake_replay_range('order-events-lake', 100, 200);
-- Returns: "AT (VERSION => 5) THROUGH AT (VERSION => 10)"

-- In DuckDB: rebuild projection as of snapshot 5.
SELECT event_type, COUNT(*)
FROM lake.pgtide.order_events AT (VERSION => 5)
GROUP BY event_type;
```

## Benefits Over a Traditional Event Store

| | pg-tide + DuckLake | Traditional Event Store (Kafka, EventStoreDB) |
|---|---|---|
| **Write guarantee** | Atomic with business transaction | Separate distributed write |
| **Query language** | Full SQL + window functions | Proprietary or limited |
| **Time-travel** | Built-in via DuckDB `AT (VERSION =>)` | Manual snapshotting |
| **Storage cost** | Parquet on cheap object storage | Typically expensive SSD |
| **Operational complexity** | Zero (reuses existing PostgreSQL) | Separate service to operate |

## Next Steps

- [Migrating from Kafka Connect](05-migrating-from-kafka-connect.md)
