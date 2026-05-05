# Tutorial: Cross-Region Event Replication

This tutorial shows how to replicate events between PostgreSQL instances in different geographic regions using pg_tide. Each region maintains its own outbox and inbox, with NATS (or Kafka) as the cross-region transport.

## Architecture

```
Region US-East                          Region EU-West
┌───────────────────┐                   ┌───────────────────┐
│  PostgreSQL (US)  │                   │  PostgreSQL (EU)  │
│                   │                   │                   │
│  outbox: orders   │──→ pg_tide ──┐    │  inbox: orders    │
│  inbox: orders    │              │    │  outbox: orders   │──→ pg_tide ──┐
└───────────────────┘              │    └───────────────────┘              │
                                   │                                       │
                                   ↓                                       ↓
                            ┌─────────────┐                         ┌─────────────┐
                            │  NATS (US)  │←── NATS Gateway ──────→│  NATS (EU)  │
                            └─────────────┘                         └─────────────┘
                                   ↑                                       ↑
                                   │                                       │
                            pg_tide ──→ inbox (US)                  pg_tide ──→ inbox (EU)
```

## What You'll Build

- Orders created in US-East are replicated to EU-West within seconds
- Orders created in EU-West are replicated to US-East
- Each region can operate independently during network partitions
- The inbox provides deduplication for messages that arrive during recovery

## Step 1: Setup US-East Region

```sql
-- US-East PostgreSQL
CREATE EXTENSION pg_tide;
SELECT tide.outbox_create('order_events');
SELECT tide.inbox_create('replicated_orders');
```

Publish pipeline (US → NATS):

```sql
SELECT tide.relay_set_outbox(
    'us-orders-publish',
    'order_events',
    '{
        "sink_type": "nats",
        "url": "nats://nats-us:4222",
        "subject_template": "orders.{op}.us-east",
        "stream": "ORDERS"
    }'::jsonb
);
```

Subscribe pipeline (NATS → US inbox, for EU-originated events):

```sql
SELECT tide.relay_set_inbox(
    'eu-orders-subscribe',
    'replicated_orders',
    '{
        "source_type": "nats",
        "url": "nats://nats-us:4222",
        "subject": "orders.*.eu-west",
        "stream": "ORDERS",
        "consumer_group": "us-east-replica",
        "durable_name": "us-east-replica"
    }'::jsonb
);
```

## Step 2: Setup EU-West Region

```sql
-- EU-West PostgreSQL
CREATE EXTENSION pg_tide;
SELECT tide.outbox_create('order_events');
SELECT tide.inbox_create('replicated_orders');
```

Publish pipeline (EU → NATS):

```sql
SELECT tide.relay_set_outbox(
    'eu-orders-publish',
    'order_events',
    '{
        "sink_type": "nats",
        "url": "nats://nats-eu:4222",
        "subject_template": "orders.{op}.eu-west",
        "stream": "ORDERS"
    }'::jsonb
);
```

Subscribe pipeline (NATS → EU inbox, for US-originated events):

```sql
SELECT tide.relay_set_inbox(
    'us-orders-subscribe',
    'replicated_orders',
    '{
        "source_type": "nats",
        "url": "nats://nats-eu:4222",
        "subject": "orders.*.us-east",
        "stream": "ORDERS",
        "consumer_group": "eu-west-replica",
        "durable_name": "eu-west-replica"
    }'::jsonb
);
```

## Step 3: Configure NATS Gateway

NATS supports multi-region replication via [Gateway connections](https://docs.nats.io/running-a-nats-service/configuration/gateways):

```yaml
# nats-us.conf
gateway {
  name: "us-east"
  listen: "0.0.0.0:7222"
  gateways: [
    { name: "eu-west", urls: ["nats://nats-eu:7222"] }
  ]
}

# nats-eu.conf
gateway {
  name: "eu-west"
  listen: "0.0.0.0:7222"
  gateways: [
    { name: "us-east", urls: ["nats://nats-us:7222"] }
  ]
}
```

## Step 4: Application Usage

In US-East:
```sql
BEGIN;
INSERT INTO orders (id, region, customer_id, total)
VALUES ('ORD-US-001', 'us-east', 'CUST-42', 149.99);

SELECT tide.outbox_publish('order_events', 'orders', jsonb_build_object(
    'order_id', 'ORD-US-001',
    'region', 'us-east',
    'customer_id', 'CUST-42',
    'total', 149.99
));
COMMIT;
```

This event automatically replicates to EU-West via: outbox → NATS (US) → NATS Gateway → NATS (EU) → inbox (EU).

## Handling Network Partitions

During a network partition between regions:

1. Each region continues operating independently (local outbox/inbox works fine)
2. Cross-region messages queue in the local NATS JetStream
3. When connectivity restores, NATS gateways resync automatically
4. The inbox's deduplication prevents double-processing of any message

## Conflict Resolution

For bidirectional replication, you need a conflict resolution strategy. Options:

- **Last-writer-wins:** Include timestamps, take the latest
- **Region priority:** US-East wins ties
- **Merge:** Application-specific merge logic in the inbox processor
- **CRDT-style:** Use commutative operations (add-only sets, counters)

## Further Reading

- [Sinks: NATS](../sinks/nats.md) — NATS JetStream configuration
- [Sources: NATS](../sources/nats.md) — NATS subscription configuration
- [Bidirectional Sync](bidirectional-sync.md) — Two-way sync patterns
- [HA Coordination](../features/ha-coordination.md) — Per-region relay HA
