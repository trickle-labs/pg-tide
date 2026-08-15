# pg-trickle Integration

[pg-trickle](https://github.com/trickle-labs/pg-trickle) is the PostgreSQL
extension that pg_tide was originally extracted from. If you already use
pg-trickle, you can adopt pg_tide incrementally — or run both side-by-side
during a migration.

## When to use pg_tide instead of pg-trickle

| Situation | Recommendation |
|-----------|---------------|
| Starting a new project | Use pg_tide — it is the focused, standalone successor |
| Existing pg-trickle installation | Migrate when ready; pg_tide's schema is compatible |
| Need stream tables (`pg_trickle_streams`) | Stay on pg-trickle for now |
| Need transactional outbox + inbox only | pg_tide covers this fully |

## Stream tables and the inbox→outbox bridge

pg-trickle's stream tables enable a powerful routing pattern: a stream table
attached to an outbox can watch a pg_tide inbox, and any new row inserted into
the inbox is automatically republished to the outbox. This creates a two-hop
path from an external source to a downstream sink:

```
External source (Kafka) → relay → inbox (PostgreSQL)
                                        ↓ stream table trigger
                                   outbox (PostgreSQL) → relay → sink (DuckLake)
```

This pattern is functionally equivalent to pg_tide's reverse pipeline sinks
(where the relay routes directly from source to sink without any PostgreSQL
intermediate), but the two approaches have different guarantees, costs, and
capabilities.

**The critical distinction is durability and the dedup boundary:**

- The inbox→outbox bridge provides **PostgreSQL-strength exactly-once**: the
  inbox `UNIQUE(event_id)` constraint is the dedup wall. Once a message is in
  PostgreSQL, it is durable regardless of Kafka retention or downstream sink
  availability. Messages accumulate in the outbox and are delivered in order
  when the sink recovers — even after days of outage.

- pg_tide's reverse pipeline sinks provide **sink-dependent exactly-once**: the
  relay routes directly from Kafka to the sink (DuckLake, MongoDB, etc.) using
  `_dedup_key` for idempotency. Durability depends on Kafka retention — if the
  sink is unavailable for longer than the Kafka topic's retention period,
  messages that have not yet been delivered are lost.

**Use the inbox→outbox bridge when** you need unconditional durability, SQL
transforms or enrichment between receipt and publication, fan-out to multiple
sinks, or a full PostgreSQL audit trail.

**Use the reverse pipeline sink when** the path is a simple A→B data move, Kafka
retention is long enough to absorb any realistic outage, and the PostgreSQL write
overhead is not justified by business requirements.

See [Message Guarantees — Reverse Pipeline Sink vs. Inbox→Outbox Bridge](../concepts/message-guarantees.md#reverse-pipeline-sink-vs-the-inboxoutbox-bridge-pattern)
for a full side-by-side comparison including the durability table, dedup
boundary explanation, and decision guide.

## Schema compatibility

pg_tide uses a `tide.*` schema prefix. pg-trickle uses `pg_trickle_*` table
names. The two schemas can coexist in the same database without conflict.

## Migrating from pg-trickle

### 1. Install pg_tide alongside pg-trickle

```sql
CREATE EXTENSION pg_tide;
```

### 2. Create matching outboxes and inboxes

For each `pg_trickle_outbox` in your existing schema:

```sql
SELECT tide.outbox_create(outbox_name, retention_hours)
FROM pg_trickle_outbox_config;
```

### 3. Migrate pending messages

```sql
INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers, created_at)
SELECT stream_name, payload, headers, created_at
FROM pg_trickle_outbox_messages
WHERE consumed_at IS NULL;
```

### 4. Point the relay at pg_tide

Update your `pg-tide` relay configuration:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'my-pipeline',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', '{"url":"nats://broker:4222"}'::jsonb
  )
);
```

### 5. Verify and cut over

Run both relays in parallel during the transition. Once the pg_tide relay is
processing all new messages, decommission the pg-trickle relay.

## Using both together

Both extensions can write to the same NATS subject or Kafka topic — consumers
should use the `x-source` header to distinguish messages originating from
pg_tide versus pg-trickle.

pg_tide stamps every envelope with:

```json
{
  "id": 42,
  "outbox_name": "orders",
  "payload": { ... },
  "headers": { "x-source": "pg-tide", "x-outbox": "orders" }
}
```
