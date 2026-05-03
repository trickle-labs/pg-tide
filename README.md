# pg_tide

Transactional outbox, idempotent inbox, and relay catalog for PostgreSQL 18+.

Extracted from [pg_trickle](https://github.com/trickle-labs/pg-trickle) v0.46.0
as a standalone extension that works independently of pg_trickle.

## Features

- **Transactional Outbox** — publish messages within a database transaction; no 2PC, no dual-writes
- **Idempotent Inbox** — exactly-once delivery semantics with event deduplication
- **Relay Catalog** — store forward/reverse relay pipeline configurations, read by the `pg-tide` binary
- **Consumer Groups** — named consumer groups with committed offsets and heartbeat tracking

## Installation

```sql
CREATE EXTENSION pg_tide;
```

## Quick Start

```sql
-- Create an outbox
SELECT tide.outbox_create('orders', 24, 10000);

-- Publish a message (within your business transaction)
SELECT tide.outbox_publish('orders', '{"order_id": 42}'::jsonb, '{}'::jsonb);

-- Create a consumer group
SELECT tide.create_consumer_group('my-consumer', 'orders');

-- Check status
SELECT tide.outbox_status('orders');
```

## Schema

All objects live in the `tide` schema:

| Object | Type | Description |
|--------|------|-------------|
| `tide.outbox_create()` | function | Create a named outbox |
| `tide.outbox_publish()` | function | Publish a message to an outbox |
| `tide.outbox_drop()` | function | Drop an outbox and all its messages |
| `tide.outbox_status()` | function | Status summary as JSONB |
| `tide.outbox_disable()` | function | Pause publishing |
| `tide.outbox_enable()` | function | Resume publishing |
| `tide.create_consumer_group()` | function | Create a consumer group |
| `tide.drop_consumer_group()` | function | Drop a consumer group |
| `tide.commit_offset()` | function | Commit consumer offset |
| `tide.consumer_heartbeat()` | function | Update consumer heartbeat |
| `tide.inbox_create()` | function | Create a named inbox |
| `tide.inbox_drop()` | function | Drop an inbox |
| `tide.inbox_mark_processed()` | function | Mark a message as processed |
| `tide.inbox_mark_failed()` | function | Mark a message as failed |
| `tide.inbox_status()` | function | Inbox status as JSONB |
| `tide.relay_set_outbox()` | function | Configure forward relay pipeline |
| `tide.relay_set_inbox()` | function | Configure reverse relay pipeline |
| `tide.relay_enable()` | function | Enable a relay pipeline |
| `tide.relay_disable()` | function | Disable a relay pipeline |
| `tide.relay_delete()` | function | Delete a relay pipeline |
| `tide.outbox_pending` | view | Pending messages per outbox |
| `tide.consumer_lag` | view | Consumer lag per group |

## Integration with pg_trickle

If you use [pg_trickle](https://github.com/trickle-labs/pg-trickle) ≥ v0.46.0,
install pg_tide first and then use `pgtrickle.attach_outbox()` to automatically
publish stream table changes to an outbox:

```sql
-- Install pg_tide first
CREATE EXTENSION pg_tide;

-- Then attach to a stream table
SELECT pgtrickle.attach_outbox('my_stream_table', retention_hours := 48);
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
