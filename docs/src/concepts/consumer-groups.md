# Consumer Groups

Consumer groups bring Kafka-style consumption semantics to pg_tide's outbox. They allow multiple consumers to read from the same outbox independently, each tracking their own progress.

---

## Core Concepts

A **consumer group** is a named entity that tracks how far a set of consumers has read through an outbox. Each consumer within a group commits its offset — the ID of the last message it successfully processed.

Key properties:

- **Independent progress** — different groups read at their own pace
- **Offset tracking** — the group remembers where it left off across restarts
- **Heartbeats** — consumers signal liveness; stale consumers can be detected
- **Visibility leases** — in-flight messages are locked to prevent double-processing

---

## Creating a Consumer Group

```sql
SELECT tide.create_consumer_group('analytics-pipeline', 'orders',
  p_auto_offset_reset := 'earliest'
);
```

| Parameter | Options | Description |
|-----------|---------|-------------|
| `p_auto_offset_reset` | `earliest`, `latest`, `none` | Where to start if no committed offset exists |

- **earliest** — start from the first available message
- **latest** — start from the current end of the outbox
- **none** — error if no committed offset exists

---

## Committing Offsets

After successfully processing a batch of messages, commit the offset:

```sql
SELECT tide.commit_offset('analytics-pipeline', 'worker-1', 42);
```

This records that `worker-1` in the `analytics-pipeline` group has processed all messages up to and including ID 42.

---

## Heartbeats

Consumers periodically send heartbeats to signal they're alive:

```sql
SELECT tide.consumer_heartbeat('analytics-pipeline', 'worker-1');
```

Stale heartbeats (configurable threshold) indicate a dead consumer whose work should be rebalanced.

---

## Monitoring Consumer Lag

The `tide.consumer_lag` view shows how far behind each consumer is:

```sql
SELECT * FROM tide.consumer_lag;
```

```
 group_name           | outbox_name | consumer_id | committed_offset | lag  | last_heartbeat
----------------------+-------------+-------------+------------------+------+--------------------
 analytics-pipeline   | orders      | worker-1    |              42  | 158  | 2025-01-15 10:30:00
 notification-sender  | orders      | relay-0     |             200  |   0  | 2025-01-15 10:31:00
```

---

## Multiple Groups, One Outbox

A common pattern: the same outbox serves different purposes for different downstream systems:

```sql
-- The relay delivers to NATS
SELECT tide.create_consumer_group('nats-relay', 'orders');

-- An analytics service reads directly
SELECT tide.create_consumer_group('analytics', 'orders');

-- An audit logger tracks everything
SELECT tide.create_consumer_group('audit-log', 'orders');
```

Each group progresses independently. The relay might be at offset 1000 while analytics is still at 500 — they don't interfere with each other.

---

## Lifecycle

```sql
-- Create
SELECT tide.create_consumer_group('my-group', 'events');

-- Drop (also removes offsets and leases)
SELECT tide.drop_consumer_group('my-group');
```

Dropping a consumer group cascades to remove all offset records and visibility leases for that group.
