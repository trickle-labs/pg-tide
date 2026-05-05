# Concept: The Transactional Outbox Pattern

The transactional outbox pattern solves one of the most common problems in distributed systems: how to reliably update a database and publish an event at the same time. Without it, you face the "dual-write problem" — a situation where your database write succeeds but the message publish fails (or vice versa), leaving your system in an inconsistent state.

## The Problem

Consider an e-commerce application that needs to save an order and notify the shipping service:

```
BEGIN;
  INSERT INTO orders (id, status) VALUES ('ORD-001', 'confirmed');
COMMIT;

-- Outside the transaction:
publish_to_kafka('order.confirmed', { order_id: 'ORD-001' });
```

What happens if the application crashes between the COMMIT and the publish? The order exists in the database, but the shipping service never learns about it. The event is lost.

What if you reverse the order — publish first, then commit? If the COMMIT fails (constraint violation, connection loss), the event was already published for an order that doesn't exist.

There is no safe ordering for two independent systems. This is the dual-write problem.

## The Solution

The transactional outbox pattern writes the event to an "outbox" table in the same database transaction as the business data:

```sql
BEGIN;
  INSERT INTO orders (id, status) VALUES ('ORD-001', 'confirmed');
  SELECT tide.outbox_publish('order_events', 'orders', '{"order_id": "ORD-001", "status": "confirmed"}');
COMMIT;
```

Both writes are part of the same ACID transaction. Either both succeed or both fail. There is no inconsistency window.

A separate process (the relay) polls the outbox table and publishes events to external systems. If the relay crashes, it simply resumes from where it left off — the events are safely persisted in PostgreSQL.

## Guarantees

The transactional outbox provides:

1. **Atomicity** — The business operation and event publication succeed or fail together
2. **Durability** — Events survive crashes (they're in PostgreSQL's WAL)
3. **Ordering** — Events from the same outbox are delivered in the order they were written
4. **At-least-once delivery** — Every committed event will eventually be delivered to the sink

## How pg_tide Implements It

```
Application                  PostgreSQL                    Relay                    Sink
    │                            │                          │                       │
    │─── BEGIN ─────────────────→│                          │                       │
    │─── INSERT INTO orders ────→│                          │                       │
    │─── outbox_publish() ──────→│ (writes to outbox table) │                       │
    │─── COMMIT ────────────────→│                          │                       │
    │                            │                          │                       │
    │                            │←── poll outbox ──────────│                       │
    │                            │─── return rows ─────────→│                       │
    │                            │                          │─── publish ──────────→│
    │                            │                          │←── ack ───────────────│
    │                            │←── mark delivered ───────│                       │
```

The relay advances through the outbox table sequentially. After successful delivery, it advances its cursor. If it crashes and restarts, it re-reads from the last acknowledged position — potentially re-delivering a few messages (at-least-once), but never losing any.

## When to Use This Pattern

Use the transactional outbox when:
- You need to update a database and publish an event reliably
- Consistency between your database and event stream matters
- You can't afford lost events (order notifications, payment confirmations, audit logs)
- You want to decouple your application from the messaging infrastructure

## Comparison with Alternatives

| Approach | Consistency | Complexity | Trade-offs |
|----------|:-----------:|:----------:|-----------|
| Transactional outbox (pg_tide) | Strong | Low | Slight delivery latency (polling interval) |
| WAL-based CDC (Debezium) | Eventual | Medium | Captures all changes, less control |
| Dual-write (publish + commit) | Weak | Low | Events can be lost or orphaned |
| Saga / 2PC | Strong | High | Complex failure handling |

## Further Reading

- [Architecture](../evaluate/architecture.md) — How pg_tide implements the pattern
- [Message Guarantees](message-guarantees.md) — Delivery semantics in detail
- [Tutorial: Getting Started](../tutorials/getting-started.md) — Hands-on first pipeline
