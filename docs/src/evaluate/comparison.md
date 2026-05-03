# Comparison with Alternatives

How does pg_tide compare to other approaches for event publishing and message relay?

---

## pg_tide vs. Debezium

| Aspect | pg_tide | Debezium |
|--------|---------|----------|
| **Mechanism** | Application writes to outbox table | CDC via PostgreSQL logical replication |
| **Message format** | You control the payload (JSONB) | Mirrors row-level changes (schema-coupled) |
| **Infrastructure** | PostgreSQL + relay binary | PostgreSQL + Kafka Connect + Kafka |
| **Exactly-once** | Built-in via inbox dedup | Requires downstream idempotency |
| **Operational cost** | Single binary, no JVM | JVM-based, requires Kafka cluster |
| **Flexibility** | Arbitrary events, not tied to table schema | Captures all row changes automatically |

**Choose pg_tide** when you want explicit control over what events you publish and prefer minimal infrastructure. **Choose Debezium** when you need automatic capture of all database changes without modifying application code.

---

## pg_tide vs. Application-Level Outbox (DIY)

| Aspect | pg_tide | Custom outbox table |
|--------|---------|-------------------|
| **Setup time** | `CREATE EXTENSION pg_tide;` | Design tables, polling logic, retry, dedup… |
| **Consumer groups** | Built-in with offsets and heartbeats | You build it |
| **Relay** | Multi-backend binary with metrics | You build it |
| **Idempotent inbox** | Built-in | You build it |
| **Maintenance** | Upgrade extension, update relay | Maintain custom code indefinitely |

**Choose pg_tide** to avoid reinventing reliable messaging infrastructure. **Choose DIY** when you have very specific requirements that don't map to pg_tide's model.

---

## pg_tide vs. pg_notify / LISTEN

| Aspect | pg_tide | pg_notify |
|--------|---------|-----------|
| **Durability** | Messages persist until consumed | Fire-and-forget (lost if no listener) |
| **Payload size** | JSONB (up to 1 GB) | 8000 bytes max |
| **Retry** | Built-in with DLQ | None |
| **Consumer groups** | Yes | No |
| **Cross-network** | Relay bridges to any system | Only in-process PostgreSQL clients |

**Choose pg_tide** when you need durable, reliable delivery. **Choose pg_notify** for lightweight real-time signals where message loss is acceptable.

---

## pg_tide vs. Kafka/NATS Directly

| Aspect | pg_tide | Direct broker writes |
|--------|---------|---------------------|
| **Transactional safety** | Guaranteed (same transaction) | Dual-write risk |
| **Operational overhead** | Extension + relay binary | Broker cluster + application integration |
| **Throughput ceiling** | PostgreSQL write speed | Broker-native (higher) |
| **Latency** | Poll interval (~100ms) | Near real-time |

**Choose pg_tide** when transactional correctness is paramount and throughput fits within PostgreSQL's capacity. **Choose direct broker writes** when you accept the dual-write tradeoff for maximum throughput and minimum latency.

---

## Summary

pg_tide sits in the middle ground: **simpler than full CDC infrastructure, more reliable than direct broker writes, and more capable than DIY outbox tables**. It's the right choice when PostgreSQL is your source of truth and you want a battle-tested pattern without the operational burden of a streaming platform.
