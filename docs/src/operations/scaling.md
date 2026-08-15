# Scaling

Strategies for scaling pg_tide as your message volume grows.

---

## Relay Scaling

### Horizontal (more instances)

Run multiple relay instances with the same `relay_group_id`. PostgreSQL advisory locks distribute pipeline ownership automatically.

- Each pipeline is handled by exactly one relay at a time
- Adding more relays improves failover speed, not per-pipeline throughput
- Useful when you have many pipelines

### Vertical (more resources)

For a single high-throughput pipeline:

- Increase `batch_size` to reduce per-message overhead
- Tune `sink_max_inflight` for higher concurrency
- Ensure the relay has sufficient CPU and network bandwidth

---

## Outbox Scaling

### Batch Size

Larger batches reduce round-trips but increase latency for individual messages:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'high-volume',
    'outbox', 'events',
    'sink_type', 'nats',
    'config', '{"url": "nats://localhost:4222", "subject": "events"}'::jsonb,
    'batch_size', 500
  )
);
```

### Multiple Outboxes

Split high-volume event streams across multiple outboxes for parallel relay consumption:

```sql
SELECT tide.outbox_create('orders-us', 24);
SELECT tide.outbox_create('orders-eu', 24);
```

Each outbox gets its own pipeline and relay ownership lock.

### Index Performance

The native relay uses an unconditional index on the shared table for logical-outbox polling:

```sql
-- Already created by the extension:
CREATE INDEX idx_tide_outbox_messages_poll
    ON tide.tide_outbox_messages (outbox_name, id);
```

The older partial `consumed_at` index remains for legacy/global cleanup surfaces.
At very high volumes (>10M rows), consider time-based partitioning.

---

## PostgreSQL Scaling

### Connection Pooling

The relay uses a single PostgreSQL connection. For applications with many connections, use PgBouncer or PgCat — pg_tide is fully compatible with connection poolers in transaction mode.

### Read Replicas

The relay must connect to the primary because it writes offsets. Monitoring queries
(`outbox_pending`, `consumer_lag`) can safely run against read replicas.

---

## Throughput Benchmarks

Typical performance on a standard cloud instance (4 vCPU, 16 GB RAM):

| Scenario | Throughput |
|----------|-----------|
| Outbox publish (single connection) | ~15,000 msg/s |
| Relay forward (NATS sink) | ~10,000 msg/s |
| Relay forward (Kafka sink) | ~8,000 msg/s |
| Relay reverse (NATS source → inbox) | ~5,000 msg/s |

These are conservative estimates. Actual performance depends on message size, PostgreSQL configuration, and network latency.
