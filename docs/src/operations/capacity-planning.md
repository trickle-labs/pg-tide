# Capacity Planning

This guide helps you estimate the resources needed for pg_tide based on your workload characteristics.

## Key Dimensions

Capacity planning for pg_tide involves three systems: PostgreSQL (where outbox/inbox tables live), the relay process (CPU and memory), and the network (bandwidth to sinks).

### PostgreSQL

The outbox table is the primary bottleneck for most deployments. Key factors:

| Factor | Impact | Mitigation |
|--------|--------|------------|
| Write throughput | INSERT rate into outbox tables | Connection pooling, partitioning |
| Table size | Unrelayed rows waiting for delivery | Tune batch size and poll interval |
| Index maintenance | Outbox has sequential ID index | Minimal — append-only workload |
| Disk I/O | WAL writes for each INSERT | Fast storage, WAL tuning |

**Rule of thumb:** A single PostgreSQL instance handles 10,000-50,000 outbox inserts/second depending on row size and hardware. The relay processes rows faster than most applications can generate them.

### Relay Process

The relay is CPU-light and memory-light for most workloads:

| Workload | CPU | Memory | Bottleneck |
|----------|-----|--------|------------|
| 1,000 msg/s, JSON, Kafka | 0.1 core | 50 MB | Network to Kafka |
| 10,000 msg/s, JSON, Kafka | 0.5 core | 100 MB | Kafka ack latency |
| 10,000 msg/s, Avro, Schema Registry | 1 core | 200 MB | Avro serialization |
| 50,000 msg/s, JSON, NATS | 0.3 core | 80 MB | Network |
| 1,000 msg/s, HTTP webhook | 0.1 core | 50 MB | Webhook response time |

**Rule of thumb:** Start with 0.5 CPU and 256 MB memory. Monitor actual usage and adjust.

### Network

Bandwidth depends on message size and throughput:

```
Bandwidth = messages_per_second × average_message_size_bytes
```

Example: 10,000 msg/s × 1 KB/msg = 10 MB/s = 80 Mbps

## Sizing Formulas

### Outbox Table Growth

If the relay is down (or slower than production), the outbox grows:

```
Rows pending = (insert_rate - relay_rate) × downtime_seconds
Disk usage = rows_pending × average_row_size
```

Example: 5,000 inserts/s with relay down for 5 minutes:
- Rows: 5,000 × 300 = 1,500,000
- Disk: 1,500,000 × 500 bytes = 750 MB

### Consumer Lag Recovery Time

After an outage, how long to drain the backlog:

```
Recovery time = pending_rows / (relay_rate - insert_rate)
```

Example: 1.5M pending rows, relay at 20,000/s, inserts at 5,000/s:
- Recovery: 1,500,000 / 15,000 = 100 seconds

### Relay Instance Count

For active-active HA with balanced load:

```
Instances = ceil(total_pipelines / pipelines_per_instance)
```

Most pipelines consume negligible resources. Start with 2 instances (for HA) and scale based on actual throughput needs.

## Batch Size Tuning

Batch size affects both throughput and latency:

| Batch Size | Throughput | Latency | Use Case |
|-----------|-----------|---------|----------|
| 1 | Lowest | Lowest | Real-time notifications |
| 10-50 | Medium | Low | General event streaming |
| 100-500 | High | Medium | Analytics, data lake loading |
| 1000+ | Highest | Higher | Bulk ETL, backfill |

Configure per-pipeline:
```json
{ "batch_size": 100 }
```

Or set a process-wide default:
```bash
pg-tide --default-batch-size 100
```

## PostgreSQL Configuration

Key PostgreSQL settings for outbox-heavy workloads:

```sql
-- Connection handling
max_connections = 200          -- Enough for app + relay + monitoring
shared_buffers = '4GB'         -- 25% of RAM

-- WAL configuration (important for high-insert workloads)
wal_buffers = '64MB'
max_wal_size = '4GB'
checkpoint_completion_target = 0.9

-- Vacuuming (outbox rows are deleted after relay)
autovacuum_vacuum_scale_factor = 0.01  -- Vacuum more aggressively
autovacuum_naptime = '10s'
```

## Monitoring for Capacity

Set alerts on these metrics to detect capacity issues early:

| Metric | Warning Threshold | Action |
|--------|-------------------|--------|
| `pg_tide_consumer_lag` | > 10,000 | Increase batch size or add relay instances |
| CPU usage (relay) | > 70% sustained | Add CPU or split pipelines |
| PostgreSQL connections | > 80% of max | Increase max_connections or use pgBouncer |
| Disk usage growth | > 1 GB/hour unrelayed | Investigate relay health |

## Further Reading

- [Scaling](scaling.md) — Strategies for increasing throughput
- [Deployment Architectures](deployment-architectures.md) — Choosing your topology
- [Monitoring](../relay-guide/monitoring.md) — Setting up observability
