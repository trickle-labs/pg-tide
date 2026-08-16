# Scaling

Scale from the measured reference profiles in
[`benchmarks/operational/`](../../../../benchmarks/operational/README.md).
Payload size, sink acknowledgments, PostgreSQL settings, batch size, and
pipeline count determine capacity; the project does not promise a universal
throughput number.

## Relay instances

Run multiple relays with the same `relay_group_id` for ownership transfer and
HA. Each pipeline has one owner at a time. More instances improve failover and
pipeline density, not the throughput of one pipeline. Measure the
`pipeline-density` profile at 1, 10, and 50 pipelines and compare idle RSS,
catalog discovery queries, CPU, and offset writes.

## One busy pipeline

Increase `batch_size` and sink in-flight concurrency only after measuring
end-to-end p99 latency and PostgreSQL lock/WAL cost. Keep the poll query
canonical:

```sql
SELECT id, outbox_name, payload, headers, created_at
FROM tide.tide_outbox_messages
WHERE outbox_name = $1 AND id > $2
ORDER BY id
LIMIT $3;
```

The `(outbox_name, id)` index is required on the shared parent and every
partition child. Verify pruning with `EXPLAIN (FORMAT JSON)`.

## Outage and disk planning

Committed rows accumulate while a sink is unavailable. Reserve:

```text
outage disk = application_rate × outage_seconds × measured_bytes_per_retained_row
```

Relay backoff/rate limiting protects a recovering sink but does not reject
application transactions. Use exact lag alerts and application admission
control if an outage window must be hard bounded. `inline_threshold` does not
provide a native pending-row cap.

## Outbox storage

Heap storage is the default. Choose ID-range partitioning when measured
retention, vacuum, index growth, or bounded cleanup requires it. It keeps the
single public parent and partitions by global message ID; it does not create a
table per outbox. Use the configured span and premake count, and monitor the
default partition and child count.

## Read replicas and pooling

The relay connects to the primary because it reads and writes checkpoints.
Monitoring queries may use a read replica when its lag is acceptable. PgBouncer
is suitable for short metadata transactions; ownership sessions must remain
dedicated and session-persistent.
