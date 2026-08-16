# Capacity planning

pg_tide does not publish a universal messages-per-second number. Capacity is
the result of payload size, PostgreSQL settings, batch size, pipeline count,
sink acknowledgment behavior, and hardware. The v0.43 reference profiles and
reviewed budgets live in
[`benchmarks/operational/`](../../../../benchmarks/operational/README.md).
The committed baseline is a schema slot until a named reference runner has
recorded measurements.

## Profiles and evidence

The required profiles are:

| Profile | Payload | Pipelines | Measures |
|---|---:|---:|---|
| `publish-single` | 1 KiB JSON | 1 | matched transaction overhead and WAL |
| `publish-concurrent` | 1 KiB JSON | 1 | p50/p95/p99 publish overhead |
| `relay-core` | 1 KiB JSON | 1 | throughput, latency, CPU, RSS, offsets |
| `relay-large` | 16/64 KiB JSON | 1 | payload scaling and claim-check guidance |
| `pipeline-density` | 1 KiB JSON | 1/10/50 | idle and active worker cost |
| `outage-recovery` | 1 KiB JSON | 1 | backlog growth and drain slope |
| `retention` | 1 KiB JSON | 2 consumers | safe watermark, sweep, WAL, vacuum |
| `ha-interruption` | 1 KiB JSON | 1, 2 relays | owner-loss delivery interruption |

Every result records the commit, environment, PostgreSQL/NATS versions,
payload, batch, poll interval, pipeline count, warmup, duration, and exact
published/acknowledged/checkpointed identities. Never substitute the Criterion
or direct-insert microbenchmarks for these profiles.

## Sizing formulas

Let `r` be application messages/second, `s` the measured total bytes per
retained message (heap plus indexes), and `w` the outage window in seconds:

```text
outage disk reservation = r × w × s
steady retained storage = r × retention_seconds × s
network bytes/second = r × measured encoded payload bytes
recovery seconds = backlog / (acknowledged_rate - application_rate)
```

Use the measured values from the matching profile, not values from a different
payload or sink. Reserve additional space for WAL, vacuum headroom, backups,
and conversion's temporary copy. A sink outage accumulates committed rows;
relay backoff protects the sink but does not reject application transactions.

## Message sizes and claim-check

The reference matrix tests 1, 16, and 64 KiB JSON. Measure larger payloads
before adopting them. Native large-object claim-check can reduce row and index
growth for large payloads, but the large object is retained until the outbox
row is safely cleaned; it is not an independent retention policy. Verify
cleanup permissions and large-object storage in the same outage reservation.

## PostgreSQL cost and vacuum

Inspect the measured relation and WAL deltas with:

```sql
SELECT pg_size_pretty(pg_relation_size('tide.tide_outbox_messages')),
       pg_size_pretty(pg_indexes_size('tide.tide_outbox_messages')),
       pg_size_pretty(pg_total_relation_size('tide.tide_outbox_messages'));

SELECT wal_bytes, stats_reset
FROM pg_stat_wal;

SELECT relname, n_live_tup, n_dead_tup, last_autovacuum,
       autovacuum_count, vacuum_count
FROM pg_stat_user_tables
WHERE schemaname = 'tide';
```

Use per-table autovacuum storage parameters for high-churn installations,
derived from the retention profile rather than copied as universal settings:

```sql
ALTER TABLE tide.tide_outbox_messages SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_vacuum_cost_limit = 2000
);
```

Bounded sweeps are preferred to one large delete: they cap lock duration and
WAL per transaction and give autovacuum regular work. Check
`n_dead_tup / greatest(n_live_tup, 1)`, relation growth after vacuum, and
`pg_stat_statements` query counts. If dead tuples or total bytes grow after
delivery and cleanup have stabilized, vacuum is behind or the retention
contract is blocked.

## Partitioning

Heap storage is sufficient when measured retained rows, vacuum, and index
growth fit the disk reservation and query-plan budgets. Choose ID-range
partitioning when retained ID ranges make maintenance or vacuum too expensive,
not because a generic throughput threshold says so. The shared parent remains
`tide.tide_outbox_messages`; children are numeric `RANGE (id)` partitions.
Choose the span from measured retained IDs and maintenance duration, keeping
the child count bounded by retained ranges plus the premade and default
children. The default span is 10,000,000 IDs and premake count is two.

Verify the canonical query and pruning with:

```sql
EXPLAIN (FORMAT JSON)
SELECT id, outbox_name, payload
FROM tide.tide_outbox_messages
WHERE outbox_name = 'orders' AND id > 1000
ORDER BY id
LIMIT 100;
```

Conversion is a blocking maintenance-window copy. Run its dry-run preflight,
reserve temporary disk, drain relays, and keep a rollback plan. The old
per-outbox/time-partition procedure is not supported.

## Pipeline density and HA

Ten or fifty pipelines are separate measured profiles, not a linear capacity
promise. Compare idle worker RSS, catalog discovery queries, active throughput,
and offset writes against the budget. Run at least two relays for HA and size
the interruption budget from the measured owner-loss-to-resumed-delivery
interval.

## Alerts

Alert on:

- exact lag from `tide.relay_pipeline_lag`;
- blocked participants and safe offset from `tide.outbox_retention_status`;
- default-partition rows and storage-layout mismatch;
- WAL rate, dead-tuple ratio, and relation growth;
- disk remaining after the configured outage reservation.

Do not use a fixed lag or throughput threshold copied from another environment.
