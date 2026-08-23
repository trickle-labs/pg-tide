# Runbook: DLQ Replay

**Applies to:** pg-tide relay v0.13.0+  
**Scope:** How to drain a flooded dead-letter queue (DLQ), requeue messages
for retry, and monitor progress.

---

## What the DLQ Is

When a message cannot be delivered after exhausting retries (permanent error
or circuit breaker open), pg-tide writes it to `tide.relay_dlq`. DLQ insertion
is atomic for the complete failed batch: the source checkpoint advances only
after every row is durable or already present under its idempotency key.

```sql
-- Count DLQ entries per pipeline:
SELECT   pipeline_name, error_kind, COUNT(*) AS entries
FROM     tide.relay_dlq
GROUP BY pipeline_name, error_kind
ORDER BY entries DESC;
```

---

## Step 1 — Identify the Root Cause

Before requeuing, understand _why_ messages landed in the DLQ:

```sql
-- Inspect recent DLQ entries for a pipeline:
SELECT id, event_id, error_kind, error_message, failed_at
FROM   tide.relay_dlq
WHERE  pipeline_name = 'my-pipeline'
ORDER  BY failed_at DESC
LIMIT  20;
```

Common `error_kind` values:

| Kind | Meaning | Resolution |
|------|---------|------------|
| `permanent` | Sink rejected the message (invalid schema, auth, etc.) | Fix the root cause before requeuing |
| `transient` | Sink was unavailable; max retries exhausted | Confirm sink is healthy, then requeue |
| `dlq_write_failure` | Secondary DLQ write failure | Indicates DLQ table is misconfigured; check `tide.relay_dlq` permissions |

---

## Step 2 — Fix the Underlying Problem

Requeuing DLQ entries while the root cause is still present will return them
to the DLQ immediately.  Fix the sink, schema, or configuration first:

```bash
# Validate pipeline config against the live catalog and sink:
pg-tide config validate --pipeline my-pipeline --postgres-url "$PG_TIDE_POSTGRES_URL"

# Check that the relay can connect to all required services:
pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"
```

---

## Step 3 — Requeue Messages for Retry

Requeue is an explicit operator action. It does not implicitly rewind a live
relay offset. For a bounded replay, use the one-shot replay command with an
explicit range and monitor its completion.

### Via SQL

```sql
-- Requeue all DLQ entries for a pipeline (marks them as pending retry):
SELECT tide.dlq_requeue('my-pipeline');

-- Requeue a single entry by ID:
SELECT tide.dlq_requeue_entry(42);
```

### Via CLI

```bash
# Requeue all DLQ entries for a pipeline:
pg-tide replay dlq-requeue --pipeline my-pipeline --postgres-url "$PG_TIDE_POSTGRES_URL"

# Preview without actually requeuing (dry run):
pg-tide replay dlq-requeue --pipeline my-pipeline --dry-run --postgres-url "$PG_TIDE_POSTGRES_URL"
```

---

## Step 4 — Monitor Progress

Watch the DLQ depth decrease and message throughput increase:

```bash
# Tail the relay logs:
docker logs -f pg-tide 2>&1 | grep -E "(dlq|pipeline=my-pipeline)"

# Or watch Prometheus metrics:
curl -s http://localhost:9090/metrics | grep pg_tide_relay_dlq
```

Key metrics:

| Metric | Description |
|--------|-------------|
| `pg_tide_relay_dlq_entries_written_total` | Cumulative DLQ writes — should stop growing after root cause is fixed |
| `pg_tide_relay_messages_published_total` | Should increase as requeued messages are delivered |
| `pg_tide_relay_consumer_lag` | Should decrease as the pipeline catches up |
| `pg_tide_relay_delivery_stage_total{stage="dlq_persisted"}` | Confirms durable DLQ terminal dispositions |
| `pg_tide_relay_delivery_stage_total{stage="dlq_failed"}` | Alerts on retained source checkpoints |

If DLQ persistence succeeds but source checkpointing fails, leave the DLQ rows
in place and allow the relay to retry. The idempotency key makes the retry
safe; do not manually advance the offset.

---

## Step 5 — Purge Resolved DLQ Entries

After successful redelivery, clean up resolved entries:

```sql
-- Delete all successfully redelivered DLQ entries for a pipeline:
DELETE FROM tide.relay_dlq
WHERE  pipeline_name = 'my-pipeline'
  AND  requeued_at IS NOT NULL;

-- Or delete all entries older than 30 days:
DELETE FROM tide.relay_dlq
WHERE  failed_at < NOW() - INTERVAL '30 days';
```

---

## Flood Control

If the DLQ is growing very fast (more than ~100 entries/min):

1. **Disable the pipeline** to stop new DLQ writes:
   ```sql
   SELECT tide.relay_disable('my-pipeline');
   ```
2. Fix the root cause.
3. Clear the DLQ entries that will never be retryable.
4. Re-enable the pipeline and requeue surviving entries:
   ```sql
   SELECT tide.relay_enable('my-pipeline');
   SELECT tide.dlq_requeue('my-pipeline');
   ```

---

## See Also

- [Crash Recovery runbook](runbook-crash-recovery.md)
- [Schema Migration runbook](runbook-schema-migration.md)
- [`pg-tide replay` CLI reference](../relay-guide/cli-reference.md)
