# Runbook: Crash Recovery

**Applies to:** pg-tide relay binary (`pg-tide run`)  
**Scope:** What happens when the relay crashes mid-batch and how to recover.

---

## At-Least-Once Guarantee

pg-tide implements an **at-least-once** delivery guarantee.  When the relay
crashes between polling messages from the outbox and acknowledging delivery to
the sink, the un-acknowledged messages will be re-delivered after restart
because the consumer offset has not advanced.

**No manual intervention is required in the normal case.** Restart the relay
and it resumes from the last committed offset. A crash after sink success but
before that commit may produce a duplicate; stable event identity and
destination deduplication determine the outcome.

---

## How the Relay Commits Offsets

The relay tracks its position in the outbox via `tide.relay_consumer_offsets`.
After a complete batch reaches a durable terminal disposition, it advances the
`last_change_id` for the pipeline. If the relay crashes before this write, the
batch is re-read and re-delivered.

```sql
-- Inspect the current offset for each pipeline:
SELECT relay_group_id, pipeline_id, outbox_name, last_change_id, updated_at
FROM   tide.relay_consumer_offsets
ORDER  BY relay_group_id, pipeline_id, outbox_name;
```

---

## Identifying a Stuck Pipeline

A stuck pipeline is one where exact pipeline lag is not decreasing despite the
relay running.  Signs include:

- `pg_tide_relay_pipeline_lag{pipeline="..."}` is high and flat in Grafana.
- `pg-tide status` shows the pipeline as owned but not progressing.
- Repeated error log entries for the same pipeline.

### Common Causes

| Cause | Resolution |
|-------|------------|
| Sink is unreachable | Fix the sink endpoint; pipeline auto-resumes |
| Circuit breaker is open | Wait for half-open probe, or restart the worker |
| DLQ is full / INSERT denied | See [DLQ Replay runbook](runbook-dlq-replay.md) |
| Advisory lock held by crashed pod | See below |

### Clearing a Stale Advisory Lock

PostgreSQL advisory locks are **session-scoped** — they are automatically
released when the backend connection closes (e.g. on relay crash or pod
eviction).  In practice, stale locks resolve themselves within seconds.

If you believe a lock is genuinely stuck (e.g. the PostgreSQL backend is
still connected after a relay pod was forcibly killed), identify and
terminate it:

```sql
-- Find the backend holding the lock for a pipeline named "my-pipeline":
SELECT pid, application_name, state, query_start
FROM   pg_stat_activity
WHERE  pid IN (
    SELECT pid
    FROM   pg_locks
    WHERE  locktype = 'advisory'
      AND  classid = hashtext('default')        -- relay_group_id
      AND  objid   = hashtext('my-pipeline')    -- pipeline name
);

-- Terminate the backend (replaces the lock with nothing; the relay
-- will reacquire on next reconcile):
SELECT pg_terminate_backend(<pid>);
```

---

## Restart Procedure

1. Verify the relay process has fully stopped (no ghost connections).
2. Restart the relay: `docker restart pg-tide` or `kubectl rollout restart deployment/pg-tide`.
3. Watch logs for ownership acquisition and checkpoint transitions confirming pipelines resume.
4. Check `pg-tide status --postgres-url $PG_TIDE_POSTGRES_URL` and
   `tide.relay_pipeline_lag` for lag convergence.

---

## After a Partial Batch

Duplicate delivery to the sink is possible after a crash.  Ensure your
sink consumers are **idempotent**:

- Use the stable `event_id`/`dedup_key` field as an idempotency key.
- For inbox targets, pg-tide automatically deduplicates via the `event_id`
  primary key with `ON CONFLICT DO NOTHING`.

---

## See Also

- [DLQ Replay runbook](runbook-dlq-replay.md)
- [Schema Migration runbook](runbook-schema-migration.md)
- [Relay Upgrade runbook](runbook-relay-upgrade.md)
