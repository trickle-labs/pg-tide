# Troubleshooting Guide

A comprehensive guide to diagnosing and resolving common pg_tide issues in production.

## Quick Diagnostic Checklist

When something isn't working:

1. **Check relay logs** — Look for ERROR or WARN messages
2. **Check `/health` endpoint** — Returns 503 if circuit breaker is open
3. **Check consumer lag** — `pg_tide_consumer_lag` metric or query outbox directly
4. **Check DLQ** — `SELECT count(*) FROM tide.relay_dlq WHERE resolved_at IS NULL`
5. **Check advisory locks** — `SELECT * FROM pg_locks WHERE locktype = 'advisory'`

## Messages Not Being Delivered

### Symptom: Outbox rows accumulate, nothing reaches the sink

**Check 1: Is the relay running?**
```bash
# Check if the process is alive
ps aux | grep pg-tide

# Check Kubernetes
kubectl get pods -l app=pg-tide-relay
```

**Check 2: Is the pipeline enabled?**
```sql
SELECT name, enabled FROM tide.relay_outbox_config;
```

**Check 3: Does the relay hold the advisory lock?**
```sql
SELECT pid, objid 
FROM pg_locks 
WHERE locktype = 'advisory' AND granted = true;
```

If no locks are held, the relay may have lost its database connection.

**Check 4: Is the circuit breaker open?**
```bash
curl http://localhost:9090/health
# If unhealthy, circuit breaker is open for one or more pipelines
```

**Check 5: Is the sink reachable?**
Test connectivity from the relay host to the sink (Kafka broker, HTTP endpoint, etc.).

### Symptom: Messages delivered but arriving slowly

**Check 1: Batch size too small?**
Small batch sizes (1-10) increase per-message overhead. Try increasing to 100+.

**Check 2: Rate limiter configured?**
```sql
SELECT config->'rate_limit' FROM tide.relay_outbox_config WHERE name = 'your-pipeline';
```

**Check 3: Sink response time?**
Check `pg_tide_delivery_latency_seconds` — if high, the sink is slow to acknowledge.

## Circuit Breaker Stuck Open

### Symptom: Pipeline unhealthy, `/health` returns 503

The circuit breaker opens after `failure_threshold` consecutive failures. It won't close until probe requests succeed.

**Resolution:**
1. Check sink availability (is the Kafka broker up? Is the webhook endpoint responding?)
2. Check relay logs for the specific error message
3. Fix the underlying issue
4. Wait for `half_open_timeout` — the circuit will probe automatically
5. If the probe succeeds, the circuit closes and normal flow resumes

**Force-close (nuclear option):** Restart the relay. Circuit breaker state is in-memory and resets on restart.

## Duplicate Messages at Sink

### Symptom: Consumers see the same message multiple times

pg_tide guarantees at-least-once delivery. Duplicates can occur when:

1. The relay publishes to the sink but crashes before acknowledging
2. Network partition causes timeout after successful publish
3. Replay mode reprocesses an already-delivered range

**Resolution:**
- Implement idempotent consumers (dedup on `outbox_id` or `dedup_key`)
- Use the [inbox](../sql-reference/inbox-api.md) on the receiving side for built-in deduplication
- For Kafka, use `enable.idempotence=true` on the consumer side

## DLQ Entries Accumulating

### Symptom: `tide.relay_dlq` table growing

**Diagnose the error pattern:**
```sql
SELECT error_kind, error_message, count(*)
FROM tide.relay_dlq
WHERE resolved_at IS NULL
GROUP BY error_kind, error_message
ORDER BY count(*) DESC
LIMIT 10;
```

**Common causes:**

| Error Kind | Typical Cause | Fix |
|-----------|---------------|-----|
| `decode` | Malformed message in outbox | Fix the producing application |
| `sink_permanent` | Auth failure, schema mismatch | Update credentials or schema |
| `inbox_permanent` | Constraint violation | Check inbox table constraints |
| `max_retries_exceeded` | Transient issue that lasted too long | Fix sink, then replay |

**After fixing:** Replay the DLQ entries:
```sql
SELECT tide.relay_dlq_retry_all('your-pipeline');
```

## Connection Issues

### Symptom: "connection refused" or "timeout" in logs

**PostgreSQL connection:**
```bash
# Test from relay host
psql "${DATABASE_URL}" -c "SELECT 1"
```

Common issues:
- Connection string wrong (check `--postgres-url`)
- PostgreSQL max_connections reached
- Firewall rules blocking relay → database
- SSL/TLS certificate issues

**Sink connection:**
- Check DNS resolution from relay host
- Check firewall/security group rules
- Verify credentials haven't expired
- Check TLS certificate validity

### Symptom: "too many connections" from PostgreSQL

Each pipeline worker uses one connection. With 50 pipelines, you need 50+ connections.

**Resolution:**
- Increase `max_connections` in PostgreSQL
- Use PgBouncer in front of PostgreSQL
- Reduce number of pipelines per relay instance

## Transform/Filter Issues

### Symptom: Messages being silently dropped

If messages are consumed but never published, a transform filter might be dropping them.

**Diagnose:**
1. Check if `messages_consumed > messages_published` consistently
2. Check transform configuration:
```sql
SELECT config->'transform' FROM tide.relay_outbox_config WHERE name = 'your-pipeline';
```
3. Test the filter expression against a sample payload
4. Temporarily remove the filter to confirm

### Symptom: Transform producing unexpected output

Set `dry_run: true` on the pipeline to see what transforms produce without publishing:
```sql
UPDATE tide.relay_outbox_config 
SET config = config || '{"dry_run": true}'::jsonb 
WHERE name = 'your-pipeline';
```

Check relay logs for the dry-run output, then disable dry-run when satisfied.

## Advisory Lock Conflicts

### Symptom: Pipeline not being picked up by any relay instance

**Check lock ownership:**
```sql
SELECT l.pid, a.application_name, l.objid
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.locktype = 'advisory';
```

If a stale connection holds the lock (zombie process), terminate it:
```sql
SELECT pg_terminate_backend(<pid>);
```

## Performance Degradation

### Symptom: Gradual slowdown over time

**Check 1: Table bloat**
```sql
SELECT relname, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables
WHERE relname LIKE 'outbox%' OR relname LIKE 'inbox%';
```

If `n_dead_tup` is high, autovacuum may be falling behind. See [Maintenance](maintenance.md).

**Check 2: Index bloat**
```sql
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid))
FROM pg_stat_user_indexes
WHERE relname LIKE 'outbox%';
```

**Check 3: Disk pressure**
High I/O wait indicates the database can't keep up with the workload.

## Further Reading

- [Monitoring Cookbook](monitoring-cookbook.md) — Alert rules and queries
- [Maintenance](maintenance.md) — Preventive maintenance tasks
- [Dead Letter Queue](../features/dead-letter-queue.md) — DLQ management
