# Monitoring Cookbook

Practical recipes for monitoring pg_tide in production. Each recipe addresses a specific operational concern with ready-to-use PromQL queries, alert rules, and dashboard configurations.

## Recipe: Basic Health Monitoring

**Goal:** Know immediately when something is wrong.

### Alerts

```yaml
groups:
  - name: pg-tide-health
    rules:
      - alert: PgTidePipelineUnhealthy
        expr: pg_tide_pipeline_healthy == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} is unhealthy (circuit breaker open)"

      - alert: PgTideNoActivity
        expr: rate(pg_tide_messages_published_total[10m]) == 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} has published zero messages for 15 minutes"
```

### Dashboard Panel
```promql
# Traffic light: 1 = green, 0 = red
pg_tide_pipeline_healthy
```

## Recipe: Throughput Monitoring

**Goal:** Understand message flow rates and detect anomalies.

### Key Queries

```promql
# Messages published per second (per pipeline)
rate(pg_tide_messages_published_total[5m])

# Total throughput across all pipelines
sum(rate(pg_tide_messages_published_total[5m]))

# Publish success ratio
1 - (rate(pg_tide_publish_errors_total[5m]) / rate(pg_tide_messages_consumed_total[5m]))
```

### Alert: Throughput Drop

```yaml
- alert: PgTideThroughputDrop
  expr: |
    rate(pg_tide_messages_published_total[5m]) 
    < 0.5 * rate(pg_tide_messages_published_total[1h] offset 1d)
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} throughput dropped >50% vs yesterday"
```

## Recipe: Latency Monitoring

**Goal:** Ensure messages are delivered within acceptable time bounds.

### Key Queries

```promql
# P50 delivery latency
histogram_quantile(0.5, rate(pg_tide_delivery_latency_seconds_bucket[5m]))

# P99 delivery latency
histogram_quantile(0.99, rate(pg_tide_delivery_latency_seconds_bucket[5m]))

# Percentage of messages delivered within 1 second
sum(rate(pg_tide_delivery_latency_seconds_bucket{le="1.0"}[5m]))
/ sum(rate(pg_tide_delivery_latency_seconds_count[5m]))
```

### Alert: High Latency

```yaml
- alert: PgTideHighLatency
  expr: histogram_quantile(0.99, rate(pg_tide_delivery_latency_seconds_bucket[5m])) > 5
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} P99 latency exceeds 5 seconds"
```

## Recipe: Consumer Lag Monitoring

**Goal:** Detect growing backlogs before they become critical.

### Key Queries

```promql
# Current lag (pending messages)
pg_tide_consumer_lag

# Lag growth rate (positive = growing, negative = draining)
deriv(pg_tide_consumer_lag[5m])

# Estimated time to drain at current rate
pg_tide_consumer_lag / rate(pg_tide_messages_published_total[5m])
```

### Alert: Growing Lag

```yaml
- alert: PgTideGrowingLag
  expr: pg_tide_consumer_lag > 10000
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} has {{ $value }} pending messages"

- alert: PgTideCriticalLag
  expr: pg_tide_consumer_lag > 100000
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} has critical backlog: {{ $value }} messages"
```

## Recipe: Error Rate Monitoring

**Goal:** Detect delivery problems early.

### Key Queries

```promql
# Errors per second
rate(pg_tide_publish_errors_total[5m])

# Error ratio (errors / total consumed)
rate(pg_tide_publish_errors_total[5m]) / rate(pg_tide_messages_consumed_total[5m])
```

### Alert: Error Spike

```yaml
- alert: PgTideErrorSpike
  expr: rate(pg_tide_publish_errors_total[5m]) > 1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} has sustained errors: {{ $value }}/s"
```

## Recipe: Dead Letter Queue Monitoring

**Goal:** Track messages that failed permanently and need attention.

### SQL Query (for custom exporter or pg_stat_monitor)

```sql
-- Unresolved DLQ entries by pipeline
SELECT pipeline_name, count(*) as unresolved
FROM tide.relay_dlq
WHERE resolved_at IS NULL
GROUP BY pipeline_name;
```

### Alert (via SQL-based exporter)

```yaml
- alert: PgTideDLQGrowing
  expr: pg_tide_dlq_unresolved > 0
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "Pipeline {{ $labels.pipeline }} has {{ $value }} unresolved DLQ entries"
```

## Recipe: Resource Monitoring

**Goal:** Ensure relay processes have adequate resources.

### Key Queries (standard node/container metrics)

```promql
# CPU usage per relay pod
rate(container_cpu_usage_seconds_total{container="pg-tide"}[5m])

# Memory usage per relay pod
container_memory_working_set_bytes{container="pg-tide"}

# PostgreSQL active connections from relay
pg_stat_activity_count{application_name="pg-tide"}
```

## Runbook Reference

| Alert | First Response | Escalation |
|-------|---------------|------------|
| PipelineUnhealthy | Check sink availability, review error logs | Restart relay if stuck |
| ThroughputDrop | Check source (outbox empty?), check sink (slow?) | Scale relay instances |
| HighLatency | Check batch size, check sink response time | Increase batch size or add instances |
| GrowingLag | Check relay health, check for slow transforms | Increase batch size, add instances |
| ErrorSpike | Check DLQ for error details, check sink logs | Fix root cause, replay DLQ |
| DLQGrowing | Inspect DLQ entries, identify error pattern | Fix issue, replay messages |

## Further Reading

- [Metrics](../features/metrics.md) — Full metrics reference
- [Dashboards](../features/dashboards.md) — Pre-built Grafana dashboard
- [Troubleshooting](troubleshooting.md) — Diagnosing common issues

---

## Inbox Fleet Status Performance Contract (v0.33.0)

`tide.inbox_status(NULL)` (fleet mode) returns a summary row for every
configured inbox.  Internally it executes a dynamic `UNION ALL` query across
all inbox message tables — **one round-trip per call, but the query grows with
the number of inboxes** (O(n) SQL length, not O(n) round-trips since v0.32.0).

**Guidelines:**

| Use case | Recommendation |
|---|---|
| Grafana dashboard panel | ✅ OK — set panel refresh to ≥ 60 s |
| Monitoring script (cron / runbook) | ✅ OK — run on demand |
| `pg-tide status --inbox-summary` CLI | ✅ OK — run ad-hoc |
| Per-message application loop | ❌ Use `tide.inbox_status('inbox_name')` per-inbox instead |
| High-frequency polling (< 5 s) | ❌ Use Prometheus metrics or per-inbox queries |

The per-inbox variant `tide.inbox_status('my_inbox')` is always O(1) and safe
to call at any frequency.

**CLI usage:**

```bash
# Include inbox fleet summary in pg-tide status output:
pg-tide status --postgres-url "$PG_URL" --inbox-summary
```
