# Monitoring

The pg-tide relay exposes Prometheus metrics and a health endpoint for observability.

---

## Endpoints

| Endpoint | Port | Description |
|----------|------|-------------|
| `GET /metrics` | 9090 (default) | Prometheus metrics |
| `GET /health` | 9090 (default) | Liveness + readiness check |

Configure the port with `--metrics-addr`:

```bash
pg-tide --metrics-addr 0.0.0.0:9090
```

---

## Prometheus Metrics

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `pg_tide_relay_messages_published_total` | `pipeline`, `direction` | Messages successfully delivered to sink |
| `pg_tide_relay_messages_consumed_total` | `pipeline`, `direction` | Messages read from source |
| `pg_tide_relay_publish_errors_total` | `pipeline`, `direction` | Failed delivery attempts |
| `pg_tide_relay_dedup_skipped_total` | `pipeline` | Messages skipped (duplicate dedup key) |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `pg_tide_relay_pipeline_healthy` | `pipeline` | 1 if pipeline is operational, 0 otherwise |

---

## Health Endpoint

```bash
curl http://localhost:9090/health
```

- **200 OK** — all pipelines healthy
- **503 Service Unavailable** — one or more pipelines unhealthy

Response body includes unhealthy pipeline names when degraded.

---

## SQL-Level Monitoring

In addition to relay metrics, monitor from PostgreSQL:

```sql
-- Pending messages per outbox
SELECT * FROM tide.outbox_pending;

-- Consumer lag
SELECT * FROM tide.consumer_lag;

-- Pipeline status
SELECT tide.relay_list_configs();
```

---

## Grafana Dashboard

Example PromQL queries for a Grafana dashboard:

```promql
# Message throughput (published/sec)
rate(pg_tide_relay_messages_published_total[5m])

# Error rate
rate(pg_tide_relay_publish_errors_total[5m])

# Consumer lag (from PostgreSQL — use a Postgres exporter)
pg_tide_consumer_lag{group_name="my-relay"}

# Pipeline health
pg_tide_relay_pipeline_healthy
```

---

## Alerting Recommendations

| Condition | Severity | Alert |
|-----------|----------|-------|
| `pg_tide_relay_pipeline_healthy == 0` | Critical | Pipeline down |
| `rate(publish_errors_total[5m]) > 0` | Warning | Delivery errors |
| Consumer lag > threshold | Warning | Relay falling behind |
| No heartbeat for > 60s | Critical | Relay process dead |
