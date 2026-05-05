# Feature: Prometheus Metrics

pg_tide exposes Prometheus-format metrics via an HTTP endpoint, giving you real-time visibility into pipeline throughput, error rates, latency, and health. These metrics integrate with Grafana, Datadog, or any Prometheus-compatible monitoring stack.

## Metrics Endpoint

The relay starts an HTTP server on port 9090 by default:

```
GET http://localhost:9090/metrics    → Prometheus text format
GET http://localhost:9090/health     → Health check (200 or 503)
```

Configure the listen address:

```bash
pg-tide --metrics-addr "0.0.0.0:9090"
```

## Available Metrics

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `pg_tide_messages_published_total` | pipeline, direction | Total messages successfully published to sink |
| `pg_tide_messages_consumed_total` | pipeline, direction | Total messages consumed from source |
| `pg_tide_publish_errors_total` | pipeline, direction | Total publish failures |
| `pg_tide_dedup_skipped_total` | pipeline | Messages skipped due to deduplication |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `pg_tide_pipeline_healthy` | pipeline | 1 = healthy, 0 = circuit breaker open |
| `pg_tide_consumer_lag` | pipeline | Pending messages in outbox (estimated) |

### Histograms

| Metric | Labels | Buckets (seconds) | Description |
|--------|--------|-------------------|-------------|
| `pg_tide_delivery_latency_seconds` | pipeline | 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0 | Time from outbox insert to sink acknowledgment |

## Labels

All metrics are labeled by:
- **`pipeline`** — Pipeline name (e.g., `"orders-to-kafka"`)
- **`direction`** — `"forward"` (outbox → sink) or `"reverse"` (source → inbox)

## Health Endpoint

The `/health` endpoint returns:
- **200 OK** with body `"healthy"` — All pipelines have closed circuit breakers
- **503 Service Unavailable** with body `"unhealthy: [pipeline-a, pipeline-b]"` — One or more pipelines have open circuit breakers

Use this for Kubernetes liveness/readiness probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Prometheus Scrape Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'pg-tide'
    static_targets:
      - targets: ['pg-tide:9090']
    scrape_interval: 15s
```

For Kubernetes with pod annotations:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "9090"
    prometheus.io/path: "/metrics"
```

## Key Queries

### Throughput (messages/second)
```promql
rate(pg_tide_messages_published_total[5m])
```

### Error rate
```promql
rate(pg_tide_publish_errors_total[5m])
```

### Delivery latency (p99)
```promql
histogram_quantile(0.99, rate(pg_tide_delivery_latency_seconds_bucket[5m]))
```

### Consumer lag
```promql
pg_tide_consumer_lag
```

### Unhealthy pipelines
```promql
pg_tide_pipeline_healthy == 0
```

## Further Reading

- [Dashboards](dashboards.md) — Pre-built Grafana dashboards
- [OpenTelemetry](opentelemetry.md) — Distributed tracing (complementary)
- [Monitoring Guide](../relay-guide/monitoring.md) — Complete observability setup
