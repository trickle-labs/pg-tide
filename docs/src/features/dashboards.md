# Feature: Grafana Dashboards

pg_tide ships with a pre-built Grafana dashboard that visualizes relay health, throughput, latency, and error rates. Import it into your Grafana instance for instant observability without manual panel creation.

## Importing the Dashboard

The dashboard JSON is located at `pg-tide/dashboards/relay-health.json` in the repository. Import it into Grafana:

1. Open Grafana → Dashboards → Import
2. Upload or paste the JSON from `pg-tide/dashboards/relay-health.json`
3. Select your Prometheus data source
4. Click Import

Or use the Grafana API:

```bash
curl -X POST http://admin:admin@grafana:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d @pg-tide/dashboards/relay-health.json
```

## Dashboard Panels

The relay health dashboard includes:

### Overview Row
- **Pipeline Status** — Table showing each pipeline's health status, last error, and uptime
- **Total Throughput** — Graph of messages/second across all pipelines
- **Active Pipelines** — Count of currently running pipelines

### Throughput Row
- **Messages Published (per pipeline)** — Rate of successful publishes
- **Messages Consumed (per pipeline)** — Rate of messages polled from source
- **Publish Errors (per pipeline)** — Rate of delivery failures

### Latency Row
- **Delivery Latency (p50/p95/p99)** — Histogram showing message transit time
- **Latency Heatmap** — Distribution of delivery times over time

### Health Row
- **Circuit Breaker State** — Timeline showing open/closed state per pipeline
- **Consumer Lag** — Current backlog per pipeline
- **DLQ Entries** — Count of unresolved dead letter queue entries

## Alerting Rules

Suggested Grafana alert rules to pair with the dashboard:

### High Error Rate
```yaml
alert: PgTideHighErrorRate
expr: rate(pg_tide_publish_errors_total[5m]) > 0.1
for: 5m
labels:
  severity: warning
annotations:
  summary: "pg_tide pipeline {{ $labels.pipeline }} has elevated errors"
```

### Circuit Breaker Open
```yaml
alert: PgTideCircuitOpen
expr: pg_tide_pipeline_healthy == 0
for: 1m
labels:
  severity: critical
annotations:
  summary: "pg_tide pipeline {{ $labels.pipeline }} circuit breaker is open"
```

### High Consumer Lag
```yaml
alert: PgTideHighLag
expr: pg_tide_consumer_lag > 10000
for: 10m
labels:
  severity: warning
annotations:
  summary: "pg_tide pipeline {{ $labels.pipeline }} has {{ $value }} pending messages"
```

## Customization

The dashboard uses standard Prometheus queries. Customize it by:
- Adding panels for specific pipelines
- Adjusting time ranges and refresh intervals
- Adding annotations for deployment events
- Linking to your tracing backend (Tempo, Jaeger) for drill-down

## Further Reading

- [Metrics](metrics.md) — Available Prometheus metrics
- [Prometheus + Grafana Integration](../integration/prometheus-grafana.md) — Full stack setup
- [Monitoring Guide](../relay-guide/monitoring.md) — Observability best practices
