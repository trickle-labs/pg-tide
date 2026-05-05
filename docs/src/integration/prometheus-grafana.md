# Integration: Prometheus + Grafana

This guide covers setting up complete observability for pg_tide using Prometheus for metrics collection and Grafana for visualization and alerting.

## Architecture

```
pg-tide relay (:9090/metrics)  →  Prometheus  →  Grafana
                                      ↓
                               Alertmanager → PagerDuty/Slack
```

## Prometheus Configuration

### Static Target

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'pg-tide'
    static_configs:
      - targets: ['pg-tide-relay:9090']
        labels:
          environment: 'production'
```

### Kubernetes Service Discovery

```yaml
scrape_configs:
  - job_name: 'pg-tide'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        target_label: __address__
        regex: (.+)
        replacement: ${1}:$1
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: pg-tide-relay
```

### Prometheus Operator (ServiceMonitor)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: pg-tide-relay
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: pg-tide-relay
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

## Grafana Dashboard

Import the pre-built dashboard from `pg-tide/dashboards/relay-health.json`:

1. Grafana → Dashboards → Import
2. Upload `relay-health.json`
3. Select your Prometheus data source

Or provision automatically:

```yaml
# grafana/provisioning/dashboards/pg-tide.yaml
apiVersion: 1
providers:
  - name: 'pg-tide'
    folder: 'Infrastructure'
    type: file
    options:
      path: /var/lib/grafana/dashboards/pg-tide
```

## Alert Rules

### Prometheus Alert Rules

```yaml
# prometheus/rules/pg-tide.yaml
groups:
  - name: pg-tide
    rules:
      - alert: PgTidePipelineDown
        expr: pg_tide_pipeline_healthy == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} circuit breaker is open"
          runbook_url: "https://wiki.example.com/pg-tide/circuit-breaker"

      - alert: PgTideHighErrorRate
        expr: rate(pg_tide_publish_errors_total[5m]) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} error rate: {{ $value }}/s"

      - alert: PgTideHighLag
        expr: pg_tide_consumer_lag > 50000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} backlog: {{ $value }} messages"

      - alert: PgTideLatencyHigh
        expr: histogram_quantile(0.99, rate(pg_tide_delivery_latency_seconds_bucket[5m])) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline {{ $labels.pipeline }} P99 latency: {{ $value }}s"

      - alert: PgTideRelayDown
        expr: up{job="pg-tide"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "pg-tide relay is not responding to scrapes"
```

### Alertmanager Routing

```yaml
# alertmanager.yml
route:
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
    - match:
        severity: warning
      receiver: 'slack'

receivers:
  - name: 'slack'
    slack_configs:
      - channel: '#alerts'
        send_resolved: true
  - name: 'pagerduty'
    pagerduty_configs:
      - routing_key: '${PAGERDUTY_KEY}'
```

## Docker Compose (Local Development)

```yaml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./rules:/etc/prometheus/rules
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    volumes:
      - ./dashboards:/var/lib/grafana/dashboards/pg-tide
      - ./provisioning:/etc/grafana/provisioning
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin

  pg-tide:
    image: pg-tide:latest
    environment:
      - DATABASE_URL=postgres://user:pass@postgres:5432/mydb
    ports:
      - "9091:9090"  # Metrics
```

## Key PromQL Queries

```promql
# Overall health
min(pg_tide_pipeline_healthy)

# Total throughput
sum(rate(pg_tide_messages_published_total[5m]))

# Per-pipeline error ratio
rate(pg_tide_publish_errors_total[5m]) / rate(pg_tide_messages_consumed_total[5m])

# Delivery latency percentiles
histogram_quantile(0.5, rate(pg_tide_delivery_latency_seconds_bucket[5m]))
histogram_quantile(0.95, rate(pg_tide_delivery_latency_seconds_bucket[5m]))
histogram_quantile(0.99, rate(pg_tide_delivery_latency_seconds_bucket[5m]))

# Lag trend (positive = growing)
deriv(pg_tide_consumer_lag[5m])
```

## Further Reading

- [Metrics](../features/metrics.md) — Complete metrics reference
- [Dashboards](../features/dashboards.md) — Dashboard details
- [Monitoring Cookbook](../operations/monitoring-cookbook.md) — Alert recipes
- [Datadog Integration](datadog.md) — Alternative monitoring platform
