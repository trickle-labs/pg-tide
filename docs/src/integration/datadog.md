# Integration: Datadog

This guide covers monitoring pg_tide with Datadog, including metrics collection, log forwarding, and APM trace correlation.

## Metrics Collection

### Option 1: Prometheus Integration

Datadog's [OpenMetrics check](https://docs.datadoghq.com/integrations/openmetrics/) can scrape pg_tide's Prometheus endpoint directly:

```yaml
# datadog-agent/conf.d/openmetrics.d/conf.yaml
instances:
  - openmetrics_endpoint: http://pg-tide-relay:9090/metrics
    namespace: pg_tide
    metrics:
      - pg_tide_messages_published_total
      - pg_tide_messages_consumed_total
      - pg_tide_publish_errors_total
      - pg_tide_dedup_skipped_total
      - pg_tide_pipeline_healthy
      - pg_tide_consumer_lag
      - pg_tide_delivery_latency_seconds
```

### Option 2: Kubernetes Annotations

With the Datadog Agent running as a DaemonSet:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-tide-relay
spec:
  template:
    metadata:
      annotations:
        ad.datadoghq.com/pg-tide.checks: |
          {
            "openmetrics": {
              "instances": [{
                "openmetrics_endpoint": "http://%%host%%:9090/metrics",
                "namespace": "pg_tide",
                "metrics": ["pg_tide_*"]
              }]
            }
          }
```

## Log Collection

### Structured JSON Logs

Configure the relay to emit JSON logs:

```bash
pg-tide --log-format json --postgres-url "..."
```

### Datadog Agent Log Collection

```yaml
# Kubernetes pod annotation
annotations:
  ad.datadoghq.com/pg-tide.logs: |
    [{
      "source": "pg-tide",
      "service": "pg-tide-relay",
      "log_processing_rules": [{
        "type": "multi_line",
        "name": "rust_panics",
        "pattern": "^thread '"
      }]
    }]
```

### Log Facets

Create facets for common fields:
- `pipeline` — Pipeline name
- `direction` — forward/reverse
- `batch_size` — Messages in batch
- `error` — Error message

## APM / Traces

### Option 1: OpenTelemetry → Datadog

pg_tide exports OTLP traces. Route them through the OTEL Collector to Datadog:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

exporters:
  datadog:
    api:
      key: ${DD_API_KEY}

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [datadog]
```

Configure pg_tide to send traces:
```bash
pg-tide --otel-endpoint "http://otel-collector:4317" --postgres-url "..."
```

### Option 2: Datadog Agent OTLP Ingestion

The Datadog Agent can receive OTLP directly (Agent 7.35+):

```yaml
# datadog.yaml
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
```

```bash
pg-tide --otel-endpoint "http://datadog-agent:4317" --postgres-url "..."
```

## Dashboards

Create a Datadog dashboard with these widgets:

### Throughput (Timeseries)
```
sum:pg_tide.pg_tide_messages_published_total.count{*} by {pipeline}.as_rate()
```

### Error Rate (Timeseries)
```
sum:pg_tide.pg_tide_publish_errors_total.count{*} by {pipeline}.as_rate()
```

### Pipeline Health (Query Value)
```
min:pg_tide.pg_tide_pipeline_healthy{*} by {pipeline}
```

### Consumer Lag (Timeseries)
```
avg:pg_tide.pg_tide_consumer_lag{*} by {pipeline}
```

## Monitors (Alerts)

### Pipeline Down
```
Monitor Type: Metric
Query: min(last_5m):min:pg_tide.pg_tide_pipeline_healthy{*} by {pipeline} < 1
Alert: Pipeline {{pipeline.name}} is unhealthy
```

### High Error Rate
```
Monitor Type: Metric
Query: sum(last_5m):sum:pg_tide.pg_tide_publish_errors_total.count{*} by {pipeline}.as_rate() > 1
Warning: Pipeline {{pipeline.name}} error rate above threshold
```

### Growing Lag
```
Monitor Type: Metric
Query: avg(last_10m):avg:pg_tide.pg_tide_consumer_lag{*} by {pipeline} > 10000
Warning: Pipeline {{pipeline.name}} has {{value}} pending messages
```

## Further Reading

- [Metrics](../features/metrics.md) — Available metrics
- [OpenTelemetry](../features/opentelemetry.md) — Trace configuration
- [Prometheus + Grafana](prometheus-grafana.md) — Alternative monitoring stack
