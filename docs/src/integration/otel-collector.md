# Integration: OpenTelemetry Collector

This guide covers advanced OpenTelemetry Collector configurations for pg_tide traces, including routing to multiple backends, sampling strategies, and enrichment.

## Basic Setup

pg_tide exports traces via OTLP gRPC. The OpenTelemetry Collector acts as a proxy/router between pg_tide and your observability backend(s).

```
pg-tide relay  →  OTEL Collector  →  Backend (Jaeger, Tempo, Datadog, etc.)
```

### Minimal Configuration

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

exporters:
  otlp:
    endpoint: "tempo:4317"
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp]
```

Start the collector:
```bash
otelcol --config otel-collector-config.yaml
```

Configure pg_tide:
```bash
pg-tide --otel-endpoint "http://otel-collector:4317" --postgres-url "..."
```

## Multi-Backend Routing

Send traces to multiple backends simultaneously:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

exporters:
  # Local Grafana Tempo for development
  otlp/tempo:
    endpoint: "tempo:4317"
    tls:
      insecure: true

  # Datadog for production alerting
  datadog:
    api:
      key: ${DD_API_KEY}

  # Jaeger for detailed trace debugging
  jaeger:
    endpoint: "jaeger-collector:14250"
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo, datadog, jaeger]
```

## Sampling Strategies

For high-throughput deployments, sampling reduces storage costs while maintaining visibility:

### Tail-Based Sampling (Recommended)

Keep all error traces and sample normal traces:

```yaml
processors:
  tail_sampling:
    decision_wait: 10s
    policies:
      # Always keep traces with errors
      - name: errors
        type: status_code
        status_code:
          status_codes: [ERROR]
      # Always keep slow traces (>5s)
      - name: slow
        type: latency
        latency:
          threshold_ms: 5000
      # Sample 10% of normal traces
      - name: normal
        type: probabilistic
        probabilistic:
          sampling_percentage: 10

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [tail_sampling]
      exporters: [otlp/tempo]
```

### Head-Based Sampling (Simpler)

```yaml
processors:
  probabilistic_sampler:
    sampling_percentage: 25  # Keep 25% of traces

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [probabilistic_sampler]
      exporters: [otlp/tempo]
```

## Resource Enrichment

Add deployment metadata to all traces:

```yaml
processors:
  resource:
    attributes:
      - key: deployment.environment
        value: production
        action: upsert
      - key: service.namespace
        value: messaging
        action: upsert
      - key: k8s.cluster.name
        value: production-east
        action: upsert

  # Detect Kubernetes metadata automatically
  k8sattributes:
    extract:
      metadata:
        - k8s.pod.name
        - k8s.namespace.name
        - k8s.deployment.name

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [resource, k8sattributes]
      exporters: [otlp/tempo]
```

## Span Processing

Filter or modify spans before export:

```yaml
processors:
  # Drop health check spans (noisy)
  filter:
    traces:
      span:
        - 'attributes["http.route"] == "/health"'
        - 'attributes["http.route"] == "/metrics"'

  # Batch spans for efficient export
  batch:
    timeout: 5s
    send_batch_size: 1000

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [filter, batch]
      exporters: [otlp/tempo]
```

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
spec:
  replicas: 2
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      containers:
        - name: collector
          image: otel/opentelemetry-collector-contrib:latest
          args: ["--config=/etc/otel/config.yaml"]
          ports:
            - containerPort: 4317  # OTLP gRPC
            - containerPort: 8888  # Collector metrics
          volumeMounts:
            - name: config
              mountPath: /etc/otel
      volumes:
        - name: config
          configMap:
            name: otel-collector-config
---
apiVersion: v1
kind: Service
metadata:
  name: otel-collector
spec:
  selector:
    app: otel-collector
  ports:
    - port: 4317
      targetPort: 4317
      name: otlp-grpc
```

## Correlating Traces with Metrics

Use the span metrics connector to derive RED metrics from traces:

```yaml
connectors:
  spanmetrics:
    histogram:
      explicit:
        buckets: [1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s]
    dimensions:
      - name: pipeline.name
      - name: pipeline.direction

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo, spanmetrics]
    metrics:
      receivers: [spanmetrics]
      exporters: [prometheus]
```

## Further Reading

- [OpenTelemetry Feature](../features/opentelemetry.md) — pg_tide trace configuration
- [Prometheus + Grafana](prometheus-grafana.md) — Metrics visualization
- [Datadog Integration](datadog.md) — Datadog as trace backend
