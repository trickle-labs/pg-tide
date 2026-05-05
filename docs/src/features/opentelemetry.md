# Feature: OpenTelemetry

pg_tide integrates with OpenTelemetry for distributed tracing, giving you end-to-end visibility into how messages flow from your application through the outbox, relay, and into the sink. Traces show exactly where time is spent — polling the source, applying transforms, publishing to the sink, and acknowledging delivery.

## What You Get

With OpenTelemetry enabled, every relay poll cycle produces a distributed trace with spans for:

- **`poll_cycle`** — Root span covering one full iteration
- **`source_poll`** — Time spent fetching messages from the source
- **`sink_publish`** — Time spent delivering the batch to the sink
- **`source_acknowledge`** — Time spent acknowledging processed messages

These spans include attributes like pipeline name, batch size, message count, and any errors encountered.

## Configuration

Enable OpenTelemetry by setting the OTLP endpoint:

```bash
pg-tide \
  --postgres-url "postgres://..." \
  --otel-endpoint "http://localhost:4317"
```

Or via environment variable:

```bash
export PG_TIDE_OTEL_ENDPOINT="http://localhost:4317"
pg-tide --postgres-url "postgres://..."
```

### Configuration Reference

| Parameter | Source | Default | Description |
|-----------|--------|---------|-------------|
| `--otel-endpoint` | CLI | `null` | OTLP gRPC endpoint |
| `PG_TIDE_OTEL_ENDPOINT` | Environment | `null` | OTLP gRPC endpoint |

When no endpoint is configured, OpenTelemetry is completely disabled (zero overhead).

## Compatible Backends

pg_tide exports traces via OTLP (OpenTelemetry Protocol) gRPC, compatible with:

- **Jaeger** — Open-source distributed tracing
- **Grafana Tempo** — Scalable trace backend
- **Honeycomb** — Observability platform
- **Datadog** — APM and tracing
- **AWS X-Ray** (via OTEL Collector)
- **Google Cloud Trace** (via OTEL Collector)
- **Any OTEL Collector** — Route to multiple backends

## Example: Grafana Tempo

```yaml
# docker-compose.yml
services:
  tempo:
    image: grafana/tempo:latest
    ports:
      - "4317:4317"   # OTLP gRPC
      - "3200:3200"   # Tempo query
  
  pg-tide:
    environment:
      PG_TIDE_OTEL_ENDPOINT: "http://tempo:4317"
```

## Trace Attributes

Spans include these attributes for filtering and analysis:

| Attribute | Description |
|-----------|-------------|
| `service.name` | `"pg-tide-relay"` |
| `pipeline.name` | Pipeline identifier |
| `pipeline.direction` | `"forward"` or `"reverse"` |
| `batch.size` | Number of messages in batch |
| `error` | Error message (if span errored) |

## Feature Gate

OpenTelemetry support is compiled behind a feature gate. The pre-built binaries include it. If building from source:

```bash
cargo build --release --features otel
```

Without the `otel` feature, all tracing functions are no-ops with zero runtime cost.

## Further Reading

- [Metrics](metrics.md) — Prometheus metrics (complementary to tracing)
- [Monitoring](../relay-guide/monitoring.md) — Complete observability setup
- [OpenTelemetry Collector Integration](../integration/otel-collector.md) — Advanced collector configuration
