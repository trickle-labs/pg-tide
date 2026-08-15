# Feature: Circuit Breaker

The circuit breaker protects your relay pipelines from cascading failures. When a sink becomes unavailable — a Kafka broker goes down, an HTTP endpoint returns 503s, a database connection drops — the circuit breaker detects the pattern of consecutive failures and stops attempting delivery. This prevents wasted resources, avoids flooding error logs, and gives the downstream system time to recover.

## State Machine

The circuit breaker has three states:

```
                 failure_threshold
    ┌────────┐   consecutive     ┌────────┐
    │ CLOSED │ ─────────────────→│  OPEN  │
    │(normal)│                   │(failing)│
    └────────┘                   └────────┘
        ↑                            │
        │   success_threshold        │ half_open_timeout
        │   consecutive              ↓
    ┌────────────┐              ┌───────────┐
    │   CLOSED   │←─────────────│ HALF-OPEN │
    └────────────┘  success     │  (probe)  │
                                └───────────┘
                                     │
                                     │ failure
                                     ↓
                                ┌────────┐
                                │  OPEN  │
                                └────────┘
```

**Closed (normal operation):** Messages flow to the sink. Each failure increments a counter; each success resets it. When consecutive failures reach `failure_threshold`, the circuit opens.

**Open (failing):** All publish attempts fail immediately without contacting the sink. After `half_open_timeout` elapses, the circuit transitions to half-open.

**Half-open (recovery probe):** A single message is allowed through as a probe. If it succeeds, `success_threshold` consecutive successes close the circuit. If it fails, the circuit re-opens immediately.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-pipeline',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "orders",
        "circuit_breaker": {
            "enabled": true,
            "failure_threshold": 5,
            "success_threshold": 3,
            "half_open_timeout_seconds": 30
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `circuit_breaker.enabled` | bool | `true` | Enable circuit breaker |
| `circuit_breaker.failure_threshold` | int | `5` | Consecutive failures to open circuit |
| `circuit_breaker.success_threshold` | int | `3` | Consecutive successes to close from half-open |
| `circuit_breaker.half_open_timeout_seconds` | int | `30` | Seconds before open → half-open transition |

## Behavior When Open

When the circuit is open:

1. Messages are **not** sent to the sink (no wasted network calls)
2. If a [DLQ](dead-letter-queue.md) is configured, messages are routed there for later replay
3. If no DLQ, the worker sleeps until the half-open timeout, then probes
4. Prometheus metrics reflect the unhealthy state (`pipeline_healthy` = 0)
5. The `/health` endpoint reports the pipeline as unhealthy

## Tuning Guidelines

**Low `failure_threshold` (2-3):** Opens quickly, aggressive protection. Use for sinks that rarely have transient errors — if they fail twice, something is seriously wrong.

**High `failure_threshold` (10-20):** Tolerates intermittent failures. Use for sinks with occasional transient errors (network blips, DNS resolution hiccups).

**Short `half_open_timeout` (5-15s):** Recovers quickly after brief outages. Use for sinks that recover fast (load-balanced services, managed cloud endpoints).

**Long `half_open_timeout` (60-300s):** Gives downstream systems more time to recover. Use for sinks that take time to restart (database failovers, broker rebalances).

## Monitoring

The circuit breaker state is reflected in:

- **Prometheus gauge:** `pg_tide_pipeline_healthy` (1 = closed, 0 = open)
- **Health endpoint:** `/health` returns 503 when any pipeline's circuit is open
- **Logs:** State transitions logged at `warn` level (open) and `info` level (close)

## Further Reading

- [Dead Letter Queue](dead-letter-queue.md) — Where messages go when circuit is open
- [Rate Limiting](rate-limiting.md) — Complementary back-pressure mechanism
- [Monitoring](../relay-guide/monitoring.md) — Prometheus metrics reference
