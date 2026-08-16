# Feature: Graceful Shutdown

When the relay receives a shutdown signal (SIGTERM or SIGINT), it performs a
graceful drain. In-flight batches finish or are aborted before the worker's
dedicated ownership session is released. A batch whose sink succeeded but whose
checkpoint did not commit may be delivered again with the same stable identity.

## Shutdown Sequence

```
1. SIGTERM received
2. Coordinator signals all worker tasks to stop
3. Each worker:
   a. Finishes the current batch publish if possible
   b. Commits the source checkpoint only after durable terminal disposition
   c. Exits its processing loop
4. Coordinator waits for all workers to exit
5. Coordinator releases ownership through the same session (or drops it after abort)
6. Metrics server stops accepting new requests
7. OpenTelemetry flushes pending traces
8. Process exits with code 0
```

## Why This Matters

Without graceful shutdown:
- **In-flight messages** could be published to the sink but not acknowledged in the source, causing re-delivery (duplicates)
- **Advisory locks** would be held until PostgreSQL's connection timeout (potentially minutes), delaying failover
- **Metrics** might not be scraped for the final interval
- **Traces** might be lost

With graceful shutdown:
- A committed source event is retried when its checkpoint was not committed
- Advisory locks are released only after the worker is gone, enabling safe failover
- Final metrics are available for scraping
- All traces are exported

## Shutdown Timeout

The relay enforces a maximum shutdown duration. If workers don't exit within the timeout, the worker is aborted before the
ownership session is released:

```bash
# Default: 30 seconds
pg-tide --shutdown-timeout 30
```

If a sink is extremely slow (e.g., a webhook endpoint that takes 60 seconds to respond), increase this timeout. In Kubernetes, ensure `terminationGracePeriodSeconds` exceeds your shutdown timeout.

## Kubernetes Integration

In Kubernetes deployments, the pod receives SIGTERM when it's being evicted, scaled down, or updated. Configure your deployment to give pg_tide enough time:

```yaml
spec:
  terminationGracePeriodSeconds: 60  # Must exceed shutdown-timeout
  containers:
    - name: pg-tide
      command: ["pg-tide", "--shutdown-timeout", "45"]
```

### PreStop Hook (optional)

If you need extra time for load balancers to drain connections to the metrics endpoint:

```yaml
lifecycle:
  preStop:
    exec:
      command: ["sleep", "5"]
```

## Signal Handling

| Signal | Behavior |
|--------|----------|
| `SIGTERM` | Graceful shutdown (standard Kubernetes signal) |
| `SIGINT` | Graceful shutdown (Ctrl+C in terminal) |
| `SIGKILL` | Immediate termination (cannot be caught) |

## Further Reading

- [HA Coordination](ha-coordination.md) — How shutdown interacts with advisory locks
- [Deployment Guide](../operations/deployment-guide.md) — Production deployment practices
