# Consumption and Relay

The relay is a separate process that reads pipeline configuration from the
PostgreSQL catalog. It acquires advisory-lock ownership for each enabled
pipeline, polls the canonical outbox, publishes a batch, and records a durable
checkpoint.

## Forward pipeline

```text
application transaction
        ↓
PostgreSQL outbox → pg-tide relay → inbox, NATS, Kafka, or HTTPS webhook
```

Create a forward pipeline with `tide.relay_set_outbox_v2()`. The source is
always `outbox`; `sink_type` selects one supported destination. stdout and file
are available for diagnostics.

## Ownership and retries

Only the relay instance holding the pipeline's advisory lock polls it. A
second instance waits and takes over after the owner stops. Destination errors
use bounded retry and circuit-breaker handling. Messages that exceed the DLQ
policy can be replayed after the destination is healthy.

## Configuration changes

Catalog updates notify active relay instances. The relay also polls the catalog
periodically, so a missed notification does not require a restart.
