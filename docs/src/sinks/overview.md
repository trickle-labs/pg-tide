# Destinations Overview

The relay delivers PostgreSQL outbox messages to the supported destinations
below. All deliveries are at-least-once and advance the source checkpoint only
after the destination acknowledges the batch.

| Destination | Use case |
|---|---|
| [PostgreSQL Inbox](pg-inbox.md) | Idempotent cross-service delivery inside PostgreSQL |
| [NATS JetStream](nats.md) | Low-latency durable messaging |
| [Apache Kafka](kafka.md) | Partitioned event streaming |
| [HTTPS Webhook](webhook.md) | Delivery to an external HTTP service |
| [stdout and file](stdout.md) | Local diagnostics and debugging |

Configure every destination with `tide.relay_set_outbox_v2()` and select the
destination with `sink_type`. Use secret references for credentials and URLs;
the relay never writes resolved secret values to logs or metric labels.

See the [connector compatibility matrix](../support/connector-compatibility.md)
for tested versions, profiles, and evidence.
