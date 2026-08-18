# Summary

[Introduction](introduction.md)

---

# Evaluate

- [Choosing pg_tide](evaluate/choosing-pg-tide.md)
- [Architecture](evaluate/architecture.md)

---

# Getting Started

- [Installation](getting-started/installation.md)
- [Your First Pipeline](getting-started/first-pipeline.md)

---

# Concepts

- [Transactional Outbox](concepts/transactional-outbox.md)
- [Idempotent Inbox](concepts/idempotent-inbox.md)
- [Consumer Groups](concepts/consumer-groups.md)
- [Message Guarantees](concepts/message-guarantees.md)
- [Consumption & Relay](concepts/consumption-and-relay.md)

---

# SQL Reference

- [Outbox API](sql-reference/outbox-api.md)
- [Inbox API](sql-reference/inbox-api.md)
- [Relay API](sql-reference/relay-api.md)
- [Consumer Groups API](sql-reference/consumer-groups-api.md)
- [Catalog Tables](sql-reference/catalog-tables.md)

---

# Relay Guide

- [Configuration](relay-guide/configuration.md)
- [Catalog vs. TOML](relay-guide/catalog-vs-toml.md)
- [CLI Reference](relay-guide/cli-reference.md)
- [Backends](relay-guide/backends.md)
- [Error Handling](relay-guide/error-handling-guide.md)
- [Monitoring](relay-guide/monitoring.md)

---

# Sinks

- [Overview](sinks/overview.md)
- [Apache Kafka](sinks/kafka.md)
- [NATS JetStream](sinks/nats.md)
- [RabbitMQ](sinks/rabbitmq.md)
- [Redis Streams](sinks/redis.md)
- [Amazon SQS](sinks/sqs.md)
- [Amazon Kinesis](sinks/kinesis.md)
- [Google Pub/Sub](sinks/pubsub.md)
- [Azure Service Bus](sinks/servicebus.md)
- [Azure Event Hubs](sinks/eventhubs.md)
- [MQTT v5](sinks/mqtt.md)
- [HTTP Webhook](sinks/webhook.md)
- [ClickHouse](sinks/clickhouse.md)
- [Snowflake](sinks/snowflake.md)
- [Google BigQuery](sinks/bigquery.md)
- [Apache Iceberg](sinks/iceberg.md)
- [Delta Lake](sinks/delta.md)
- [DuckLake](sinks/ducklake.md)
- [MongoDB](sinks/mongodb.md)
- [Elasticsearch](sinks/elasticsearch.md)
- [Object Storage (S3/GCS/Azure)](sinks/object-storage.md)
- [Apache Arrow Flight](sinks/arrow-flight.md)
- [Slack](sinks/slack.md)
- [Discord](sinks/discord.md)
- [PagerDuty](sinks/pagerduty.md)
- [Singer Target](sinks/singer.md)
- [Airbyte Destination](sinks/airbyte.md)
- [PostgreSQL Inbox](sinks/pg-inbox.md)
- [PostgreSQL Outbox](sinks/pg-outbox.md)
- [stdout](sinks/stdout.md)

---

# Sources

- [Overview](sources/overview.md)
- [PostgreSQL Outbox](sources/outbox.md)
- [Apache Kafka](sources/kafka.md)
- [NATS JetStream](sources/nats.md)
- [RabbitMQ](sources/rabbitmq.md)
- [Redis Streams](sources/redis.md)
- [Amazon SQS](sources/sqs.md)
- [Amazon Kinesis](sources/kinesis.md)
- [Google Pub/Sub](sources/pubsub.md)
- [Azure Service Bus](sources/servicebus.md)
- [Azure Event Hubs](sources/eventhubs.md)
- [MQTT v5](sources/mqtt.md)
- [HTTP Webhook Receiver](sources/webhook-receiver.md)
- [Singer Tap](sources/singer.md)
- [Airbyte Source](sources/airbyte.md)
- [stdin / File](sources/stdin.md)

---

# Wire Formats

- [Overview](wire-formats/overview.md)
- [Native](wire-formats/native.md)
- [Debezium](wire-formats/debezium.md)
- [Maxwell](wire-formats/maxwell.md)
- [Canal](wire-formats/canal.md)
- [CDC JSON](wire-formats/cdc-json.md)

---

# Features

- [Dead Letter Queue](features/dead-letter-queue.md)
- [Circuit Breaker](features/circuit-breaker.md)
- [Rate Limiting](features/rate-limiting.md)
- [Schema Registry](features/schema-registry.md)
- [Transforms (JMESPath)](features/transforms.md)
- [Content-Based Routing](features/routing.md)
- [Webhook Signatures](features/webhook-signatures.md)
- [Dry-Run & Replay](features/dry-run-replay.md)
- [Configuration Hot-Reload](features/config-reload.md)
- [OpenTelemetry](features/opentelemetry.md)
- [HA Coordination](features/ha-coordination.md)
- [Graceful Shutdown](features/graceful-shutdown.md)
- [Prometheus Metrics](features/metrics.md)
- [Grafana Dashboards](features/dashboards.md)
- [Singer Protocol](features/singer-protocol.md)
- [Airbyte Protocol](features/airbyte-protocol.md)

---

# Operations

- [Deployment Guide](operations/deployment-guide.md)
- [Deployment Architectures](operations/deployment-architectures.md)
- [Scaling](operations/scaling.md)
- [Capacity Planning](operations/capacity-planning.md)
- [Maintenance](operations/maintenance.md)
- [Monitoring Cookbook](operations/monitoring-cookbook.md)
- [Operations Runbooks](operations/runbooks.md)
- [Partition Management](operations/partition-management.md)
- [Troubleshooting](operations/troubleshooting.md)
- [Troubleshooting Guide](operations/troubleshooting-guide.md)

## Runbooks

- [Crash Recovery](operations/runbook-crash-recovery.md)
- [DLQ Replay](operations/runbook-dlq-replay.md)
- [Schema Migration](operations/runbook-schema-migration.md)
- [Relay Upgrade](operations/runbook-relay-upgrade.md)
- [PostgreSQL Inbox](operations/runbook-pg-inbox.md)
- [NATS JetStream](operations/runbook-nats.md)
- [Apache Kafka](operations/runbook-kafka.md)
- [HTTPS Webhook](operations/runbook-webhook.md)

---

# Tutorials

- [Getting Started](tutorials/getting-started.md)
- [End-to-End Pipelines](tutorials/end-to-end-pipeline.md)
- [Real-World Scenarios](tutorials/real-world-scenarios.md)
- [Bidirectional Sync](tutorials/bidirectional-sync.md)
- [Fan-out Pattern](tutorials/fan-out-pattern.md)
- [Dead-Letter Queue](tutorials/dead-letter-queue.md)
- [Kafka + Flink Streaming](tutorials/kafka-flink.md)
- [Data Lake Loading](tutorials/data-lake-loading.md)
- [Microservice Event Bus](tutorials/microservice-events.md)
- [Debezium-Compatible CDC](tutorials/debezium-replication.md)
- [Singer/Meltano ETL](tutorials/singer-etl.md)
- [Notification Fan-Out](tutorials/notification-fanout.md)
- [Cross-Region Replication](tutorials/cross-region.md)

---

# Guides

- [Migrating to pg_tide](guides/migrating-to-pg-tide.md)
- [Security Guide](guides/security-guide.md)

---

# Integrations

- [pg-trickle](integration/pg-trickle.md)
- [dbt](integration/dbt.md)
- [CloudNativePG](integration/cloudnativepg.md)
- [PgBouncer](integration/pgbouncer.md)
- [Terraform](integration/terraform.md)
- [GitHub Actions](integration/github-actions.md)
- [Prometheus + Grafana](integration/prometheus-grafana.md)
- [Datadog](integration/datadog.md)
- [OpenTelemetry Collector](integration/otel-collector.md)
- [Microcks (AsyncAPI contract testing)](integration/microcks.md)

---

# Reference

- [Security](reference/security.md)
- [Threat Model](reference/threat-model.md)
- [Security Evidence](reference/security-evidence.md)
- [Dependency Policy](reference/dependency-policy.md)
- [Architecture Decisions](reference/architecture-decisions.md)
- [Version Compatibility](reference/version-compatibility.md)
- [Changelog](../CHANGELOG.md)

---

# Support

- [Support Policy](support/support-policy.md)
- [Production-Supported Definition](support/production-supported.md)
- [Test Levels](support/test-levels.md)
- [PostgreSQL 17 Feasibility](support/postgresql-17-feasibility.md)

---

[Glossary](glossary.md)
