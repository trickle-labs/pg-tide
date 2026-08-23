# Summary

- [Evaluate](evaluate/choosing-pg-tide.md)

## Getting Started

- [Introduction](introduction.md)
- [Installation](getting-started/installation.md)
- [Your first pipeline](getting-started/first-pipeline.md)

## Concepts

- [Transactional outbox](concepts/transactional-outbox.md)
- [Idempotent inbox](concepts/idempotent-inbox.md)
- [Message guarantees](concepts/message-guarantees.md)
- [Consumption and relay](concepts/consumption-and-relay.md)

## SQL API

- [Outbox API](sql-reference/outbox-api.md)
- [Inbox API](sql-reference/inbox-api.md)
- [Relay API](sql-reference/relay-api.md)
- [Consumer groups API](sql-reference/consumer-groups-api.md)
- [Catalog tables](sql-reference/catalog-tables.md)

## Relay

- [Configuration](relay-guide/configuration.md)
- [CLI reference](relay-guide/cli-reference.md)
- [Error handling](relay-guide/error-handling-guide.md)
- [Monitoring](relay-guide/monitoring.md)
- [Metrics](features/metrics.md)

## Supported Destinations

- [Destination overview](sinks/overview.md)
- [PostgreSQL inbox](sinks/pg-inbox.md)
- [NATS JetStream](sinks/nats.md)
- [Apache Kafka](sinks/kafka.md)
- [HTTPS webhook](sinks/webhook.md)
- [Diagnostic stdout and file output](sinks/stdout.md)

## Operations

- [Deployment guide](operations/deployment-guide.md)
- [Capacity planning](operations/capacity-planning.md)
- [Maintenance](operations/maintenance.md)
- [Monitoring cookbook](operations/monitoring-cookbook.md)
- [Backup and restore](operations/runbook-backup-restore.md)
- [Runbooks](operations/runbooks.md)
- [Relay upgrade](operations/runbook-relay-upgrade.md)
- [Stability guarantees](stability-guarantees.md)

## Security

- [Security](reference/security.md)
- [Security guide](guides/security-guide.md)
- [Threat model](reference/threat-model.md)
- [Dependency policy](reference/dependency-policy.md)

## Upgrades

- [Version compatibility](reference/version-compatibility.md)
- [v1 migration guide](operations/v1-migration-guide.md)
- [Config migration](relay-guide/config-migration.md)

## Troubleshooting

- [Troubleshooting](operations/troubleshooting.md)
- [Support bundles](support/support-bundles.md)
- [Crash recovery](operations/runbook-crash-recovery.md)
- [DLQ replay](operations/runbook-dlq-replay.md)
- [NATS runbook](operations/runbook-nats.md)
- [Kafka runbook](operations/runbook-kafka.md)
- [PostgreSQL inbox runbook](operations/runbook-pg-inbox.md)
- [Webhook runbook](operations/runbook-webhook.md)

## Reference

- [Error catalog](reference/error-catalog.md)
- [v1 scope](v1-scope.md)
- [Glossary](glossary.md)
- [Changelog](../CHANGELOG.md)

## Support

- [Support policy](support/support-policy.md)
- [Production-supported definition](support/production-supported.md)
- [Connector compatibility](support/connector-compatibility.md)
- [Test levels](support/test-levels.md)
- [Deprecation policy](support/deprecation-policy.md)

## Labs and Historical Material

- [Circuit breaker](features/circuit-breaker.md)
- [Config reload](features/config-reload.md)
- [Dashboards](features/dashboards.md)
- [Dead-letter queue](features/dead-letter-queue.md)
- [Graceful shutdown](features/graceful-shutdown.md)
- [HA coordination](features/ha-coordination.md)
- [Rate limiting](features/rate-limiting.md)
- [Webhook signatures](features/webhook-signatures.md)
- [CloudNativePG](integration/cloudnativepg.md)
- [Datadog](integration/datadog.md)
- [dbt](integration/dbt.md)
- [GitHub Actions](integration/github-actions.md)
- [Microcks](integration/microcks.md)
- [pg_trickle](integration/pg-trickle.md)
- [pgBouncer](integration/pgbouncer.md)
- [Prometheus and Grafana](integration/prometheus-grafana.md)
- [Terraform](integration/terraform.md)
- [Migrating to pg_tide](guides/migrating-to-pg-tide.md)

- [Deployment architectures](operations/deployment-architectures.md)
- [Independent review](operations/independent-review.md)
- [Partition management](operations/partition-management.md)
- [Pilot evidence](operations/pilot-evidence.md)
- [Pre-GA checklist](operations/pre-ga-checklist.md)
- [Release evidence](operations/release-evidence.md)
- [Release manager checklist](operations/release-manager-checklist.md)
- [Runbook schema migration](operations/runbook-schema-migration.md)
- [Scaling](operations/scaling.md)
- [Troubleshooting guide](operations/troubleshooting-guide.md)
- [Vulnerability response](operations/vulnerability-response.md)
- [Security evidence](reference/security-evidence.md)
- [PostgreSQL 17 feasibility](support/postgresql-17-feasibility.md)
- [Connector promotion](support/connector-promotion.md)
- [Connector release checklist](support/connector-release-checklist.md)

- [Consumer groups](concepts/consumer-groups.md)
- [Airbyte](sources/airbyte.md)
- [Event Hubs](sources/eventhubs.md)
- [Kafka source](sources/kafka.md)
- [Kinesis](sources/kinesis.md)
- [MQTT](sources/mqtt.md)
- [NATS source](sources/nats.md)
- [Outbox source](sources/outbox.md)
- [Source overview](sources/overview.md)
- [Pub/Sub](sources/pubsub.md)
- [RabbitMQ](sources/rabbitmq.md)
- [Redis](sources/redis.md)
- [Service Bus](sources/servicebus.md)
- [Singer](sources/singer.md)
- [SQS](sources/sqs.md)
- [stdin](sources/stdin.md)
- [Webhook receiver](sources/webhook-receiver.md)
- [Bidirectional sync](tutorials/bidirectional-sync.md)
- [Cross-region](tutorials/cross-region.md)
- [Data lake loading](tutorials/data-lake-loading.md)
- [Dead-letter queue tutorial](tutorials/dead-letter-queue.md)
- [Debezium replication](tutorials/debezium-replication.md)
- [End-to-end pipeline](tutorials/end-to-end-pipeline.md)
- [Fan-out pattern](tutorials/fan-out-pattern.md)
- [Getting started tutorial](tutorials/getting-started.md)
- [Kafka and Flink](tutorials/kafka-flink.md)
- [Notification fan-out](tutorials/notification-fanout.md)
- [Real-world scenarios](tutorials/real-world-scenarios.md)
- [Singer ETL](tutorials/singer-etl.md)
- [Microservice events](tutorials/microservice-events.md)

- [Canal](wire-formats/canal.md)
- [CDC JSON](wire-formats/cdc-json.md)
- [Debezium](wire-formats/debezium.md)
- [Maxwell](wire-formats/maxwell.md)
- [Native wire format](wire-formats/native.md)
- [Wire-format overview](wire-formats/overview.md)
- [PostgreSQL outbox sink](sinks/pg-outbox.md)

- [Architecture](evaluate/architecture.md)
- [Architecture decisions](reference/architecture-decisions.md)
