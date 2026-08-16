# Connector release checklist

Generated from [`connectors.toml`](../../../connectors.toml). This is a release-review artifact, not a promise that unchecked evidence exists.

## PostgreSQL native outbox (`postgresql-outbox`)

- Maturity: **supported**
- Owner: @grove
- [x] contract tests: [outbox_source_test.rs](../../../pg-tide-relay/tests/outbox_source_test.rs)
- [x] integration tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] e2e tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] failure before publish tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] failure after publish tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] restart tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] duplicate tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] auth tests: [publisher_acl_test.rs](../../../pg-tide-relay/tests/publisher_acl_test.rs)
- [x] tls tests: [tls_test.rs](../../../pg-tide-relay/tests/tls_test.rs)
- [x] redaction tests: [config.rs](../../../pg-tide-relay/src/config.rs)
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [x] runbooks: [runbook-crash-recovery.md](../operations/runbook-crash-recovery.md)
- [x] upgrade tests: [v042_validation_test.rs](../../../pg-tide-relay/tests/v042_validation_test.rs)

## pg_trickle outbox compatibility (`pg-trickle-compatibility`)

- Maturity: **preview**
- Owner: @grove
- [x] contract tests: [outbox_source_test.rs](../../../pg-tide-relay/tests/outbox_source_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## stdin, stdout, and file diagnostics (`diagnostics`)

- Maturity: **supported**
- Owner: @grove
- [x] contract tests: [postgres_insert_microbenchmark.rs](../../../pg-tide-relay/tests/postgres_insert_microbenchmark.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## PostgreSQL inbox (`postgresql-inbox`)

- Maturity: **supported**
- Owner: @grove
- [x] contract tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] integration tests: [inbox_sink_test.rs](../../../pg-tide-relay/tests/inbox_sink_test.rs)
- [x] e2e tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] failure before publish tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] failure after publish tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] restart tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] duplicate tests: [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs)
- [x] auth tests: [publisher_acl_test.rs](../../../pg-tide-relay/tests/publisher_acl_test.rs)
- [x] tls tests: [tls_test.rs](../../../pg-tide-relay/tests/tls_test.rs)
- [x] redaction tests: [config.rs](../../../pg-tide-relay/src/config.rs)
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [x] runbooks: [runbook-relay-upgrade.md](../operations/runbook-relay-upgrade.md)
- [x] upgrade tests: [v042_validation_test.rs](../../../pg-tide-relay/tests/v042_validation_test.rs)

## NATS JetStream (`nats`)

- Maturity: **supported**
- Owner: @grove
- [x] contract tests: [nats_test.rs](../../../pg-tide-relay/tests/nats_test.rs)
- [x] integration tests: [nats_test.rs](../../../pg-tide-relay/tests/nats_test.rs)
- [x] e2e tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] failure before publish tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] failure after publish tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] restart tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] duplicate tests: [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs)
- [x] auth tests: [tls_test.rs](../../../pg-tide-relay/tests/tls_test.rs)
- [x] tls tests: [tls_test.rs](../../../pg-tide-relay/tests/tls_test.rs)
- [x] redaction tests: [config.rs](../../../pg-tide-relay/src/config.rs)
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [x] runbooks: [runbook-crash-recovery.md](../operations/runbook-crash-recovery.md)
- [x] upgrade tests: [v042_validation_test.rs](../../../pg-tide-relay/tests/v042_validation_test.rs)

## HTTP webhook (`webhook`)

- Maturity: **preview**
- Owner: @grove
- [x] contract tests: [webhook_test.rs](../../../pg-tide-relay/tests/webhook_test.rs), [webhook_sig_test.rs](../../../pg-tide-relay/tests/webhook_sig_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Apache Kafka (`kafka`)

- Maturity: **preview**
- Owner: @grove
- [x] contract tests: [kafka_test.rs](../../../pg-tide-relay/tests/kafka_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Redis Streams (`redis`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [redis_test.rs](../../../pg-tide-relay/tests/redis_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Amazon SQS (`sqs`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [sqs_test.rs](../../../pg-tide-relay/tests/sqs_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## RabbitMQ (`rabbitmq`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [rabbitmq_test.rs](../../../pg-tide-relay/tests/rabbitmq_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Google Pub/Sub (`pubsub`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [pubsub_test.rs](../../../pg-tide-relay/tests/pubsub_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Amazon Kinesis (`kinesis`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [kinesis_test.rs](../../../pg-tide-relay/tests/kinesis_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Azure Service Bus (`servicebus`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [servicebus_test.rs](../../../pg-tide-relay/tests/servicebus_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## MQTT v5 (`mqtt`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [mqtt_test.rs](../../../pg-tide-relay/tests/mqtt_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Azure Event Hubs (`eventhubs`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [eventhubs_test.rs](../../../pg-tide-relay/tests/eventhubs_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Elasticsearch (`elasticsearch`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [elasticsearch_test.rs](../../../pg-tide-relay/tests/elasticsearch_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Object storage (`object-storage`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [object_storage_test.rs](../../../pg-tide-relay/tests/object_storage_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Slack (`slack`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [slack_test.rs](../../../pg-tide-relay/tests/slack_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Discord (`discord`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [discord_test.rs](../../../pg-tide-relay/tests/discord_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## PagerDuty (`pagerduty`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [pagerduty_test.rs](../../../pg-tide-relay/tests/pagerduty_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Apache Arrow Flight (`arrow-flight`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [arrow_flight_test.rs](../../../pg-tide-relay/tests/arrow_flight_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Singer (`singer`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [singer_test.rs](../../../pg-tide-relay/tests/singer_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Airbyte (`airbyte`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [airbyte_test.rs](../../../pg-tide-relay/tests/airbyte_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## ClickHouse (`clickhouse`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [clickhouse_test.rs](../../../pg-tide-relay/tests/clickhouse_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## MongoDB (`mongodb`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [mongodb_test.rs](../../../pg-tide-relay/tests/mongodb_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Google BigQuery (`bigquery`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [bigquery_test.rs](../../../pg-tide-relay/tests/bigquery_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Snowflake (`snowflake`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [snowflake_test.rs](../../../pg-tide-relay/tests/snowflake_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Delta Lake (`delta`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [delta_test.rs](../../../pg-tide-relay/tests/delta_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Apache Iceberg (`iceberg`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [iceberg_test.rs](../../../pg-tide-relay/tests/iceberg_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## DuckLake (`ducklake`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [ducklake_test.rs](../../../pg-tide-relay/tests/ducklake_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## RockLake (`rocklake`)

- Maturity: **experimental**
- Owner: @grove
- [x] contract tests: [rocklake_test.rs](../../../pg-tide-relay/tests/rocklake_test.rs)
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## Fan-in compatibility surface (`fan-in`)

- Maturity: **experimental**
- Owner: @grove
- [ ] contract tests: —
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## DuckLake reverse source (unavailable) (`ducklake-reverse`)

- Maturity: **experimental**
- Owner: @grove
- [ ] contract tests: —
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —

## PostgreSQL WAL logical source (groundwork) (`wal-logical-source`)

- Maturity: **experimental**
- Owner: @grove
- [ ] contract tests: —
- [ ] integration tests: —
- [ ] e2e tests: —
- [ ] failure before publish tests: —
- [ ] failure after publish tests: —
- [ ] restart tests: —
- [ ] duplicate tests: —
- [ ] auth tests: —
- [ ] tls tests: —
- [ ] redaction tests: —
- [x] metrics evidence: [metrics.rs](../../../pg-tide-relay/src/metrics.rs)
- [ ] runbooks: —
- [ ] upgrade tests: —
