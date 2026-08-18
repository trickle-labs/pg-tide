# Connector release checklist

Generated from [`connectors.toml`](../../../connectors.toml). This is a release-review artifact, not a promise that unchecked evidence exists.

## PostgreSQL native outbox (`postgresql-outbox`)

- Maturity: **supported**
- Owner: @grove
- [x] postgresql-outbox-poll (integration, source): pg-tide-relay/tests/outbox_source_test.rs::test_outbox_poll_returns_pending_messages in `test-integration-core`

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
- [ ] metrics evidence: —
- [ ] runbooks: —
- [ ] upgrade tests: —

## PostgreSQL inbox (`postgresql-inbox`)

- Maturity: **supported**
- Owner: @grove
- [x] postgresql-inbox-round-trip (integration, sink): pg-tide-relay/tests/pg_inbox_sink_test.rs::test_pg_inbox_sink_round_trip in `test-integration-relay`
- [x] postgresql-inbox-deduplication (integration, sink): pg-tide-relay/tests/inbox_sink_test.rs::test_inbox_deduplication in `test-integration-relay`

## NATS JetStream outbound (`nats-jetstream-sink`)

- Maturity: **supported**
- Owner: @grove
- [x] nats-jetstream-publish-ack (e2e, sink): pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs::public_api_outbox_to_nats_e2e in `public-api-nats-e2e`

## NATS inbound (`nats-source`)

- Maturity: **preview**
- Owner: @grove
- [x] contract tests: [nats_test.rs](../../../pg-tide-relay/tests/nats_test.rs)
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

## HTTPS webhook outbound (`webhook-sink`)

- Maturity: **supported**
- Owner: @grove
- [x] webhook-http-post (integration, sink): pg-tide-relay/tests/webhook_test.rs::test_webhook_sink_posts_messages in `test-integration`

## Webhook inbound (`webhook-source`)

- Maturity: **preview**
- Owner: @grove
- [x] contract tests: [webhook_sig_test.rs](../../../pg-tide-relay/tests/webhook_sig_test.rs)
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

## Apache Kafka outbound (`kafka-sink`)

- Maturity: **supported**
- Owner: @grove
- [x] kafka-public-api-delivery (e2e, sink): pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs::public_api_outbox_to_kafka_e2e in `public-api-kafka-e2e`

## Apache Kafka inbound (`kafka-source`)

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
