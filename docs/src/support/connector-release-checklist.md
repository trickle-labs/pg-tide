# Connector release checklist

Generated from [`connectors.toml`](../../../connectors.toml). This is a release-review artifact, not a promise that unchecked evidence exists.

## PostgreSQL native outbox (`postgresql-outbox`)

- Maturity: **supported**
- Owner: @grove
- [x] postgresql-outbox-poll (integration, source): pg-tide-relay/tests/outbox_source_test.rs::test_outbox_poll_returns_pending_messages in `test-integration-core`

## stdout and file diagnostics (`diagnostics`)

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

## HTTPS webhook outbound (`webhook-sink`)

- Maturity: **supported**
- Owner: @grove
- [x] webhook-http-post (integration, sink): pg-tide-relay/tests/webhook_test.rs::test_webhook_sink_posts_messages in `test-integration`

## Apache Kafka outbound (`kafka-sink`)

- Maturity: **supported**
- Owner: @grove
- [x] kafka-public-api-delivery (e2e, sink): pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs::public_api_outbox_to_kafka_e2e in `public-api-kafka-e2e`
