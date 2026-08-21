# Connector compatibility

This page is generated from [`connectors.toml`](../../../connectors.toml). Maturity follows the [production-supported policy](production-supported.md).

| ID | Direction | Maturity | Cargo feature | Profiles | Tested versions | Owner | Docs | Evidence |
|---|---|---|---|---|---|---|---|---|
| <a id="postgresql-outbox"></a>postgresql-outbox | source | supported | `built in` | core | PostgreSQL 18 | @grove | [outbox.md](../sources/outbox.md) | [outbox_source_test.rs](../../../pg-tide-relay/tests/outbox_source_test.rs) |
| <a id="diagnostics"></a>diagnostics | sink | supported | `stdout` | core | local process | @grove | [stdout.md](../sinks/stdout.md) | [postgres_insert_microbenchmark.rs](../../../pg-tide-relay/tests/postgres_insert_microbenchmark.rs) |
| <a id="postgresql-inbox"></a>postgresql-inbox | sink | supported | `built in` | core | PostgreSQL 18 | @grove | [pg-inbox.md](../sinks/pg-inbox.md), [pg-outbox.md](../sinks/pg-outbox.md) | [pg_inbox_sink_test.rs](../../../pg-tide-relay/tests/pg_inbox_sink_test.rs), [inbox_sink_test.rs](../../../pg-tide-relay/tests/inbox_sink_test.rs), [public_api_outbox_to_pg_inbox_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_pg_inbox_e2e.rs) |
| <a id="nats-jetstream-sink"></a>nats-jetstream-sink | sink | supported | `nats` | core | NATS Server 2.11.0 with JetStream | @grove | [nats.md](../sinks/nats.md) | [public_api_outbox_to_nats_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs) |
| <a id="webhook-sink"></a>webhook-sink | sink | supported | `webhook` | core | HTTP/1.1 with TLS 1.3 | @grove | [webhook.md](../sinks/webhook.md) | [webhook_test.rs](../../../pg-tide-relay/tests/webhook_test.rs), [public_api_outbox_to_webhook_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_webhook_e2e.rs) |
| <a id="kafka-sink"></a>kafka-sink | sink | supported | `kafka` | core-kafka | Apache Kafka 3.8.0 KRaft | @grove | [kafka.md](../sinks/kafka.md) | [public_api_outbox_to_kafka_e2e.rs](../../../pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs) |

A missing evidence category is `not yet proved`; compiling a connector does not promote it.
The normal relay build is `core`. Kafka production support is explicit in the tested `core-kafka` profile.
