# Runbook: Apache Kafka

**Scope:** Outbox to Kafka producer delivery.
**Profile:** `core-kafka`.

## Symptoms

Use `pg-tide status --output json --postgres-url "$PG_TIDE_POSTGRES_URL"` and
inspect bounded codes: `unavailable`, `timeout`, `throttled`, `authentication`,
`authorization`, `invalid_destination`, `message_too_large`, and `protocol_rejection`.

## Diagnosis

1. Run `pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"`.
2. Verify the broker listener, topic, partition policy, and producer ACL.
3. Check `pg_tide_relay_connector_failures_total{connector="kafka"}` and the
   pipeline checkpoint. Do not expose broker addresses or raw librdkafka errors in
   status or metrics.
4. Confirm the topic message limit is at least the configured pg_tide encoded
   message limit.

## Recovery

- `unavailable`, `timeout`, or `throttled`: restore broker quorum or capacity and
  allow the bounded retry policy to operate.
- `authentication` or `authorization`: repair SASL/mTLS credentials or ACLs.
- `invalid_destination`: create the topic or correct its name/template.
- `message_too_large`: reduce the event or raise the broker limit consistently;
  do not split one logical event.
- `protocol_rejection`: repair the topic contract before replaying.

The producer uses `acks=all`, idempotence, bounded retries, and bounded in-flight
requests. Acknowledgment means the broker accepted the record. A replay after relay
restart may still be observed by consumers; Kafka producer idempotence is not an
exactly-once consumer contract.
