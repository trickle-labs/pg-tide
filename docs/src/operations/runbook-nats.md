# Runbook: NATS JetStream

**Scope:** Outbox to a configured JetStream stream.
**Profile:** `core`.

## Symptoms

Use `pg-tide status --output json --postgres-url "$PG_TIDE_POSTGRES_URL"` and
look for `unavailable`, `timeout`, `throttled`, `authentication`, `authorization`,
`invalid_destination`, or `protocol_rejection`.

## Diagnosis

1. Run `pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"`.
2. Verify the configured stream exists and its subject filter includes the rendered
   subject. A missing stream is `invalid_destination`, not a transient outage.
3. Check `pg_tide_relay_connector_failures_total{connector="nats"}` without adding
   URLs, subjects, or upstream exception text as labels.
4. Confirm NATS credentials and TLS trust configuration from the secret reference.

## Recovery

- `unavailable`, `timeout`, or `throttled`: restore the broker or capacity and let
  the worker retry.
- `authentication` or `authorization`: repair credentials or permissions.
- `invalid_destination` or `protocol_rejection`: create or repair the JetStream
  stream and subject policy before retrying.

The sink returns success only after a JetStream publish acknowledgment. `Nats-Msg-Id`
is the stable event identity; JetStream suppresses duplicates only inside the
stream's configured duplicate window. A restart can therefore produce a duplicate
after that window expires.

## Safety

Do not advance offsets manually or delete the stream to clear a retry. Preserve the
source batch until the downstream acknowledgment and checkpoint transition are
visible in `status`.
