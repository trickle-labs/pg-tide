# Runbook: PostgreSQL Inbox

**Scope:** `sink_type = "inbox"` and the `pg_outbox` compatibility alias.
**Profile:** `core`.

## Symptoms

Inspect the pipeline with `pg-tide status --output json --postgres-url "$PG_TIDE_POSTGRES_URL"`.
The useful bounded codes are `unavailable`, `timeout`, `authentication`, `authorization`,
`protocol_rejection`, and `unknown`.

## Diagnosis

1. Run `pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"`.
2. Confirm the destination inbox exists and is owned by the configured role:
   `SELECT to_regclass('tide."orders_inbox"');`
3. For a remote destination, verify the secret reference resolves to a PostgreSQL
   URL with certificate verification enabled. Do not paste the URL into logs or tickets.
4. Check `pg_tide_relay_connector_failures_total{connector="inbox"}` and the
   pipeline's `last_error_code`.

## Recovery

- `unavailable` or `timeout`: restore connectivity or failover, then allow the
  worker to retry. The source checkpoint is not advanced before the destination commit.
- `authentication` or `authorization`: repair the role or secret and restart the
  worker. Do not increase retry counts.
- `protocol_rejection`: repair the inbox schema or permissions before replaying.
- `unknown`: inspect redacted relay logs and escalate; do not treat a display string
  as a retry policy.

Repeated delivery is expected after a crash between destination commit and source
checkpoint commit. The inbox unique `event_id` constraint absorbs that duplicate.

## Configuration Contract

Use `sink.inbox` as the destination name. Omit `sink.postgres_url` for the relay's
local database; set it to a secret reference for a remote database. `pg_outbox` is
only a compatibility alias for the remote inbox behavior.
