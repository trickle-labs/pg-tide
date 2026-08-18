# Runbook: HTTPS Webhook

**Scope:** Outbox to a receiver-controlled HTTP endpoint.
**Profile:** `core`.

## Symptoms

Use `pg-tide status --output json --postgres-url "$PG_TIDE_POSTGRES_URL"` and
inspect bounded codes: `unavailable`, `timeout`, `throttled`, `authentication`,
`authorization`, `tls_verification`, `invalid_destination`, `message_too_large`,
and `protocol_rejection`.

## Diagnosis

1. Run `pg-tide doctor --postgres-url "$PG_TIDE_POSTGRES_URL"`.
2. Confirm the endpoint certificate, hostname, and receiver status without placing
   credentials or request bodies in logs.
3. Check `pg_tide_relay_connector_failures_total{connector="webhook"}` and the
   pipeline's last success and retry state.
4. For a signed endpoint, verify the receiver computes HMAC-SHA256 over the exact
   serialized request body and reads `X-Pg-Tide-Signature`.

## Recovery

- `unavailable`, `timeout`, or `throttled`: restore the endpoint or honor its rate
  limit and let the worker retry.
- `authentication` or `authorization`: repair the receiver credential contract.
- `tls_verification`: repair the certificate chain or hostname; do not disable
  verification in production.
- `invalid_destination`: correct the URL or redirect configuration. Redirects are
  refused and are never followed.
- `protocol_rejection`: repair the receiver's request contract before replaying.

Every batch has a deterministic `Idempotency-Key`; a single message uses its stable
`event_id` and a batch uses a hash of ordered event IDs. The receiver owns the
retention and meaning of that key. A 2xx response is the downstream acknowledgment.
