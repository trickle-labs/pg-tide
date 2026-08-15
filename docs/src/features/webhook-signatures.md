# Feature: Webhook Signatures

When pg_tide sends outgoing webhooks (via the HTTP webhook sink) or receives incoming webhooks (via the webhook receiver source), it can sign and verify messages using HMAC-based signatures. This ensures the recipient can verify the webhook came from pg_tide (outgoing) and that pg_tide can verify webhooks come from a trusted sender (incoming).

## Outgoing Webhook Signatures

When publishing to an HTTP webhook endpoint, pg_tide can sign the request body and include the signature in a header. The receiving service verifies the signature to ensure the request is authentic and hasn't been tampered with.

### Configuration (Outgoing)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'order-notifications',
    'outbox', 'order_events',
    'sink_type', 'webhook',
    'config', '{
        "url": "https://partner.example.com/webhooks/orders",
        "signature": {
            "scheme": "hmac-sha256",
            "secret": "${env:WEBHOOK_SIGNING_SECRET}",
            "header": "X-Signature-256"
        }
    }'::jsonb
  )
);
```

The signature is computed as `HMAC-SHA256(secret, request_body)` and sent as a hex-encoded string in the configured header.

## Incoming Webhook Verification

When receiving webhooks from external services via the [webhook receiver source](../sources/webhook-receiver.md), pg_tide verifies the signature before accepting the message. Requests with invalid signatures are rejected with HTTP 401.

### Configuration (Incoming)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'stripe-events',
    'inbox', 'payment_inbox',
    'source', 'webhook',
    'config', '{
        "listen_addr": "0.0.0.0:8080",
        "path": "/webhooks/stripe",
        "signature_scheme": "stripe",
        "signature_secret": "${env:STRIPE_WEBHOOK_SECRET}"
    }'::jsonb
  )
);
```

## Supported Signature Schemes

### `hmac-sha256` — Standard HMAC

The most common webhook signing scheme. Computes HMAC-SHA256 of the request body using a shared secret.

- **Header:** Configurable (default: `X-Signature-256`)
- **Format:** `sha256=<hex-encoded-hmac>`
- **Verification:** Recompute HMAC and compare (constant-time)

### `github` — GitHub Webhooks

GitHub's webhook signature format using `X-Hub-Signature-256`.

- **Header:** `X-Hub-Signature-256`
- **Format:** `sha256=<hex-encoded-hmac>`
- **Secret:** Your GitHub webhook secret

### `stripe` — Stripe Webhooks

Stripe's signature includes a timestamp to prevent replay attacks.

- **Header:** `Stripe-Signature`
- **Format:** `t=<timestamp>,v1=<hmac>`
- **Verification:** HMAC computed over `timestamp.body`
- **Replay protection:** Rejects signatures with timestamps too far in the past

### `svix` — Svix Webhook Platform

[Svix](https://www.svix.com/) is a webhook delivery platform. pg_tide can verify Svix-signed webhooks.

- **Header:** `svix-signature`
- **Format:** Svix-specific signature scheme
- **Includes:** Message ID, timestamp, and signature

## Security Considerations

- **Always use HTTPS** for webhook endpoints. Signatures prove authenticity but don't encrypt the payload.
- **Rotate secrets periodically.** When rotating, you can temporarily accept both old and new secrets.
- **Use environment variables** for secrets (never hardcode): `${env:SECRET_NAME}`
- **Reject stale timestamps** (Stripe scheme does this automatically) to prevent replay attacks.

## Further Reading

- [Sinks: HTTP Webhook](../sinks/webhook.md) — Outgoing webhook configuration
- [Sources: Webhook Receiver](../sources/webhook-receiver.md) — Incoming webhook server
- [Security Guide](../guides/security-guide.md) — Broader security practices
