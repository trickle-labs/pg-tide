# Source: HTTP Webhook Receiver

The Webhook Receiver source starts an HTTP server within the relay that accepts incoming webhook POST requests and delivers them into a pg_tide inbox. This turns pg_tide into a webhook endpoint — external services (Stripe, GitHub, Shopify, or any custom service) can push events directly to your relay, which stores them reliably in your PostgreSQL inbox for processing.

## When to Use This Source

Use the webhook receiver when external services push events to you via HTTP (payment notifications from Stripe, repository events from GitHub, order updates from Shopify), when you want to decouple webhook reception from processing (accept the webhook immediately, process later within a transaction), or when you need webhook signature verification and idempotent deduplication built in.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'stripe-webhooks',
    'payment_events',
    '{
        "source_type": "webhook",
        "listen_addr": "0.0.0.0:8080",
        "path": "/webhooks/stripe",
        "signature_scheme": "stripe",
        "signature_secret": "${env:STRIPE_WEBHOOK_SECRET}",
        "idempotency_header": "Stripe-Idempotency-Key"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"webhook"` |
| `listen_addr` | string | `"0.0.0.0:8080"` | Address and port for the HTTP server |
| `path` | string | `"/"` | URL path to accept webhooks on |
| `signature_scheme` | string | `null` | Verification scheme: `"hmac-sha256"`, `"github"`, `"stripe"`, `"svix"` |
| `signature_secret` | string | `null` | Secret key for signature verification |
| `idempotency_header` | string | `null` | Header containing the dedup key |
| `max_body_size` | int | `1048576` | Maximum request body size (bytes) |

## Signature Verification

The webhook receiver can verify the authenticity of incoming requests using various signature schemes:

- **`hmac-sha256`** — Standard HMAC-SHA256 signature in a configurable header
- **`github`** — GitHub's `X-Hub-Signature-256` header format
- **`stripe`** — Stripe's `Stripe-Signature` header with timestamp verification
- **`svix`** — Svix webhook signature format

Requests with invalid signatures are rejected with HTTP 401, protecting your inbox from spoofed events.

## Response Codes

The webhook receiver responds to senders with:
- **200 OK** — Message accepted and written to inbox
- **401 Unauthorized** — Signature verification failed
- **409 Conflict** — Duplicate message (already in inbox, idempotent success)
- **413 Payload Too Large** — Body exceeds `max_body_size`
- **500 Internal Server Error** — Database write failed

## Troubleshooting

- **"Connection refused" from sender** — Check `listen_addr` port and firewall rules
- **HTTP 401 from all requests** — Verify `signature_secret` matches the sender's configuration
- **Missing events** — Check that `path` matches what the sender is configured to POST to

## Further Reading

- [Webhook Signatures Feature](../features/webhook-signatures.md) — Detailed signature scheme documentation
- [Sinks: HTTP Webhook](../sinks/webhook.md) — Sending webhooks (forward direction)
