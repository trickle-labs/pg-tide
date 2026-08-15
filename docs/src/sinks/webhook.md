# HTTP Webhook

An HTTP webhook is one of the most versatile ways to connect pg_tide to the outside world. Rather than requiring a specific message broker or protocol, webhooks deliver your outbox messages as HTTP POST requests to any URL endpoint. This makes webhooks ideal for integrating with third-party APIs, triggering serverless functions, notifying external services, and building custom integrations where the destination simply needs to accept an HTTP request.

When you configure a webhook sink, the pg_tide relay acts as an HTTP client that sends your outbox messages as JSON payloads to the configured URL. The relay handles retries, timeouts, signature verification, and error tracking automatically — all you need to provide is a URL and optionally some authentication headers.

## When to Use This Sink

Choose the webhook sink when:

- **Integrating with third-party APIs** — Most SaaS platforms accept webhook-style HTTP callbacks. If the service you want to notify has a REST API or webhook endpoint, this sink works immediately.
- **Triggering serverless functions** — AWS Lambda (via function URLs or API Gateway), Google Cloud Functions, Azure Functions, and Cloudflare Workers all accept HTTP requests as triggers.
- **Custom microservices** — Your internal services expose HTTP endpoints for receiving events. Webhooks provide a simple, protocol-agnostic delivery mechanism.
- **No broker infrastructure** — You don't want to run Kafka or NATS just to deliver events to a single service. An HTTP webhook requires no message broker infrastructure at all.
- **Prototyping and development** — Webhooks are the fastest way to verify your outbox pipeline works end-to-end, since you can use services like webhook.site or ngrok to inspect deliveries.

Consider a dedicated message queue sink (Kafka, NATS, SQS) if you need fan-out to multiple consumers, message replay, or if the destination processes events asynchronously with its own consumer group model.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-webhook',
    'outbox', 'orders',
    'sink_type', 'webhook',
    'config', '{
        "url": "https://api.example.com/webhooks/orders"
    }'::jsonb
  )
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-webhook',
    'outbox', 'orders',
    'sink_type', 'webhook',
    'config', '{
        "url": "${env:WEBHOOK_URL}",
        "headers": {
            "Authorization": "Bearer ${env:WEBHOOK_TOKEN}",
            "X-Source": "pg-tide"
        },
        "timeout_ms": 10000,
        "signature_secret": "${env:WEBHOOK_SIGNING_SECRET}",
        "signature_scheme": "hmac-sha256",
        "signature_header": "X-Signature-256"
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"webhook"` |
| `url` | string | — | Target URL. Supports template variables: `{stream_table}`, `{op}` |
| `method` | string | `"POST"` | HTTP method |
| `headers` | object | `{}` | Static headers added to every request |
| `timeout_ms` | int | `30000` | Request timeout in milliseconds |
| `signature_secret` | string | `null` | Secret key for HMAC signature generation |
| `signature_scheme` | string | `null` | Signature scheme: `"hmac-sha256"`, `"github"`, `"stripe"`, `"svix"` |
| `signature_header` | string | `"X-Webhook-Signature"` | Header name for the computed signature |
| `batch_size` | int | `1` | Messages per request (1 = individual delivery) |
| `content_type` | string | `"application/json"` | Content-Type header value |

## Authentication

### Bearer Token

The most common webhook authentication pattern:

```json
{
    "sink_type": "webhook",
    "url": "https://api.example.com/events",
    "headers": {
        "Authorization": "Bearer ${env:API_TOKEN}"
    }
}
```

### API Key Header

Some services use a custom header for API key authentication:

```json
{
    "sink_type": "webhook",
    "url": "https://api.example.com/ingest",
    "headers": {
        "X-API-Key": "${env:API_KEY}"
    }
}
```

### HMAC Signature (Webhook Signing)

For secure webhook delivery, pg_tide can compute an HMAC-SHA256 signature of the request body and include it in a header. This allows the receiver to verify that the request genuinely came from pg_tide:

```json
{
    "sink_type": "webhook",
    "url": "https://myservice.example.com/hooks",
    "signature_secret": "${env:SIGNING_SECRET}",
    "signature_scheme": "hmac-sha256",
    "signature_header": "X-Signature-256"
}
```

The receiver computes `HMAC-SHA256(body, secret)` and compares it to the header value using a timing-safe comparison.

## Delivery Guarantees

The webhook sink provides **at-least-once delivery**. A delivery is considered successful when the target endpoint responds with an HTTP status code in the 2xx range. Any other response (4xx, 5xx, timeout, connection error) triggers a retry.

The retry sequence uses exponential backoff: 100ms → 200ms → 400ms → 800ms → ... up to a maximum of 30 seconds between attempts. After exhausting all retries, the message is routed to the dead-letter queue.

**Important:** Your webhook receiver should be idempotent. Because delivery is at-least-once, the same message may be delivered more than once in edge cases (e.g., the relay crashes after the endpoint processed the request but before acknowledging the outbox offset). Use the `dedup_key` included in the payload or headers to detect and skip duplicates.

## URL Routing

The `url` field supports template variables for dynamic routing:

```json
{
    "sink_type": "webhook",
    "url": "https://api.example.com/hooks/{stream_table}/{op}"
}
```

This routes INSERT events from the `orders` outbox to `https://api.example.com/hooks/orders/insert`.

## Complete Example: Notify a Fulfillment Service

### 1. Create the Outbox

```sql
SELECT tide.outbox_create('fulfillment_events', retention_hours => 72);
```

### 2. Publish an Event

```sql
BEGIN;
UPDATE orders SET status = 'ready_to_ship' WHERE id = 'ord-123';

SELECT tide.outbox_publish(
    'fulfillment_events',
    jsonb_build_object(
        'event', 'order.ready_to_ship',
        'order_id', 'ord-123',
        'warehouse', 'us-east-1',
        'items', jsonb_build_array('SKU-001', 'SKU-002')
    ),
    'ord-123-ready'
);
COMMIT;
```

### 3. Configure the Pipeline

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'fulfillment-webhook',
    'outbox', 'fulfillment_events',
    'sink_type', 'webhook',
    'config', '{
        "url": "https://fulfillment.internal/api/v1/events",
        "headers": {
            "Authorization": "Bearer ${env:FULFILLMENT_API_KEY}",
            "Content-Type": "application/json"
        },
        "timeout_ms": 5000,
        "signature_secret": "${env:WEBHOOK_SECRET}",
        "signature_scheme": "hmac-sha256"
    }'::jsonb
  )
);
SELECT tide.relay_enable('fulfillment-webhook');
```

### 4. Start the Relay

```bash
export FULFILLMENT_API_KEY="sk_live_abc123"
export WEBHOOK_SECRET="whsec_xyz789"
pg-tide --postgres-url "postgresql://relay@localhost:5432/mydb"
```

The fulfillment service will receive an HTTP POST with the event payload and a signature header it can verify.

## Troubleshooting

### "Connection refused" or timeout

The target URL is not reachable:
- Verify the URL is correct and the service is running
- Check DNS resolution for the hostname
- Verify network connectivity (firewalls, security groups, VPN)
- Check that `timeout_ms` is sufficient for the endpoint's response time

### HTTP 401 / 403 responses

Authentication failed:
- Verify the `Authorization` header value is correct
- Check that API keys/tokens have not expired
- Ensure the token has permission to access the webhook endpoint

### HTTP 429 (Too Many Requests)

The endpoint is rate-limiting your requests:
- Add a [rate limiter](../features/rate-limiting.md) to your pipeline configuration
- Consider increasing `batch_size` if the endpoint supports batch payloads
- Check the endpoint's rate limit documentation

### Signature verification failures on the receiver

The receiver rejects the webhook signature:
- Ensure the signing secret matches on both sides
- Verify the `signature_scheme` matches what the receiver expects
- Check that the receiver is comparing against the raw request body (not a parsed/re-serialized version)

## Further Reading

- [Webhook Signature Verification](../features/webhook-signatures.md) — Detailed signature scheme documentation
- [Rate Limiting](../features/rate-limiting.md) — Protecting endpoints from overload
- [Dead-Letter Queue](../features/dead-letter-queue.md) — Handling persistent delivery failures
- [Content-Based Routing](../features/routing.md) — Dynamic URL routing based on message content
