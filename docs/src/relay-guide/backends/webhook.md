# Webhook Backend

HTTP webhook integration for push-based event delivery and reception.

---

## Forward (Outbox → HTTP Webhook)

Delivers outbox messages as HTTP POST requests to a configured URL.

```sql
SELECT tide.relay_set_outbox('events-webhook', 'events', 'webhook',
  jsonb_build_object(
    'url', 'https://api.example.com/webhooks/events',
    'timeout_ms', 5000
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Webhook endpoint URL |
| `timeout_ms` | No | `30000` | Request timeout in milliseconds |
| `headers` | No | `{}` | Additional HTTP headers (JSONB object) |
| `method` | No | `POST` | HTTP method |
| `retry_codes` | No | `[429, 500, 502, 503, 504]` | Status codes that trigger retry |

### Request Format

```http
POST /webhooks/events HTTP/1.1
Content-Type: application/json
X-PgTide-Dedup-Key: orders:42:0
X-PgTide-Event-Type: order.created

{"order_id": 42, "total": 99.99}
```

---

## Reverse (HTTP Webhook → Inbox)

Exposes an HTTP endpoint that accepts incoming webhook deliveries and writes them to an inbox.

```sql
SELECT tide.relay_set_inbox('webhook-receiver', 'incoming-hooks',
  jsonb_build_object(
    'port', 8080,
    'path', '/webhooks/incoming'
  ),
  p_source := 'webhook'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `port` | No | `8080` | Port to listen on |
| `path` | No | `/` | URL path to accept requests on |
| `auth_header` | No | — | Expected Authorization header value |

### Dedup Key Extraction

The webhook source uses these headers (in priority order) as the dedup key:

1. `X-Request-ID`
2. `X-Idempotency-Key`
3. `X-Webhook-ID`
4. Auto-generated UUID (fallback)

---

## Cargo Feature

Enabled by default:

```bash
cargo build --package pg-tide-relay  # webhook included
```
