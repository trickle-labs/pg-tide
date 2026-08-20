# HTTPS webhook

The webhook sink publishes native pg_tide JSON or CloudEvents to an HTTPS
endpoint. It is outbound only and rejects private or link-local destinations by
default.

```json
{
  "source_type": "outbox",
  "source": {"outbox": "orders"},
  "sink_type": "webhook",
  "sink": {
    "url": "https://events.example.test/pg-tide",
    "timeout_secs": 30,
    "ssrf_protection": true,
    "signing_secret": "${env:WEBHOOK_SECRET}"
  },
  "wire_format": "cloudevents"
}
```

HTTPS is required unless `allow_http` is explicitly enabled for local
development. Retries are bounded and successful 2xx responses acknowledge the
batch. Configure HMAC signing when the receiver needs request authentication.
