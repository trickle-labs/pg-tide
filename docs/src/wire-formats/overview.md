# Wire Formats

A wire format defines the serialized message envelope sent by the relay.
v0.49.0 supports native pg_tide JSON and CloudEvents for outbound delivery.

| Format | Use case |
|---|---|
| [Native](native.md) | Default pg_tide envelope with stable event identity |
| CloudEvents | Standard event metadata for HTTP and broker consumers |

Select a format with the pipeline's `wire_format` key. If omitted, the relay
uses `native`.

```json
{
  "wire_format": "cloudevents",
  "sink_type": "kafka"
}
```

Both formats preserve the outbox operation, subject, payload, and deduplication
identity. Delivery remains at-least-once.
