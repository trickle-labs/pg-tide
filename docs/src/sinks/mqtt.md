# MQTT v5

MQTT (Message Queuing Telemetry Transport) is a lightweight publish/subscribe messaging protocol designed for constrained devices and low-bandwidth, high-latency networks. It has become the de facto standard for IoT (Internet of Things) communication, connecting everything from industrial sensors and smart home devices to connected vehicles and healthcare equipment. When pg_tide publishes to an MQTT broker, your outbox messages are delivered to MQTT topics where IoT devices, edge gateways, and backend services can subscribe.

MQTT v5 (the version pg_tide supports) adds features like message expiry, topic aliases, shared subscriptions, and user properties that make it suitable for more sophisticated use cases beyond basic IoT telemetry.

## When to Use This Sink

Choose the MQTT sink when you need to deliver events to IoT devices or edge computing infrastructure, when you are integrating with industrial IoT platforms (HiveMQ, EMQX, AWS IoT Core, Azure IoT Hub), or when you need the lightweight protocol overhead that MQTT provides for bandwidth-constrained environments. MQTT's Quality of Service (QoS) levels let you choose between maximum performance (QoS 0), guaranteed delivery (QoS 1), and exactly-once delivery (QoS 2).

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'telemetry-to-mqtt',
    'outbox', 'device_commands',
    'sink_type', 'mqtt',
    'config', '{
        "url": "mqtts://${env:MQTT_BROKER}:8883",
        "topic": "devices/{stream_table}/commands",
        "qos": 1,
        "username": "${env:MQTT_USER}",
        "password": "${env:MQTT_PASS}",
        "client_id": "pg-tide-relay-01",
        "tls_enabled": true
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"mqtt"` |
| `url` | string | — | MQTT broker URL (`mqtt://` or `mqtts://`) |
| `topic` | string | — | MQTT topic template. Supports `{stream_table}`, `{op}` |
| `qos` | int | `1` | Quality of Service: 0 (at-most-once), 1 (at-least-once), 2 (exactly-once) |
| `username` | string | `null` | Authentication username |
| `password` | string | `null` | Authentication password |
| `client_id` | string | auto-generated | MQTT client identifier |
| `tls_enabled` | bool | `false` | Enable TLS |
| `tls_ca_cert` | string | `null` | CA certificate path |
| `retain` | bool | `false` | Set retain flag on published messages |
| `clean_start` | bool | `true` | Start with a clean session |
| `batch_size` | int | `1` | Messages per batch (most MQTT use cases are individual) |

## Delivery Guarantees

Delivery guarantees depend on the QoS level:

- **QoS 0 (At-most-once):** Fire-and-forget. Fastest but messages can be lost.
- **QoS 1 (At-least-once):** Broker acknowledges receipt. Messages may be delivered more than once. **Recommended for most use cases.**
- **QoS 2 (Exactly-once):** Four-phase handshake ensures each message is delivered exactly once. Highest overhead but strongest guarantee.

## Topic Hierarchy

MQTT topics use `/` as a separator (unlike NATS which uses `.`). pg_tide's template variables work naturally with MQTT's topic model:

```
devices/sensors/temperature   → sensor readings
commands/device-01/reboot    → device commands
events/orders/created        → business events
```

## Troubleshooting

- **"Connection refused"** — Check broker URL, port (1883 for TCP, 8883 for TLS), and firewall rules
- **"Not authorized"** — Verify username/password or client certificate
- **"Client ID already in use"** — Each relay instance needs a unique `client_id`
- **Messages not received by subscribers** — Check topic name matches subscriber's subscription pattern (MQTT uses `+` and `#` wildcards)

## Further Reading

- [Sources: MQTT](../sources/mqtt.md) — Subscribing to MQTT topics into pg_tide inbox
- [Rate Limiting](../features/rate-limiting.md) — Protecting IoT brokers from overload
