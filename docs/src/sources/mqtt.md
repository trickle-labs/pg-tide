# Source: MQTT v5

The MQTT source subscribes to MQTT topics and delivers messages into a pg_tide inbox. This is ideal for ingesting IoT telemetry, sensor data, and device events into PostgreSQL for processing and analytics.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'telemetry-from-devices',
    'inbox', 'device_telemetry',
    'source', 'mqtt',
    'config', '{
        "url": "mqtts://${env:MQTT_BROKER}:8883",
        "topic": "devices/+/telemetry",
        "qos": 1,
        "client_id": "pg-tide-ingest-01",
        "username": "${env:MQTT_USER}",
        "password": "${env:MQTT_PASS}",
        "tls_enabled": true
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"mqtt"` |
| `url` | string | — | MQTT broker URL |
| `topic` | string | — | MQTT topic filter (supports `+` and `#` wildcards) |
| `qos` | int | `1` | Quality of Service level |
| `client_id` | string | auto | MQTT client ID |
| `username` | string | `null` | Auth username |
| `password` | string | `null` | Auth password |
| `tls_enabled` | bool | `false` | Enable TLS |
| `clean_start` | bool | `false` | Clean session on connect |

## Delivery Guarantees

With QoS 1, the MQTT broker guarantees at-least-once delivery to the relay. The inbox's deduplication handles any duplicates that arrive. With `clean_start: false`, the broker retains subscriptions and queues messages while the relay is offline.

## Further Reading

- [Sinks: MQTT](../sinks/mqtt.md) — Publishing to MQTT
