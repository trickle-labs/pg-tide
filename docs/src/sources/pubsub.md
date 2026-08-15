# Source: Google Cloud Pub/Sub

The Pub/Sub source subscribes to a Google Cloud Pub/Sub subscription and delivers messages into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'events-from-pubsub',
    'inbox', 'incoming_events',
    'source', 'pubsub',
    'config', '{
        "project_id": "${env:GCP_PROJECT_ID}",
        "subscription": "pg-tide-sub",
        "credentials_json": "${file:/etc/gcp/sa.json}",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"pubsub"` |
| `project_id` | string | — | GCP project ID |
| `subscription` | string | — | Pub/Sub subscription name |
| `credentials_json` | string | `null` | Service account JSON |
| `batch_size` | int | `100` | Messages per pull |
| `ack_deadline_secs` | int | `30` | Acknowledgment deadline |

## Delivery Guarantees

Messages are acknowledged only after inbox insertion. The inbox deduplicates using the Pub/Sub message ID. Unacknowledged messages are redelivered after the ack deadline expires.

## Further Reading

- [Sinks: Pub/Sub](../sinks/pubsub.md) — Publishing to Pub/Sub
