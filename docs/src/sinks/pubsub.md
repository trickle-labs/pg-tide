# Google Cloud Pub/Sub

Google Cloud Pub/Sub is a fully managed, globally distributed messaging service built for reliability at scale. It decouples services by allowing publishers to send messages to topics without knowing who will receive them, and subscribers to receive messages without knowing who sent them. When pg_tide publishes to Pub/Sub, your outbox messages become available to any GCP service or application subscribed to the topic — from Cloud Functions and Cloud Run to Dataflow and BigQuery subscriptions.

Pub/Sub handles automatic scaling, message retention (up to 31 days), and global message routing without any infrastructure management. It is the natural choice for GCP-native architectures.

## When to Use This Sink

Choose Pub/Sub when your infrastructure runs on Google Cloud Platform, when you need global message distribution across regions, or when you want deep integration with GCP services like Cloud Functions (event triggers), Dataflow (stream processing), and BigQuery (direct subscriptions for analytics). Pub/Sub supports ordering within ordering keys and scales to millions of messages per second without provisioning.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-pubsub',
    'events',
    'pubsub-relay',
    '{
        "sink_type": "pubsub",
        "project_id": "my-gcp-project",
        "topic": "outbox-events"
    }'::jsonb
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-pubsub',
    'events',
    'pubsub-relay',
    '{
        "sink_type": "pubsub",
        "project_id": "${env:GCP_PROJECT_ID}",
        "topic": "events-{stream_table}",
        "credentials_json": "${file:/etc/gcp/service-account.json}",
        "ordering_key": "{dedup_key}",
        "batch_size": 100
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"pubsub"` |
| `project_id` | string | — | GCP project ID |
| `topic` | string | — | Pub/Sub topic name. Supports templates |
| `credentials_json` | string | `null` | Service account JSON (falls back to Application Default Credentials) |
| `ordering_key` | string | `null` | Ordering key template for ordered delivery |
| `batch_size` | int | `100` | Messages per publish request |
| `attributes` | object | `null` | Custom message attributes |

## Authentication

On GCP (GKE, Cloud Run, Compute Engine), use Workload Identity or the default service account — no explicit credentials needed. For external deployments, provide a service account JSON key file via `credentials_json`.

## Delivery Guarantees

Pub/Sub provides at-least-once delivery by default. With ordering keys configured, messages sharing the same ordering key are delivered in publish order to subscribers. Combined with subscriber-side deduplication (using the message ID or the `dedup_key` attribute), you can achieve effectively exactly-once processing.

## Complete Example

```sql
SELECT tide.outbox_publish(
    'analytics_events',
    '{"event": "page_view", "user_id": "u-456", "page": "/checkout"}'::jsonb,
    'pv-u456-checkout-1715000000'
);
```

Messages appear in the Pub/Sub topic and can be consumed by any subscriber:

```bash
gcloud pubsub subscriptions pull my-subscription --auto-ack --limit=5
```

## Troubleshooting

- **"Permission denied"** — Service account needs `roles/pubsub.publisher` on the topic
- **"Topic not found"** — Create the topic first: `gcloud pubsub topics create outbox-events`
- **"Ordering key too long"** — Ordering keys must be ≤ 1024 bytes

## Further Reading

- [Sources: Pub/Sub](../sources/pubsub.md) — Subscribing to Pub/Sub messages into pg_tide inbox
- [BigQuery](bigquery.md) — For direct analytics sink on GCP
