# Tutorial: Notification Fan-Out

This tutorial demonstrates how to fan out events from a single outbox to multiple notification channels simultaneously — Slack for team alerts, PagerDuty for on-call escalation, email via webhook, and a message archive.

## What You'll Build

```
                          ┌──→ Slack (#alerts channel)
                          │
PostgreSQL  →  pg_tide  ──┼──→ PagerDuty (critical only)
(incidents)               │
                          ├──→ Email webhook (all incidents)
                          │
                          └──→ Kafka (archive + analytics)
```

## Step 1: Create the Outbox

```sql
CREATE EXTENSION pg_tide;
SELECT tide.outbox_create('incident_events');
```

## Step 2: Application Publishes Events

```sql
BEGIN;
INSERT INTO incidents (id, title, severity, team, description)
VALUES ('INC-042', 'Database latency spike', 'critical', 'platform', 'P99 > 5s');

SELECT tide.outbox_publish('incident_events', 'incidents', jsonb_build_object(
    'event_type', 'incident.created',
    'incident_id', 'INC-042',
    'title', 'Database latency spike',
    'severity', 'critical',
    'team', 'platform',
    'description', 'P99 latency exceeded 5s threshold'
));
COMMIT;
```

## Step 3: Configure Multiple Pipelines

### Pipeline 1: Slack (all incidents)

```sql
SELECT tide.relay_set_outbox(
    'incidents-to-slack',
    'incident_events',
    '{
        "sink_type": "slack",
        "webhook_url": "${env:SLACK_ALERTS_WEBHOOK}",
        "channel": "#incidents",
        "transform": {
            "payload": "{ text: join('"'"''"'"', ['"'"'🚨 *'"'"', payload.title, '"'"'* ('"'"', payload.severity, '"'"')\\nTeam: '"'"', payload.team]) }"
        }
    }'::jsonb
);
```

### Pipeline 2: PagerDuty (critical only)

```sql
SELECT tide.relay_set_outbox(
    'incidents-to-pagerduty',
    'incident_events',
    '{
        "sink_type": "pagerduty",
        "routing_key": "${env:PAGERDUTY_ROUTING_KEY}",
        "transform": {
            "filter": "payload.severity == '"'"'critical'"'"'"
        }
    }'::jsonb
);
```

### Pipeline 3: Email webhook (all incidents)

```sql
SELECT tide.relay_set_outbox(
    'incidents-to-email',
    'incident_events',
    '{
        "sink_type": "webhook",
        "url": "https://api.sendgrid.com/v3/mail/send",
        "method": "POST",
        "headers": {
            "Authorization": "Bearer ${env:SENDGRID_API_KEY}"
        },
        "transform": {
            "payload": "{ personalizations: [{ to: [{ email: '"'"'oncall@company.com'"'"' }] }], from: { email: '"'"'alerts@company.com'"'"' }, subject: join('"'"''"'"', ['"'"'['"'"', payload.severity, '"'"'] '"'"', payload.title]), content: [{ type: '"'"'text/plain'"'"', value: payload.description }] }"
        }
    }'::jsonb
);
```

### Pipeline 4: Kafka archive (all incidents)

```sql
SELECT tide.relay_set_outbox(
    'incidents-to-archive',
    'incident_events',
    '{
        "sink_type": "kafka",
        "brokers": "kafka:9092",
        "topic": "incidents-archive",
        "routing": {
            "rules": [
                { "match_field": "severity", "match_value": "critical", "subject": "incidents.critical" },
                { "match_field": "severity", "match_value": "warning", "subject": "incidents.warning" }
            ],
            "default_template": "incidents.info"
        }
    }'::jsonb
);
```

## Step 4: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

All four pipelines run independently. A single outbox event fans out to four destinations.

## How Fan-Out Works

Each pipeline is independent:
- They each have their own consumer position (last relayed outbox ID)
- They process at their own pace
- If one sink is slow or down, others continue unaffected
- Each can have different transforms, filters, and routing rules

## Rate Limiting Notifications

To avoid notification storms, add rate limiting to chatty channels:

```sql
-- Limit Slack to 1 message per second
SELECT tide.relay_set_outbox(
    'incidents-to-slack',
    'incident_events',
    '{
        "sink_type": "slack",
        "webhook_url": "${env:SLACK_ALERTS_WEBHOOK}",
        "rate_limit": {
            "enabled": true,
            "max_messages_per_second": 1,
            "burst_size": 5
        }
    }'::jsonb
);
```

## Further Reading

- [Sinks: Slack](../sinks/slack.md) — Slack configuration
- [Sinks: PagerDuty](../sinks/pagerduty.md) — PagerDuty configuration
- [Feature: Routing](../features/routing.md) — Content-based routing
- [Feature: Transforms](../features/transforms.md) — Message filtering and reshaping
- [Feature: Rate Limiting](../features/rate-limiting.md) — Controlling notification rate
