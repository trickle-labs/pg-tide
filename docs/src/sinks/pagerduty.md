# PagerDuty

PagerDuty is an incident management platform that helps teams detect, respond to, and resolve operational issues. The PagerDuty sink delivers your outbox messages as Events API v2 events, which can trigger incidents, route to on-call responders, and integrate with PagerDuty's full incident lifecycle management. This enables your PostgreSQL events to directly drive incident response — when a critical business condition is detected, pg_tide can automatically page the right team.

## When to Use This Sink

Choose PagerDuty when critical business events in your PostgreSQL database should trigger immediate human response. Examples include: payment processing failures that need urgent attention, security-relevant events (unauthorized access attempts), SLA violations, or infrastructure conditions detected through database monitoring.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'critical-alerts',
    'outbox', 'incidents',
    'sink_type', 'pagerduty',
    'config', '{
        "routing_key": "${env:PAGERDUTY_ROUTING_KEY}",
        "severity": "critical",
        "source": "pg-tide",
        "component": "payment-service",
        "dedup_key_field": "dedup_key"
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"pagerduty"` |
| `routing_key` | string | — | PagerDuty Events API v2 routing (integration) key |
| `severity` | string | `"error"` | Event severity: `"critical"`, `"error"`, `"warning"`, `"info"` |
| `source` | string | `"pg-tide"` | Source of the event |
| `component` | string | `null` | Component or service name |
| `dedup_key_field` | string | `"dedup_key"` | Field to use as PagerDuty dedup key (prevents duplicate incidents) |
| `event_action` | string | `"trigger"` | Action: `"trigger"`, `"acknowledge"`, `"resolve"` |

## Deduplication

PagerDuty uses deduplication keys to group related events into a single incident. By default, pg_tide uses the outbox message's `dedup_key` as PagerDuty's dedup key. This means publishing multiple events with the same dedup_key updates the existing incident rather than creating duplicate pages.

## Auto-Resolve

You can configure separate pipelines for triggering and resolving incidents:

```sql
-- Trigger incidents from error events
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'trigger-alerts',
    'outbox', 'errors',
    'sink_type', 'pagerduty',
    'config', '{ "routing_key": "${env:PD_KEY}", "event_action": "trigger"}'::jsonb
  )
);

-- Resolve incidents from recovery events
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'resolve-alerts',
    'outbox', 'recoveries',
    'sink_type', 'pagerduty',
    'config', '{ "routing_key": "${env:PD_KEY}", "event_action": "resolve"}'::jsonb
  )
);
```

## Troubleshooting

- **"Invalid routing key"** — Verify the routing key from your PagerDuty integration settings
- **Duplicate incidents** — Check that `dedup_key_field` maps to a consistent identifier
- **No incidents triggered** — Verify the service has an active escalation policy

## Further Reading

- [Slack](slack.md) — For less urgent team notifications
- [HTTP Webhook](webhook.md) — For custom alerting endpoints
- [Circuit Breaker](../features/circuit-breaker.md) — Preventing alert storms during outages
