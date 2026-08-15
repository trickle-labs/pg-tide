# Slack

Slack is the leading workplace communication platform used by millions of teams. The Slack sink delivers your outbox messages as formatted notifications to Slack channels using incoming webhooks. This enables real-time operational alerts, business event notifications, and workflow triggers delivered directly to the channels where your team collaborates.

## When to Use This Sink

Choose the Slack sink when you want your team to be notified immediately when important business events occur — new high-value orders, system errors, deployment completions, or compliance-relevant actions. The Slack sink formats messages using Slack's Block Kit for rich, readable notifications.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'alerts-to-slack',
    'outbox', 'alerts',
    'sink_type', 'slack',
    'config', '{
        "webhook_url": "${env:SLACK_WEBHOOK_URL}",
        "channel": "#ops-alerts",
        "username": "pg_tide",
        "icon_emoji": ":database:"
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"slack"` |
| `webhook_url` | string | — | Slack incoming webhook URL |
| `channel` | string | `null` | Override channel (must be allowed by webhook config) |
| `username` | string | `"pg_tide"` | Display name for the bot |
| `icon_emoji` | string | `null` | Emoji icon for the bot |
| `template` | string | `null` | Custom Block Kit template for message formatting |

## Rate Limits

Slack imposes rate limits on incoming webhooks (approximately 1 message per second per webhook). For high-volume outboxes, use the [rate limiter](../features/rate-limiting.md) to stay within limits:

```json
{
    "sink_type": "slack",
    "webhook_url": "${env:SLACK_WEBHOOK_URL}",
    "rate_limit": {"messages_per_second": 1}
}
```

## Troubleshooting

- **"Invalid webhook URL"** — Webhook URLs expire if the app is uninstalled; regenerate in Slack app settings
- **"Channel not found"** — The webhook's default channel was deleted; set `channel` explicitly
- **HTTP 429** — Rate limited; add rate limiting to the pipeline configuration

## Further Reading

- [Discord](discord.md) — Similar notification sink for Discord
- [PagerDuty](pagerduty.md) — For incident management alerting
- [HTTP Webhook](webhook.md) — For custom HTTP endpoints
