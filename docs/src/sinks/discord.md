# Discord

Discord is a popular communication platform originally designed for gaming communities but now widely used for developer communities, open-source projects, and team collaboration. The Discord sink delivers your outbox messages as webhook notifications to Discord channels, using Discord's embed format for rich, formatted messages.

## When to Use This Sink

Choose the Discord sink for community notifications (open-source project updates, release announcements), developer team alerts, or any scenario where your audience lives in Discord rather than Slack.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-discord',
    'community_events',
    'discord-relay',
    '{
        "sink_type": "discord",
        "webhook_url": "${env:DISCORD_WEBHOOK_URL}",
        "username": "pg_tide Bot",
        "avatar_url": "https://example.com/bot-avatar.png"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"discord"` |
| `webhook_url` | string | — | Discord webhook URL |
| `username` | string | `null` | Override bot display name |
| `avatar_url` | string | `null` | Override bot avatar image URL |
| `template` | string | `null` | Custom embed template |

## Rate Limits

Discord webhooks are limited to approximately 30 requests per minute per channel. Use the [rate limiter](../features/rate-limiting.md) to avoid HTTP 429 responses.

## Troubleshooting

- **"Unknown Webhook"** — The webhook was deleted from Discord; create a new one in channel settings
- **HTTP 429** — Rate limited; reduce send rate with rate limiting

## Further Reading

- [Slack](slack.md) — Team communication alternative
- [HTTP Webhook](webhook.md) — Generic HTTP delivery
