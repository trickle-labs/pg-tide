# Feature: Configuration Hot-Reload

pg_tide supports hot-reloading pipeline configurations without restarting the relay process. When you add, modify, or disable a pipeline in the PostgreSQL catalog, the relay detects the change and reconciles — starting new pipelines, stopping removed ones, and updating modified configurations in place.

## How It Works

The relay discovers configuration changes through two mechanisms:

1. **LISTEN/NOTIFY** — Immediate notification when catalog tables change
2. **Periodic polling** — Rediscovers pipelines every `discovery_interval_secs` (fallback)

When a change is detected, the coordinator compares the new pipeline set against the currently running pipelines:

- **New pipeline** → Acquire advisory lock, spawn worker task
- **Removed pipeline** → Signal worker to stop, release advisory lock
- **Modified pipeline** → Stop old worker, start new one with updated config
- **Disabled pipeline** → Same as removed (worker stopped, lock released)

## Triggering a Reload

### Automatic (via LISTEN/NOTIFY)

The relay listens on the `tide_relay_config` PostgreSQL notification channel. When you call any `tide.relay_set_*` function, a notification is emitted automatically:

```sql
-- This triggers immediate reload
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-pipeline',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{ "brokers": "kafka:9092", "topic": "orders"}'::jsonb
  )
);
```

### Periodic Discovery

Even without NOTIFY (e.g., if the relay reconnects after a network partition), the coordinator polls for changes every `discovery_interval_secs`:

```toml
# Default: 30 seconds
discovery_interval_secs = 30
```

## What Can Be Changed Without Restart

| Change | Hot-Reload? | Notes |
|--------|:-----------:|-------|
| Add new pipeline | ✓ | Started within seconds |
| Remove pipeline | ✓ | Gracefully drained and stopped |
| Change sink type | ✓ | Worker restarted with new sink |
| Change sink config (URL, topic) | ✓ | Worker restarted |
| Change transforms/routing | ✓ | Worker restarted |
| Enable/disable pipeline | ✓ | Started or stopped |
| Change relay process config | ✗ | Requires restart |
| Change `metrics_addr` | ✗ | Requires restart |
| Change `postgres_url` | ✗ | Requires restart |

## Graceful Pipeline Transitions

When a pipeline configuration changes, the existing worker is drained before the new one starts:

1. Worker receives stop signal
2. Current batch completes (in-flight messages finish)
3. Source acknowledgment completes
4. Worker task exits
5. New worker spawns with updated config
6. New worker begins polling

This preserves the source checkpoint during reconfiguration. An in-flight
batch may be redelivered after a restart or ownership transfer; stable
deduplication identities make that duplicate observable and safe where the
destination supports deduplication.

## Configuration

The discovery mechanism itself is configured at the process level:

```toml
# How often to poll for pipeline changes (fallback)
discovery_interval_secs = 30
```

Or via CLI:

```bash
pg-tide --discovery-interval 30
```

## Further Reading

- [Relay Configuration](../relay-guide/configuration.md) — Full process-level configuration
- [HA Coordination](ha-coordination.md) — How multiple relays handle config changes
