# Feature: Dry-Run and Replay

Dry-run mode lets you test a pipeline configuration without actually publishing messages to the sink. Replay mode lets you reprocess a range of messages from your outbox — useful for backfilling a new consumer, recovering from a sink failure, or reprocessing after fixing a bug.

## Dry-Run Mode

In dry-run mode, the relay performs every step of the pipeline (polling, transforms, routing) but skips the actual publish to the sink. Instead, it logs what would have been sent. This is invaluable for:

- **Validating configuration** before going live
- **Testing transforms** to see which messages pass the filter and what the output looks like
- **Verifying routing** to confirm messages end up on the expected topics
- **Capacity planning** to understand message volume and size before connecting a real sink

### Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-test',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "orders",
        "dry_run": true
    }'::jsonb
  )
);
```

Or via TOML:

```toml
[[pipelines]]
name = "orders-test"
dry_run = true
```

### What Gets Logged

In dry-run mode, each batch produces log output like:

```
INFO [orders-test] dry-run: would publish 5 messages to "orders"
INFO [orders-test] dry-run: msg[0] key="ORD-001" size=342 bytes
INFO [orders-test] dry-run: msg[1] key="ORD-002" size=287 bytes
...
```

Messages are still acknowledged from the source after logging — so the pipeline advances through the outbox even in dry-run mode. This means you can run dry-run temporarily to see what's flowing, then disable it to start real delivery from the current position.

## Replay Mode

Replay mode reprocesses a range of outbox messages, typically to backfill a new consumer or recover from a failure. You specify a starting offset and optionally an ending offset, and the relay processes only messages within that range.

### Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-backfill',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "orders-v2",
        "replay": {
            "from_offset": 1000,
            "to_offset": 5000
        }
    }'::jsonb
  )
);
```

### Replay Behavior

- Messages outside the offset range are skipped (not published, not acknowledged)
- When the range is exhausted, the pipeline exits cleanly
- Replay pipelines can run alongside live pipelines — they don't interfere
- Combine with transforms to replay with different filtering or reshaping

### Use Cases

**Backfilling a new consumer:**
When you add a new Kafka consumer that needs historical data, create a replay pipeline targeting the new topic. Once the replay completes, switch to a live pipeline for ongoing messages.

**Recovering from sink failure:**
If your sink was down and messages went to the DLQ, you can replay the affected range instead of retrying individual DLQ entries.

**Reprocessing after a bug fix:**
If a transform had a bug that produced incorrect output, fix the transform and replay the affected range to the same (or a new) destination.

## Combining Dry-Run and Replay

You can use both together to preview what a replay would produce without actually sending anything:

```json
{
  "dry_run": true,
  "replay": {
    "from_offset": 1000,
    "to_offset": 2000
  }
}
```

This is useful for estimating replay volume and validating transform behavior on historical messages.

## Further Reading

- [Dead Letter Queue](dead-letter-queue.md) — Alternative recovery path for individual messages
- [Transforms](transforms.md) — Reshape messages during replay
