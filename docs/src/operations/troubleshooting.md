# Troubleshooting

Common issues and their solutions.

---

## Relay Won't Start

### "error: --postgres-url is required"

The relay needs a PostgreSQL connection. Provide it via CLI flag or environment variable:

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
# or
export PGTRICKLE_RELAY_POSTGRES_URL="postgres://..."
pg-tide
```

### "PostgreSQL connection failed, retrying"

The relay cannot reach PostgreSQL. Check:

- Is PostgreSQL running and accepting connections?
- Is the connection string correct?
- Are firewall rules allowing the connection?
- Is the database user granted `CONNECT` privilege?

The relay retries with exponential backoff indefinitely.

---

## No Messages Being Delivered

### Check pending messages exist

```sql
SELECT * FROM tide.outbox_pending;
```

If empty, no messages have been published yet.

### Check pipeline is enabled

```sql
SELECT tide.relay_list_configs();
```

Ensure the pipeline shows `"enabled": true`.

### Check consumer group exists

Forward pipelines need a consumer group:

```sql
SELECT * FROM tide.tide_consumer_groups;
```

### Check relay owns the pipeline

If another relay instance holds the advisory lock, this instance won't process the pipeline. Check logs for `"acquired lock"` messages.

---

## Duplicate Messages

### In the outbox

This is normal — your application may publish the same logical event multiple times. Consider adding a deterministic dedup key in the headers.

### In the inbox

If the inbox receives duplicates, the `UNIQUE(event_id)` constraint should prevent it. If duplicates appear, check:

- Is the dedup key truly unique per logical event?
- Was the inbox table created with the UNIQUE constraint?

---

## High Consumer Lag

```sql
SELECT * FROM tide.consumer_lag WHERE lag > 1000;
```

Possible causes:

- **Sink is slow** — check sink latency and error rate
- **Relay is overwhelmed** — increase `batch_size` or deploy more relay instances
- **Relay is down** — check health endpoint and process status
- **Advisory lock not acquired** — another relay or stale process holds the lock

---

## Extension Errors

### "outbox already exists"

Use `p_if_not_exists` or check before creating:

```sql
SELECT tide.create_consumer_group('my-group', 'events', p_if_not_exists := true);
```

### "outbox not found"

The named outbox doesn't exist. Create it first:

```sql
SELECT tide.outbox_create('my-outbox');
```

---

## Useful Diagnostic Queries

```sql
-- All outbox config
SELECT * FROM tide.tide_outbox_config;

-- All inbox config
SELECT * FROM tide.tide_inbox_config;

-- Active relay pipelines
SELECT * FROM tide.relay_outbox_config WHERE enabled;
SELECT * FROM tide.relay_inbox_config WHERE enabled;

-- Advisory locks held (relay pipelines)
SELECT * FROM pg_locks WHERE locktype = 'advisory';

-- Relay offset tracking
SELECT * FROM tide.relay_consumer_offsets;
```
