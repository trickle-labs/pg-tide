# Error Codes

Errors raised by pg_tide SQL functions and the relay binary.

---

## Extension Errors

| Error | Raised by | Description |
|-------|-----------|-------------|
| `outbox already exists: {name}` | `outbox_create` | An outbox with this name already exists |
| `outbox not found: {name}` | `outbox_publish`, `outbox_drop`, `outbox_status`, `outbox_enable`, `outbox_disable` | No outbox with this name |
| `inbox already exists: {name}` | `inbox_create` | An inbox with this name already exists |
| `inbox not found: {name}` | `inbox_drop`, `inbox_mark_processed`, `inbox_mark_failed`, `inbox_status` | No inbox with this name |
| `relay pipeline not found: {name}` | `relay_enable`, `relay_disable`, `relay_delete`, `relay_get_config` | No pipeline with this name |
| `invalid argument: {details}` | Various | Invalid parameter value |
| `SPI error: {details}` | Various | Internal database error |

---

## Relay Errors

| Error | Category | Description |
|-------|----------|-------------|
| `postgres error` | Connection | Database communication failure |
| `postgres connection failed` | Connection | Initial connection could not be established |
| `config error` | Configuration | Invalid TOML or missing required field |
| `invalid config for pipeline` | Configuration | Pipeline-specific config validation failure |
| `pipeline not found` | Configuration | Referenced pipeline doesn't exist in catalog |
| `missing required config key` | Configuration | A required sink/source config key is absent |
| `unsupported outbox payload version` | Payload | Message format version mismatch |
| `payload decode error` | Payload | Cannot deserialize message payload |
| `sink publish error` | Delivery | Sink rejected or timed out on delivery |
| `sink unhealthy` | Delivery | Sink is not accepting connections |
| `source poll error` | Ingestion | Source read failure |
| `channel closed` | Internal | Internal communication channel dropped |

---

## Handling Errors

### In SQL

All pg_tide functions raise PostgreSQL `ERROR` level exceptions on failure. Use standard PL/pgSQL exception handling:

```sql
DO $$
BEGIN
  PERFORM tide.outbox_publish('maybe-missing', '{}'::jsonb, '{}'::jsonb);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'publish failed: %', SQLERRM;
END $$;
```

### In the Relay

The relay logs all errors with structured fields and retries transient failures automatically. Monitor via:

- Prometheus: `pg_tide_relay_publish_errors_total`
- Health endpoint: `GET /health`
- Logs: structured JSON with `level=error`
