# PgBouncer Integration

[PgBouncer](https://www.pgbouncer.org) is the standard PostgreSQL connection
pooler. pg_tide's relay can run comfortably behind PgBouncer with a couple of
small configuration choices.

## Which pool mode to use

| Pool mode | Advisory locks | LISTEN/NOTIFY | pg_tide compatibility |
|-----------|---------------|--------------|----------------------|
| Session   | ✅ Work        | ✅ Work       | ✅ Recommended       |
| Transaction | ❌ Released between statements | ❌ Do not work | ⚠️ Not recommended for the relay |
| Statement | ❌ | ❌ | ❌ Do not use |

**Use session mode for the relay connection.** The relay holds PostgreSQL
advisory locks for pipeline ownership and uses `LISTEN tide_relay_config` for
hot-reload — both require a persistent session.

## Recommended setup

### 1. Dedicate a small pool for the relay

Give the relay its own PgBouncer database entry so it always gets a session-mode
connection, even if the main application pool uses transaction mode:

```ini
# pgbouncer.ini
[databases]
myapp        = host=127.0.0.1 port=5432 dbname=myapp  pool_mode=transaction
myapp_relay  = host=127.0.0.1 port=5432 dbname=myapp  pool_mode=session

[pgbouncer]
listen_port = 6432
listen_addr = 127.0.0.1
auth_type = scram-sha-256
```

### 2. Point the relay at the session pool

```bash
pg-tide \
  --postgres-url "postgres://relay_user:pass@127.0.0.1:6432/myapp_relay"
```

### 3. Application connections use transaction mode

Your application can still use the standard `myapp` pool with transaction mode —
only the relay needs session mode.

## Connection count

The relay opens one persistent PostgreSQL connection per `pg-tide` process
(plus one short-lived connection for the LISTEN/NOTIFY channel). For a typical
deployment:

- 1 relay process → 2 PgBouncer connections → 2 PostgreSQL server connections.
- 3 relay replicas (HA) → 6 PgBouncer connections → at most 3 active
  PostgreSQL connections (only the primary lock-holder does real work).

Set `max_client_conn` and `default_pool_size` in PgBouncer to accommodate this.

## Health check

PgBouncer's `server_check_query` pings idle connections. pg_tide's relay sends
its own heartbeat updates (`UPDATE tide.tide_consumer_offsets SET last_heartbeat = now()`),
so no additional check query is needed.

## TLS

If PgBouncer terminates TLS from the relay, ensure the relay's `--postgres-url`
includes `sslmode=require`:

```
postgres://relay_user:pass@pgbouncer:6432/myapp_relay?sslmode=require
```

The relay will negotiate TLS with PgBouncer; PgBouncer can then connect to
PostgreSQL using its own TLS configuration (including certificate pinning).

## Pgpool-II

Pgpool-II is an alternative pooler with load-balancing capabilities. The same
session-mode requirement applies. Route relay connections to the primary node
only — the relay must never connect to a read replica because it writes
consumer offsets.
