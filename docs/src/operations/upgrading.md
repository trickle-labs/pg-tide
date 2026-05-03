# Upgrading

How to upgrade pg_tide to a new version.

---

## Extension Upgrades

pg_tide uses PostgreSQL's built-in extension versioning. Upgrade scripts are provided for each version transition.

### Check Current Version

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
```

### Upgrade

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.2.0';
```

PostgreSQL runs the appropriate migration script (`sql/pg_tide--0.1.0--0.2.0.sql`).

### Rollback

Extension downgrades are not supported by PostgreSQL's `ALTER EXTENSION`. To roll back, restore from backup.

---

## Relay Upgrades

The relay binary is stateless — all state lives in PostgreSQL. Upgrade by replacing the binary:

### Binary Replacement

```bash
# Stop relay
systemctl stop pg-tide-relay

# Replace binary
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-*.tar.gz
sudo mv pg-tide /usr/local/bin/

# Restart
systemctl start pg-tide-relay
```

### Docker / Kubernetes

```bash
# Update image tag
kubectl set image deployment/pg-tide-relay relay=ghcr.io/trickle-labs/pg-tide:0.2.0
```

Rolling updates work seamlessly: the new instance acquires advisory locks as the old instance releases them during shutdown.

---

## Zero-Downtime Upgrades

1. Deploy new relay instances alongside old ones (same `relay_group_id`)
2. New instances wait for advisory locks
3. Gracefully stop old instances (`SIGTERM`)
4. Old instances release locks → new instances acquire them
5. Processing resumes with no message loss

---

## Compatibility Matrix

| pg_tide Extension | Relay Binary | PostgreSQL |
|-------------------|-------------|-----------|
| 0.1.x | 0.1.x | 18+ |

The relay binary is forward-compatible with same-minor extension versions. Always upgrade the extension before upgrading the relay.
