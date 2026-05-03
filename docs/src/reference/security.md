# Security

Security considerations for pg_tide deployments.

---

## Extension Security

### Schema Isolation

All pg_tide objects live in the `tide` schema. The extension is marked `trusted = true` and `superuser = false` — it can be installed by any user with `CREATE` privilege on the database.

### No Elevated Privileges

The extension uses no:

- Background workers
- Shared memory
- File system access
- Network connections
- `SECURITY DEFINER` functions

All operations run with the privileges of the calling user.

### Catalog Table Access

By default, any user with access to the `tide` schema can read and write all catalog tables. For multi-tenant deployments, consider:

```sql
-- Restrict outbox creation to admin role
REVOKE INSERT ON tide.tide_outbox_config FROM PUBLIC;
GRANT INSERT ON tide.tide_outbox_config TO tide_admin;

-- Allow publishing to all application users
GRANT EXECUTE ON FUNCTION tide.outbox_publish(text, jsonb, jsonb) TO app_user;
```

---

## Relay Security

### Connection String Protection

Never embed passwords in config files committed to version control. Use environment variables:

```toml
postgres_url = "postgres://${ENV:PG_USER}:${ENV:PG_PASSWORD}@${ENV:PG_HOST}:5432/mydb"
```

Or use Kubernetes secrets, HashiCorp Vault, or your platform's secret management.

### Least-Privilege Database User

The relay needs minimal privileges:

```sql
CREATE ROLE pg_tide_relay LOGIN PASSWORD 'secret';
GRANT USAGE ON SCHEMA tide TO pg_tide_relay;
GRANT SELECT, UPDATE ON tide.tide_outbox_messages TO pg_tide_relay;
GRANT SELECT ON tide.tide_outbox_config TO pg_tide_relay;
GRANT SELECT ON tide.relay_outbox_config TO pg_tide_relay;
GRANT SELECT ON tide.relay_inbox_config TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE ON tide.tide_consumer_offsets TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON tide.tide_consumer_leases TO pg_tide_relay;
GRANT SELECT, INSERT, UPDATE ON tide.relay_consumer_offsets TO pg_tide_relay;
```

### Network Security

- The relay's metrics endpoint (default `:9090`) should not be exposed publicly
- Use TLS for PostgreSQL connections in production (`sslmode=require`)
- Use TLS for sink connections (NATS TLS, Kafka SSL, HTTPS webhooks)

### Docker Security

The official Docker image runs as non-root user `pgtide` (UID 1000):

```dockerfile
USER pgtide
```

No capabilities are required. Use `securityContext` in Kubernetes:

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
```

---

## Payload Security

### Sensitive Data

Avoid publishing sensitive data (PII, credentials) in outbox payloads. If you must, encrypt at the application layer before calling `outbox_publish()`.

### Input Validation

pg_tide accepts any valid JSONB as payload. Validate payloads at the application level before publishing. The extension does not perform content validation.

---

## Reporting Vulnerabilities

Report security issues to: security@trickle-labs.com

Do not open public GitHub issues for security vulnerabilities.
