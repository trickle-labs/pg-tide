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
- unrestricted `SECURITY DEFINER` functions

Administrative and maintenance functions use a fixed `search_path =
pg_catalog, tide`, qualify protected objects, authorize `session_user`, and
revoke `PUBLIC` execution. The v0.44 migration grants only the canonical group
roles.

### Catalog Table Access

Install the canonical roles with
`deploy/postgres/pg_tide_roles.sql`. Do not grant direct catalog access to
application logins:

```sql
GRANT tide_publisher TO app_login;
SELECT tide.outbox_grant_publish('orders', 'app_login');
```

---

## Relay Security

### Connection String Protection

Never embed passwords in config files committed to version control. Use environment variables:

```toml
postgres_url = "postgres://${env:PG_USER}:${env:PG_PASSWORD}@${env:PG_HOST}:5432/mydb"
```

File references use `${file:/absolute/path}` and are checked for ownership,
permissions, type, and size. Resolved values are never serialized or logged.
Secret references are resolved only at the final connector boundary; malformed,
unknown, missing, or unsafe references fail closed.

### Least-Privilege Database User

The relay login receives membership in the non-login `tide_relay` group. The
provisioning script never creates login roles or passwords.

### Network Security

- The relay's metrics endpoint (default `:9090`) should not be exposed publicly
- Use TLS for PostgreSQL connections in production (`sslmode=require`)
- Use TLS for sink connections (NATS TLS, Kafka SSL, HTTPS webhooks)
- HTTP clients use HTTPS, disable automatic redirects, and ignore ambient
  `HTTP_PROXY`/`HTTPS_PROXY` settings. Development exceptions must be explicit
  connector configuration.
- URL validation rejects loopback, private, link-local, metadata, mapped,
  documentation, multicast, and other special-use addresses.

### Encryption Provider Scope

`LocalKeyFile` is the only supported v0.44.0 encryption provider. AWS KMS,
GCP KMS, and Vault Transit names remain unavailable/experimental and are not
included in the production connector profile.

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
