# Security Guide

This guide covers security best practices for deploying pg_tide in production, including secret management, network security, authentication, and access control.

## Secret Management

### Environment Variable Substitution

pg_tide supports the strict `${env:VARIABLE_NAME}` syntax in pipeline
configurations. `${ENV:VARIABLE_NAME}` is accepted for one compatibility release
with a deprecation warning. Malformed, nested, unknown, or unresolved references
fail validation.

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'my-pipeline',
    'outbox', 'events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "${env:KAFKA_BROKERS}",
        "sasl_username": "${env:KAFKA_USER}",
        "sasl_password": "${env:KAFKA_PASS}"
    }'::jsonb
  )
);
```

The catalog stores the `${env:...}` tokens, not the resolved values. The relay resolves them at startup.

### File-Based Secrets

For secrets stored on disk (Kubernetes mounted secrets, vault agent files):

```json
{
  "password": "${file:/run/secrets/db-password}"
}
```

The path must be absolute. The relay opens it without following symlinks and
requires a regular file owned by the relay user with no group/world permission
bits and a bounded size. Unsafe files fail closed.

### Best Practices

- **Never** hardcode secrets in pipeline configurations
- Use `${env:NAME}` or `${file:/absolute/path}` references
- Rotate secrets regularly — update the environment/file and the relay picks up new values on restart
- Use separate credentials per pipeline when possible (principle of least privilege)
- Resolved values are never written to catalog JSON, history, status, metrics,
  templates, logs, or error output

## Database Access Control

### Principle of Least Privilege

Provision the canonical non-login group roles after installing or upgrading the
extension:

```bash
psql -f deploy/postgres/pg_tide_roles.sql "$DATABASE_URL"
```

The script creates only `NOLOGIN` group roles:
`tide_admin`, `tide_publisher`, `tide_relay`, `tide_operator`, and
`tide_reader`. Grant those groups to existing login roles according to the
least-privilege matrix. Existing `pg_tide_admin` is retained as a deprecated
compatibility alias and inherits `tide_admin`; it is not created on fresh
installations.

`tide_publisher` membership is not enough to publish. The caller also needs an
explicit row in the outbox publisher ACL. Reader and operator surfaces expose
sanitized status only; raw configuration, history, payloads, and secret-bearing
catalogs remain unavailable.

### Connection Security

Always use TLS for PostgreSQL connections:

```bash
pg-tide --postgres-url "postgres://relay:pass@db:5432/mydb?sslmode=require"
```

For strict certificate verification:
```bash
pg-tide --postgres-url "postgres://relay:pass@db:5432/mydb?sslmode=verify-full&sslrootcert=/certs/ca.pem"
```

## Network Security

### Relay Process

The relay exposes two network endpoints:
- **Metrics endpoint** (default `:9090`) — Prometheus metrics and health check
- **Webhook receiver** (if configured) — Incoming webhooks

Secure them:
- Bind metrics to internal network only: `--metrics-addr "10.0.0.0:9090"`
- Use network policies (Kubernetes) to restrict access
- Never expose metrics to the public internet

### Sink Connections

- **Use TLS** for all sink connections (Kafka, NATS, HTTP, cloud services)
- **Use SASL/mTLS** for Kafka when available
- **Verify certificates** — don't disable TLS verification in production
- **Use private endpoints** for cloud services (AWS PrivateLink, GCP Private Service Connect)

### Kubernetes Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pg-tide-relay
spec:
  podSelector:
    matchLabels:
      app: pg-tide-relay
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: prometheus
      ports:
        - port: 9090
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: postgres
      ports:
        - port: 5432
    - to:  # Allow outbound to sinks
        - namespaceSelector: {}
```

## Webhook Security

### Outgoing Webhooks

Sign outgoing webhooks so recipients can verify authenticity:

```json
{
  "sink_type": "webhook",
  "url": "https://partner.example.com/events",
  "signature": {
    "scheme": "hmac-sha256",
    "secret": "${env:WEBHOOK_SECRET}",
    "header": "X-Signature-256"
  }
}
```

### Incoming Webhooks

Always verify incoming webhook signatures:

```json
{
  "source_type": "webhook",
  "signature_scheme": "stripe",
  "signature_secret": "${env:STRIPE_WEBHOOK_SECRET}"
}
```

Reject unsigned requests. See [Webhook Signatures](../features/webhook-signatures.md).

## Audit Trail

pg_tide maintains a natural audit trail:
- Every published event has a sequential ID, timestamp, and stream table
- The DLQ records all delivery failures with error details
- Relay logs show all pipeline activity

For compliance, ensure:
- Relay logs are shipped to a centralized logging system
- DLQ entries are reviewed within your SLA
- Outbox tables have appropriate retention policies

## Common Vulnerabilities to Avoid

| Risk | Mitigation |
|------|-----------|
| Secrets in catalog | Use `${env:...}` substitution |
| Unencrypted connections | Enforce `sslmode=require` or `verify-full` |
| Open metrics endpoint | Bind to internal network, use network policies |
| Excessive permissions | Use dedicated roles with minimal grants |
| Unsigned webhooks | Always configure signature verification |
| Stale credentials | Implement secret rotation procedures |

## Further Reading

- [Webhook Signatures](../features/webhook-signatures.md) — Signature schemes
- [Deployment Guide](../operations/deployment-guide.md) — Production deployment
- [Reference: Security](../reference/security.md) — Extension security model
- [Threat Model](../reference/threat-model.md) — Threats, evidence, and owners
- [Dependency Policy](../reference/dependency-policy.md) — Dependency and artifact gates
