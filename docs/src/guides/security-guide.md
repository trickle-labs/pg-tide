# Security Guide

This guide covers security best practices for deploying pg_tide in production, including secret management, network security, authentication, and access control.

## Secret Management

### Environment Variable Substitution

pg_tide supports `${env:VARIABLE_NAME}` syntax in pipeline configurations. Secrets are resolved at runtime from the relay process's environment — they never appear in the PostgreSQL catalog:

```sql
SELECT tide.relay_set_outbox('my-pipeline', 'events', '{
    "sink_type": "kafka",
    "brokers": "${env:KAFKA_BROKERS}",
    "sasl_username": "${env:KAFKA_USER}",
    "sasl_password": "${env:KAFKA_PASS}"
}'::jsonb);
```

The catalog stores the `${env:...}` tokens, not the resolved values. The relay resolves them at startup.

### File-Based Secrets

For secrets stored on disk (Kubernetes mounted secrets, vault agent files):

```json
{
  "password": "${file:/run/secrets/db-password}"
}
```

The relay reads the file content, trims whitespace, and substitutes the value.

### Best Practices

- **Never** hardcode secrets in pipeline configurations
- Use Kubernetes Secrets mounted as environment variables or files
- Rotate secrets regularly — update the environment/file and the relay picks up new values on restart
- Use separate credentials per pipeline when possible (principle of least privilege)
- Restrict access to the `tide` schema — only the application and relay need access

## Database Access Control

### Principle of Least Privilege

Create dedicated roles for different access patterns:

```sql
-- Application role: can publish to outbox and read inbox
CREATE ROLE app_writer;
GRANT USAGE ON SCHEMA tide TO app_writer;
GRANT EXECUTE ON FUNCTION tide.outbox_publish TO app_writer;
GRANT EXECUTE ON FUNCTION tide.inbox_mark_processed TO app_writer;
GRANT SELECT ON tide.inbox_pending TO app_writer;

-- Relay role: needs full access to catalog and outbox/inbox tables
CREATE ROLE relay_worker;
GRANT USAGE ON SCHEMA tide TO relay_worker;
GRANT ALL ON ALL TABLES IN SCHEMA tide TO relay_worker;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA tide TO relay_worker;

-- Read-only monitoring role
CREATE ROLE monitor;
GRANT USAGE ON SCHEMA tide TO monitor;
GRANT SELECT ON tide.outbox_status TO monitor;
GRANT SELECT ON tide.relay_dlq TO monitor;
```

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
