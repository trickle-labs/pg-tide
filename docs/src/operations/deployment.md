# Deployment

Guidance for deploying pg_tide in production environments.

---

## Components to Deploy

1. **pg_tide extension** — installed in your PostgreSQL database
2. **pg-tide relay** — one or more relay binary instances

---

## Extension Deployment

Install the extension on your PostgreSQL 18+ server:

```sql
CREATE EXTENSION pg_tide;
```

The extension creates the `tide` schema with all required tables, views, and functions. No background workers, no shared memory — compatible with all managed PostgreSQL services and connection poolers.

---

## Relay Deployment Options

### Standalone binary

Download from [GitHub Releases](https://github.com/trickle-labs/pg-tide/releases) and run:

```bash
pg-tide --postgres-url "postgres://..." --relay-group-id production
```

### Docker

```bash
docker run -d \
  -e PGTRICKLE_RELAY_POSTGRES_URL="postgres://..." \
  -e PGTRICKLE_RELAY_LOG_FORMAT=json \
  -p 9090:9090 \
  ghcr.io/trickle-labs/pg-tide:0.1.0
```

### Kubernetes

See [Kubernetes deployment guide](kubernetes.md).

---

## High Availability

Run multiple relay instances with the same `relay_group_id`. PostgreSQL advisory locks ensure each pipeline is owned by exactly one instance. If an instance dies, another automatically takes over.

```bash
# Instance A
pg-tide --relay-group-id production --postgres-url ...

# Instance B (standby — acquires pipelines on failover)
pg-tide --relay-group-id production --postgres-url ...
```

---

## Resource Requirements

The relay is lightweight:

- **CPU:** ~50m per active pipeline under load
- **Memory:** ~20-50 MB base + message buffer
- **Network:** PostgreSQL connection + sink connections
- **Disk:** None (stateless — all state in PostgreSQL)

---

## Pre-Deployment Checklist

- [ ] PostgreSQL 18+ with `pg_tide` extension installed
- [ ] Relay binary or Docker image available
- [ ] Pipeline configurations created in the database
- [ ] Consumer groups created for each forward pipeline
- [ ] Monitoring set up (Prometheus scrape + alerts)
- [ ] Health check configured in load balancer / orchestrator
