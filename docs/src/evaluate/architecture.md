# Architecture

pg_tide consists of two components that work together: a **PostgreSQL extension** that manages the outbox/inbox catalog, and a **relay binary** that bridges messages to external systems.

---

## High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL 18+                         │
│                                                          │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────┐  │
│  │ tide_outbox   │   │ tide_inbox    │   │ relay_     │  │
│  │ _config       │   │ _config       │   │ *_config   │  │
│  └──────┬───────┘   └──────┬───────┘   └─────┬──────┘  │
│         │                   │                  │         │
│  ┌──────▼───────┐   ┌──────▼───────┐         │         │
│  │ tide_outbox   │   │ {name}_inbox  │         │         │
│  │ _messages     │   │  (per inbox)  │         │         │
│  └──────┬───────┘   └──────▲───────┘         │         │
│         │                   │                  │         │
└─────────┼───────────────────┼──────────────────┼─────────┘
          │                   │                  │
          │  LISTEN/NOTIFY    │                  │
          ▼                   │                  ▼
┌─────────────────────────────────────────────────────────┐
│                   pg-tide relay binary                    │
│                                                          │
│  ┌────────────┐        ┌────────────┐                   │
│  │   Source    │───────▶│    Sink     │                   │
│  │ (outbox    │        │ (NATS,     │                   │
│  │  poller)   │        │  Kafka,    │                   │
│  └────────────┘        │  Redis…)   │                   │
│                         └────────────┘                   │
│  ┌────────────┐        ┌────────────┐                   │
│  │   Source    │───────▶│    Sink     │                   │
│  │ (NATS,     │        │ (inbox     │                   │
│  │  Kafka…)   │        │  writer)   │                   │
│  └────────────┘        └────────────┘                   │
└─────────────────────────────────────────────────────────┘
          │                                     │
          ▼                                     ▼
┌──────────────┐                    ┌──────────────────┐
│  External     │                    │  External         │
│  Systems      │                    │  Systems          │
│  (consumers)  │                    │  (producers)      │
└──────────────┘                    └──────────────────┘
```

---

## The Extension Layer

The `pg_tide` extension installs into the `tide` schema and provides:

- **Catalog tables** — configuration for outboxes, inboxes, consumer groups, and relay pipelines
- **Message storage** — a shared `tide_outbox_messages` table for all outboxes, individual `{name}_inbox` tables for each inbox
- **SQL API** — functions for publishing, consuming, and managing the lifecycle
- **NOTIFY triggers** — real-time notifications when relay config changes

The extension has no background workers and no shared memory. All state is purely relational, making it compatible with connection poolers (PgBouncer, PgCat) and managed PostgreSQL services.

---

## The Relay Binary

The `pg-tide` binary is a standalone Rust process that:

1. **Connects to PostgreSQL** and reads pipeline configurations from the relay catalog
2. **Acquires advisory locks** for each pipeline (enabling multi-relay HA deployments)
3. **Polls outbox tables** for new messages (forward mode)
4. **Subscribes to external sources** for incoming messages (reverse mode)
5. **Delivers messages** to configured sinks with retry and dedup
6. **Commits offsets** after successful delivery (exactly-once semantics)
7. **Exposes Prometheus metrics** and a health endpoint

The relay supports hot-reload via `LISTEN tide_relay_config` — when you update pipeline config in the database, the relay picks up changes without restart.

---

## Forward Mode (Outbox → External)

```
Application   ──INSERT──▶  tide_outbox_messages
                                   │
                                   │ (relay polls)
                                   ▼
                             pg-tide relay  ──publish──▶  NATS / Kafka / Redis / …
                                   │
                                   │ (on success)
                                   ▼
                          commit offset + mark consumed
```

---

## Reverse Mode (External → Inbox)

```
NATS / Kafka / Redis / …  ──subscribe──▶  pg-tide relay
                                                  │
                                                  │ (dedup + insert)
                                                  ▼
                                           {name}_inbox table
                                                  │
                                                  │ (application reads)
                                                  ▼
                                            Your application
```

---

## Deployment Topologies

### Single relay (simplest)

One relay instance handles all pipelines. Suitable for low-to-medium throughput.

### Multiple relays (HA)

Multiple relay instances connect to the same database. PostgreSQL advisory locks ensure each pipeline is owned by exactly one relay — automatic failover when a relay dies.

### Sidecar pattern

Deploy the relay as a sidecar container alongside your application pod in Kubernetes. Each pod handles its own subset of pipelines.
