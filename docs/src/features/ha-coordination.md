# Feature: High Availability Coordination

pg_tide achieves high availability through PostgreSQL advisory locks. Multiple relay instances can run simultaneously — each discovers the same set of pipelines, but only one instance owns each pipeline at any time. If an instance crashes or loses its database connection, its locks are automatically released and another instance takes over.

## How It Works

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Relay #1   │     │  Relay #2   │     │  Relay #3   │
│ owns: A, B  │     │ owns: C, D  │     │ owns: E     │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────────────┐
                    │  PostgreSQL  │
                    │ advisory locks│
                    └──────────────┘
```

Each relay instance:

1. Discovers all enabled pipelines from the catalog
2. Attempts to acquire a PostgreSQL advisory lock for each pipeline
3. Only starts worker tasks for pipelines where it holds the lock
4. Periodically re-checks lock ownership during discovery

If Relay #1 crashes:
- PostgreSQL automatically releases its advisory locks (session locks die with the connection)
- Relay #2 or #3 acquires locks for pipelines A and B on the next discovery cycle
- Messages continue flowing within `discovery_interval_secs`

## Advisory Lock Mechanics

pg_tide uses `pg_try_advisory_lock(key1, key2)` where:
- `key1` = `hashtext(relay_group_id)` — Groups relays into a coordination cluster
- `key2` = `hashtext(pipeline_name)` — Identifies the specific pipeline

`pg_try_advisory_lock` is non-blocking — if another instance holds the lock, it returns false immediately rather than waiting. This means relay instances never deadlock or block each other.

## Configuration

### Relay Group ID

All relay instances that should coordinate must share the same `relay_group_id`:

```bash
# Instance 1
pg-tide --postgres-url "..." --relay-group-id "production"

# Instance 2
pg-tide --postgres-url "..." --relay-group-id "production"
```

Instances with different group IDs operate independently — they can both own the same pipeline (useful for blue/green deployments or multi-region setups with separate databases).

### Discovery Interval

Controls how quickly failover happens:

```bash
pg-tide --discovery-interval 10  # Check every 10 seconds
```

Lower values = faster failover, but more frequent PostgreSQL queries. The default of 30 seconds is a good balance for most deployments.

## Failover Timeline

```
t=0s    Relay #1 crashes (holds locks for pipeline A, B)
t=0s    PostgreSQL closes connection, releases advisory locks
t=10s   Relay #2 runs discovery cycle
t=10s   Relay #2 acquires lock for pipeline A (success)
t=10s   Relay #2 spawns worker for pipeline A
t=10s   Pipeline A resumes processing
```

With `discovery_interval = 10`, worst-case failover is 10 seconds. Messages are never lost — they wait safely in the outbox until a relay instance picks them up.

## Scaling Patterns

### Active-Active (recommended)
Run N relay instances. Pipelines are distributed across all instances automatically. As you add instances, pipelines rebalance on the next discovery cycle.

### Active-Standby
Run 2 instances. The primary acquires all locks. If it fails, the standby takes over. Simpler but less efficient than active-active.

### Per-Pipeline Scaling
For high-throughput pipelines, use [consumer groups](../concepts/consumer-groups.md) to parallelize within a single pipeline rather than running multiple relay instances.

## Graceful Shutdown

When a relay instance receives SIGTERM:

1. Coordinator sends stop signal to all owned workers
2. Workers complete their current batch (in-flight messages finish)
3. Workers acknowledge processed messages
4. Coordinator releases all advisory locks
5. Process exits

Other instances detect released locks on next discovery and take ownership.

## Monitoring HA

- **Prometheus gauge:** `pg_tide_pipeline_healthy` per instance shows which pipelines each instance owns
- **Advisory locks query:** `SELECT * FROM pg_locks WHERE locktype = 'advisory'` shows current ownership
- **Health endpoint:** `/health` reports healthy only if the instance owns at least one pipeline

## Further Reading

- [Graceful Shutdown](graceful-shutdown.md) — Clean shutdown behavior
- [Deployment Architectures](../operations/deployment-architectures.md) — HA deployment patterns
- [Scaling](../operations/scaling.md) — Scaling strategies
