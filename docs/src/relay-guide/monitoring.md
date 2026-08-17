# Monitoring

Scrape `/metrics` from the relay. Use `/livez` for Kubernetes liveness and
`/readyz` for readiness; `/health` is a legacy compatibility endpoint and must
not be used for both probes.

| Endpoint | Purpose |
|---|---|
| `GET /metrics` | Prometheus exposition |
| `GET /livez` | Process liveness |
| `GET /readyz` | Pipeline/readiness state |

The metric contract uses names such as `pg_tide_relay_pipeline_healthy`,
`pg_tide_relay_consumer_lag`, `pg_tide_relay_publish_errors_total`,
`pg_tide_relay_retry_state`, and `pg_tide_relay_dlq_depth`. Do not substitute
unprefixed names or infer health from missing series.

Install the core dashboard and alert rules from `pg-tide/dashboards/`; the
PostgreSQL detail dashboard is separate and optional. For incidents start with
`pg-tide doctor --output json` and `pg-tide status --output json`, then follow
the [runbook index](../operations/runbooks.md).
