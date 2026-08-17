# Prometheus metrics

The relay exposes `/metrics`, `/livez`, and `/readyz` on the configured metrics
address (default `0.0.0.0:9090`). `/livez` answers whether the process is alive;
`/readyz` answers whether it can serve owned pipelines. A pipeline failure must
remove a pod from service, not cause a restart loop.

## Core metrics

All relay metrics use the `pg_tide_relay_` prefix. Labels are only documented
where emitted by the relay (`pipeline`, `direction`, and metric-specific labels).

| Metric | Meaning |
|---|---|
| `pg_tide_relay_messages_published_total` | Successful sink publishes |
| `pg_tide_relay_messages_consumed_total` | Messages read from sources |
| `pg_tide_relay_publish_errors_total` | Failed publish attempts |
| `pg_tide_relay_pipeline_healthy` | 1 when pipeline is healthy, 0 otherwise |
| `pg_tide_relay_consumer_lag` | Pending source messages |
| `pg_tide_relay_delivery_latency_seconds` | Delivery latency histogram |
| `pg_tide_relay_retry_state` | Current retry/backoff state |
| `pg_tide_relay_dlq_depth` | Unresolved DLQ entries |
| `pg_tide_relay_owned_pipelines` | Pipelines owned by this relay |
| `pg_tide_relay_pool_connections` | Pool connections by state |

See `pg-tide/dashboards/relay-health.json` for the core dashboard and
`pg-tide/dashboards/alerts.yaml` for actionable thresholds. No-data is unknown,
never healthy. Use the [operations runbooks](../operations/runbooks.md) for
incident response.

```promql
sum by (pipeline) (rate(pg_tide_relay_messages_published_total[5m]))
pg_tide_relay_consumer_lag
histogram_quantile(0.99, sum by (le,pipeline) (rate(pg_tide_relay_delivery_latency_seconds_bucket[5m])))
```
