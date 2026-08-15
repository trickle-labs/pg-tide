# Apache Arrow Flight

Apache Arrow Flight is a high-performance RPC framework for transferring large datasets between systems using the Apache Arrow columnar memory format. Unlike JSON-based protocols that require serialization/deserialization, Arrow Flight transfers data in-memory columnar format over gRPC, achieving throughput measured in gigabytes per second. When pg_tide delivers messages via Arrow Flight, your events are batched into Arrow record batches and streamed to any Arrow Flight-compatible endpoint.

## When to Use This Sink

Choose Arrow Flight when you need maximum throughput for analytical workloads (machine learning pipelines, real-time feature stores, analytics engines), when the receiving system supports Arrow natively (DuckDB, DataFusion, Polars, pandas, many ML frameworks), or when you want to minimize serialization overhead for high-volume data transfer. Arrow Flight is particularly effective for scenarios where events are consumed in batches for computation rather than processed individually.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-flight',
    'outbox', 'events',
    'sink_type', 'arrow_flight',
    'config', '{
        "endpoint": "grpc://${env:FLIGHT_HOST}:8815",
        "batch_size": 5000,
        "tls_enabled": false
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"arrow_flight"` |
| `endpoint` | string | — | gRPC endpoint URL |
| `batch_size` | int | `1000` | Records per Arrow record batch |
| `tls_enabled` | bool | `false` | Enable TLS for gRPC |
| `auth_token` | string | `null` | Bearer token for authentication |

## How It Works

Messages are accumulated into batches and converted to Arrow columnar format (record batches). The relay then streams these record batches to the Flight endpoint using gRPC's DoPut RPC. This approach is dramatically more efficient than JSON-over-HTTP for large batch transfers because:

1. Arrow's columnar format enables zero-copy reads on the receiver side
2. gRPC streaming amortizes connection overhead across many records
3. Arrow's type system preserves data types without string conversion

## Troubleshooting

- **"Connection failed"** — Verify the gRPC endpoint is reachable and the port is correct
- **"Unauthenticated"** — Set `auth_token` if the Flight server requires authentication
- **Low throughput** — Increase `batch_size`; Arrow Flight is most efficient with large batches

## Further Reading

- [ClickHouse](clickhouse.md) — For persistent analytical storage
- [Object Storage](object-storage.md) — For file-based data delivery
