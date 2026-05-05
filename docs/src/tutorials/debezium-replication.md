# Tutorial: Debezium-Compatible CDC Replication

This tutorial shows how to use pg_tide as a Debezium-compatible CDC (Change Data Capture) source. If you're currently using Debezium to capture PostgreSQL changes and publish them to Kafka, pg_tide can replace the Debezium connector while producing identical message formats — giving you transactional outbox guarantees instead of WAL-based CDC.

## Why Replace Debezium with pg_tide?

| Aspect | Debezium | pg_tide |
|--------|----------|---------|
| Mechanism | Reads WAL (logical replication) | Transactional outbox (application-level) |
| Consistency | Eventually consistent (WAL delay) | Transactionally consistent |
| Schema changes | Can miss or break on DDL | Application controls the schema |
| Selectivity | Captures all row changes | Application chooses what to publish |
| Infrastructure | Kafka Connect cluster | Single relay binary |
| Message format | Debezium JSON/Avro | Same (via wire_format = "debezium") |

## What You'll Build

A pipeline that publishes order changes in Debezium format to Kafka, compatible with existing Debezium consumers.

## Step 1: Create the Outbox

```sql
CREATE EXTENSION pg_tide;
SELECT tide.outbox_create('cdc_events');
```

## Step 2: Create a Trigger (Optional)

If you want automatic CDC (capture all changes without modifying application code), add a trigger:

```sql
CREATE OR REPLACE FUNCTION capture_order_changes() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM tide.outbox_publish('cdc_events', 'orders', 
            jsonb_build_object(
                'op', 'insert',
                'new_row', row_to_json(NEW)::jsonb,
                'old_row', null
            )
        );
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM tide.outbox_publish('cdc_events', 'orders',
            jsonb_build_object(
                'op', 'update',
                'new_row', row_to_json(NEW)::jsonb,
                'old_row', row_to_json(OLD)::jsonb
            )
        );
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM tide.outbox_publish('cdc_events', 'orders',
            jsonb_build_object(
                'op', 'delete',
                'new_row', null,
                'old_row', row_to_json(OLD)::jsonb
            )
        );
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_cdc
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH ROW EXECUTE FUNCTION capture_order_changes();
```

## Step 3: Configure Debezium Wire Format

```sql
SELECT tide.relay_set_outbox(
    'cdc-orders',
    'cdc_events',
    '{
        "sink_type": "kafka",
        "brokers": "kafka:9092",
        "topic": "dbserver1.public.orders",
        "wire_format": "debezium",
        "wire_config": {
            "server_name": "dbserver1",
            "emit_tombstones": true,
            "key_strategy": "primary_key"
        }
    }'::jsonb
);
```

The topic name `dbserver1.public.orders` follows Debezium's naming convention: `{server_name}.{schema}.{table}`.

## Step 4: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

## Step 5: Verify Consumer Compatibility

Your existing Debezium consumers should work without changes. The messages have the same shape:

```json
{
  "schema": { ... },
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "customer_id": "CUST-42",
      "total": 149.99,
      "status": "created"
    },
    "op": "c",
    "ts_ms": 1714029482000,
    "source": {
      "version": "pg-tide",
      "connector": "postgresql",
      "name": "dbserver1",
      "ts_ms": 1714029482000,
      "db": "mydb",
      "schema": "public",
      "table": "orders"
    }
  }
}
```

The only visible difference: `source.version` says `"pg-tide"` instead of a Debezium version number.

## Migration Strategy

1. **Run in parallel:** Deploy pg_tide alongside Debezium, publishing to a test topic
2. **Compare output:** Verify messages are compatible with your consumers
3. **Switch consumers:** Point consumers to the pg_tide topic
4. **Decommission Debezium:** Remove the Debezium connector and Kafka Connect cluster

## With Schema Registry (Avro)

For Avro-encoded Debezium messages:

```sql
SELECT tide.relay_set_outbox(
    'cdc-orders-avro',
    'cdc_events',
    '{
        "sink_type": "kafka",
        "brokers": "kafka:9092",
        "topic": "dbserver1.public.orders",
        "wire_format": "debezium",
        "wire_config": {
            "server_name": "dbserver1",
            "envelope": "avro"
        },
        "schema_registry": {
            "url": "http://schema-registry:8081"
        }
    }'::jsonb
);
```

## Further Reading

- [Wire Format: Debezium](../wire-formats/debezium.md) — Complete Debezium format reference
- [Schema Registry](../features/schema-registry.md) — Avro + Schema Registry integration
- [Sinks: Kafka](../sinks/kafka.md) — Kafka sink configuration
