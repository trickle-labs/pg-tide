# Wire Formats

A wire format defines how messages are serialized on the transport layer — what shape the bytes take when they travel between systems. When pg_tide relays messages from an outbox to a sink, it encodes them into a wire format. When it receives messages from a source into an inbox, it decodes them from a wire format. The wire format is the contract between pg_tide and the external system.

## Why Wire Formats Matter

Different systems expect different message shapes. A Debezium consumer expects a specific JSON envelope with `before`, `after`, and `op` fields. A Maxwell consumer expects `database`, `table`, `type`, and `data`. Your own services might have a custom CDC format. Wire formats let pg_tide speak all of these languages without changing your application code or database schema.

## Supported Formats

| Format | Encode (outbox → sink) | Decode (source → inbox) | Use Case |
|--------|:----------------------:|:-----------------------:|----------|
| [Native](native.md) | ✓ | ✓ | Default pg_tide format — simple and complete |
| [Debezium](debezium.md) | ✓ | ✓ | Kafka Connect ecosystem, schema registries |
| [Maxwell](maxwell.md) | — | ✓ | Ingest from Maxwell CDC tool |
| [Canal](canal.md) | — | ✓ | Ingest from Alibaba Canal CDC tool |
| [CDC JSON](cdc-json.md) | — | ✓ | Map any JSON CDC format via JSONPath |

## Choosing a Wire Format

**Use native** when both sides are pg_tide, when you control the consumer, or when you want the simplest possible format. Native passes through the outbox row with minimal transformation.

**Use debezium** when you're feeding Kafka consumers that already understand Debezium, when you need schema registry integration, when you're replacing Debezium CDC with pg_tide, or when you want compatibility with the broader Kafka Connect ecosystem.

**Use maxwell or canal** when you're migrating from those MySQL CDC tools and want to ingest their streams into PostgreSQL via a pg_tide inbox.

**Use cdc_json** when you have a custom CDC format from any system — define JSONPath expressions that map fields to pg_tide's internal model, and you can ingest any JSON-based CDC stream without writing code.

## Configuration

Wire format is specified per-pipeline in the relay configuration:

```toml
[[pipelines]]
name = "orders-to-kafka"
wire_format = "debezium"

[pipelines.wire_config]
server_name = "production"
emit_tombstones = true
```

Or via SQL:

```sql
SELECT tide.relay_set_outbox(
    'orders-pipeline',
    'order_events',
    '{
        "sink_type": "kafka",
        "wire_format": "debezium",
        "wire_config": {
            "server_name": "production",
            "emit_tombstones": true
        }
    }'::jsonb
);
```

## Message Flow

```
┌─────────────┐     encode      ┌───────────────┐
│  Outbox Row  │ ──────────────→ │ Encoded Bytes  │ → Sink
└─────────────┘                  └───────────────┘

┌───────────────┐     decode     ┌─────────────┐
│  Raw Message   │ ──────────────→ │  Inbox Row   │ → PostgreSQL
└───────────────┘                 └─────────────┘
```

During encoding, the wire format takes an outbox row (with fields like `op`, `new_row`, `old_row`, `stream_table`) and produces bytes suitable for the transport. During decoding, it takes raw bytes from a source and extracts the semantic fields (operation type, payload, event ID, timestamps) into an inbox row.

## Further Reading

- [Native Format](native.md) — The default, transparent format
- [Debezium Format](debezium.md) — Full Kafka Connect compatibility
- [CDC JSON Format](cdc-json.md) — Map any custom format via JSONPath
