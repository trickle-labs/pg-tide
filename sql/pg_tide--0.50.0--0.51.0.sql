-- pg_tide 0.50.0 -> 0.51.0
--
-- Register durable extension state for logical backup.  This migration does
-- not rewrite or remove catalog data.

SELECT pg_catalog.pg_extension_config_dump('tide.tide_outbox_config', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_outbox_messages', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_consumer_groups', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_consumer_offsets', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_inbox_config', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_outbox_config', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_inbox_config', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_consumer_offsets', '');
SELECT pg_catalog.pg_extension_config_dump('tide.outbox_publishers', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_schema_fingerprints', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_limits', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_dlq', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_tenant_grants', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_tenant_roles', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_delivery_receipts', '');

-- Fresh installs seed these standard rows; only user-created templates belong
-- in a logical backup.
SELECT pg_catalog.pg_extension_config_dump(
    'tide.relay_pipeline_templates',
    'WHERE name NOT IN (
        ''kafka-topic-mirror'',
        ''ducklake-daily-sink'',
        ''nats-jetstream-fanout'',
        ''pg-inbox-relay'',
        ''webhook-notification''
    )'
);

SELECT pg_catalog.pg_extension_config_dump('tide.relay_config_audit', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_pipeline_deps', '');
SELECT pg_catalog.pg_extension_config_dump('tide.relay_pipeline_state', '');
SELECT pg_catalog.pg_extension_config_dump('tide.outbox_cleanup_state', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_partition_events', '');
SELECT pg_catalog.pg_extension_config_dump('tide.outbox_encryption_config', '');
SELECT pg_catalog.pg_extension_config_dump('tide.tide_security_audit', '');

-- v0.43 creates this singleton while installing the extension.  Its physical
-- layout is derived during installation, so exclude the bootstrap row to
-- avoid a duplicate key during logical restore.
SELECT pg_catalog.pg_extension_config_dump(
    'tide.outbox_storage_config',
    'WHERE NOT singleton'
);

COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.51.0';
