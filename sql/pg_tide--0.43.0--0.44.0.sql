-- pg_tide v0.43.0 -> v0.44.0: final database-boundary ACL hardening.
REVOKE CREATE ON SCHEMA tide FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA tide FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tide FROM PUBLIC;

DO $acl$
DECLARE f RECORD;
BEGIN
 FOR f IN
   SELECT p.oid::regprocedure signature, p.prosecdef
   FROM pg_catalog.pg_proc p
   JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname='tide'
 LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',f.signature);
  IF f.prosecdef THEN
   EXECUTE format('ALTER FUNCTION %s SET search_path = pg_catalog, tide',f.signature);
  END IF;
 END LOOP;
END $acl$;

DO $grants$
DECLARE f RECORD;
BEGIN
 IF to_regrole('tide_admin') IS NOT NULL THEN
   GRANT USAGE ON SCHEMA tide TO tide_admin;
   -- Administrative APIs are invoker-rights functions. Keep the grant
   -- explicit while covering every catalog relation created by the chain.
   FOR f IN
     SELECT unnest(ARRAY[
       'backfill_jobs', 'ducklake_offset_map', 'ducklake_partition_config',
       'ducklake_source_config', 'outbox_cleanup_state',
       'outbox_encryption_config', 'outbox_publishers', 'outbox_storage_config',
       'relay_airbyte_state', 'relay_config_audit', 'relay_consumer_offsets',
       'relay_delivery_receipts', 'relay_dlq', 'relay_fanin_config',
       'relay_inbox_config', 'relay_limits', 'relay_outbox_config',
       'relay_pipeline_deps', 'relay_pipeline_state', 'relay_pipeline_templates',
       'relay_schema_fingerprints', 'relay_tenant_grants', 'relay_tenant_roles',
       'singer_schema_log', 'singer_state', 'tide_consumer_groups',
       'tide_consumer_leases', 'tide_consumer_offsets', 'tide_inbox_config',
       'tide_outbox_config', 'tide_outbox_messages', 'tide_partition_events',
       'tide_security_audit'
     ]) table_name
   LOOP
     IF to_regclass(format('tide.%I', f.table_name)) IS NOT NULL THEN
       EXECUTE format(
         'GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
          ON TABLE tide.%I TO tide_admin',
         f.table_name
       );
     END IF;
   END LOOP;
   FOR f IN
     SELECT p.oid::regprocedure signature
     FROM pg_catalog.pg_proc p
     JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='tide'
   LOOP
     EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_admin', f.signature);
   END LOOP;
 END IF;

 IF to_regrole('tide_publisher') IS NOT NULL THEN
   GRANT USAGE ON SCHEMA tide TO tide_publisher;
   GRANT SELECT ON tide.tide_outbox_config, tide.outbox_publishers
     TO tide_publisher;
   GRANT INSERT ON tide.tide_outbox_messages TO tide_publisher;
   IF to_regclass('tide.tide_outbox_messages_id_seq') IS NOT NULL THEN
     GRANT USAGE, SELECT ON SEQUENCE tide.tide_outbox_messages_id_seq
       TO tide_publisher;
   END IF;
   FOR f IN
     SELECT p.oid::regprocedure signature
     FROM pg_catalog.pg_proc p
     JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='tide'
       AND p.proname IN ('outbox_publish', 'outbox_publish_large')
   LOOP
     EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_publisher', f.signature);
   END LOOP;
 END IF;

 IF to_regrole('tide_relay') IS NOT NULL THEN
   GRANT USAGE ON SCHEMA tide TO tide_relay;
   GRANT SELECT ON tide.tide_outbox_config,
                    tide.tide_outbox_messages,
                    tide.tide_consumer_groups,
                    tide.tide_consumer_offsets,
                    tide.tide_consumer_leases,
                    tide.relay_outbox_config,
                    tide.relay_inbox_config,
                    tide.relay_consumer_offsets,
                    tide.relay_pipeline_state,
                    tide.relay_pipeline_deps,
                    tide.relay_limits,
                    tide.relay_schema_fingerprints
     TO tide_relay;
   GRANT UPDATE ON tide.relay_outbox_config, tide.relay_inbox_config TO tide_relay;
   GRANT INSERT, UPDATE ON tide.relay_pipeline_state TO tide_relay;
   GRANT INSERT, UPDATE ON tide.relay_consumer_offsets TO tide_relay;
   GRANT INSERT ON tide.relay_delivery_receipts, tide.relay_dlq TO tide_relay;
   GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA tide TO tide_relay;
   FOR f IN
     SELECT p.oid::regprocedure signature
     FROM pg_catalog.pg_proc p
     JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='tide'
       AND p.proname IN (
         'relay_list_configs', 'relay_get_config',
         'relay_pipeline_state_upsert', 'relay_auto_resume_candidates',
         'outbox_delivery_confirm', 'relay_dlq_retry', 'relay_dlq_retry_all',
         'commit_offset', 'consumer_heartbeat', 'outbox_rows_consumed',
         'poll_outbox', 'relay_truncate_delivery_receipts'
       )
   LOOP
     EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_relay', f.signature);
   END LOOP;
 END IF;

 IF to_regrole('tide_reader') IS NOT NULL THEN
   GRANT USAGE ON SCHEMA tide TO tide_reader;
   GRANT SELECT ON tide.relay_pipeline_lag,
                    tide.outbox_retention_status,
                    tide.inbox_fleet_summary,
                    tide.outbox_pending,
                    tide.consumer_lag
     TO tide_reader;
   FOR f IN
     SELECT p.oid::regprocedure signature
     FROM pg_catalog.pg_proc p
     JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='tide'
       AND p.proname IN ('outbox_status', 'inbox_status', 'relay_pipeline_lag')
   LOOP
     EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_reader', f.signature);
   END LOOP;
 END IF;

 IF to_regrole('tide_operator') IS NOT NULL THEN
   GRANT USAGE ON SCHEMA tide TO tide_operator;
   GRANT SELECT ON tide.relay_pipeline_lag,
                    tide.outbox_retention_status,
                    tide.inbox_fleet_summary,
                    tide.outbox_pending,
                    tide.consumer_lag
     TO tide_operator;
   FOR f IN
     SELECT p.oid::regprocedure signature
     FROM pg_catalog.pg_proc p
     JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
     WHERE n.nspname='tide'
       AND p.proname IN (
         'outbox_status', 'inbox_status', 'relay_pipeline_lag',
         'relay_enable', 'relay_disable', 'outbox_sweep',
         'outbox_truncate_delivered', 'backfill_status'
       )
   LOOP
     EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_operator', f.signature);
   END LOOP;
 END IF;
END $grants$;

DO $compat$
BEGIN
 IF to_regrole('tide_admin') IS NOT NULL AND to_regrole('pg_tide_admin') IS NOT NULL THEN GRANT tide_admin TO pg_tide_admin; END IF;
END $compat$;
