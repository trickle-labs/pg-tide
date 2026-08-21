-- pg_tide canonical cluster roles (v0.44.0).
-- Run after installing/upgrading the extension. Never creates credentials.
DO $provision$
DECLARE r TEXT;
BEGIN
 IF NOT (current_user = 'postgres' OR EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=current_user AND (rolsuper OR rolcreaterole))) THEN
  RAISE EXCEPTION 'pg_tide_roles.sql requires superuser or CREATEROLE';
 END IF;
 FOREACH r IN ARRAY ARRAY['tide_admin','tide_publisher','tide_relay','tide_operator','tide_reader'] LOOP
  IF to_regrole(r) IS NULL THEN
   EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',r);
  ELSE
   EXECUTE format('ALTER ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',r);
  END IF;
 END LOOP;
 IF to_regrole('pg_tide_admin') IS NOT NULL THEN GRANT tide_admin TO pg_tide_admin; END IF;
END $provision$;

SELECT r.rolname, r.rolcanlogin, r.rolsuper, m.member::regrole AS member_of
FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid=m.roleid
WHERE r.rolname IN ('tide_admin','tide_publisher','tide_relay','tide_operator','tide_reader')
ORDER BY 1,4;

-- Apply the same exact object grants as the extension migration when this
-- script is run after an existing installation. Missing optional historical
-- relations are skipped so the script remains valid across upgrade points.
DO $grants$
DECLARE table_name TEXT;
BEGIN
 IF to_regclass('tide.tide_outbox_config') IS NULL THEN
   RETURN;
 END IF;

 GRANT USAGE ON SCHEMA tide TO tide_admin, tide_publisher, tide_relay,
                                tide_operator, tide_reader;
 FOREACH table_name IN ARRAY ARRAY[
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
 ] LOOP
   IF to_regclass(format('tide.%I', table_name)) IS NOT NULL THEN
     EXECUTE format(
       'GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
        ON TABLE tide.%I TO tide_admin',
       table_name
     );
   END IF;
 END LOOP;

 GRANT SELECT ON tide.tide_outbox_config, tide.outbox_publishers TO tide_publisher;
 REVOKE INSERT ON tide.tide_outbox_messages FROM tide_publisher;
 IF to_regclass('tide.tide_outbox_messages_id_seq') IS NOT NULL THEN
   REVOKE ALL ON SEQUENCE tide.tide_outbox_messages_id_seq FROM tide_publisher;
 END IF;

 GRANT SELECT ON tide.tide_outbox_config, tide.tide_outbox_messages,
                tide.tide_consumer_groups, tide.tide_consumer_offsets,
                tide.tide_consumer_leases, tide.relay_outbox_config,
                tide.relay_inbox_config, tide.relay_consumer_offsets,
                tide.relay_pipeline_state, tide.relay_pipeline_deps,
                tide.relay_limits, tide.relay_schema_fingerprints
   TO tide_relay;
 GRANT UPDATE ON tide.relay_outbox_config, tide.relay_inbox_config TO tide_relay;
 GRANT INSERT, UPDATE ON tide.relay_pipeline_state, tide.relay_consumer_offsets
   TO tide_relay;
 GRANT INSERT ON tide.relay_delivery_receipts, tide.relay_dlq TO tide_relay;
 GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA tide TO tide_relay;

 FOR table_name IN
   SELECT p.oid::regprocedure::text
   FROM pg_catalog.pg_proc p
   JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'tide'
 LOOP
   EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_admin', table_name);
 END LOOP;
 IF to_regprocedure('tide.outbox_publish(text,jsonb,jsonb)') IS NOT NULL THEN
   GRANT EXECUTE ON FUNCTION tide.outbox_publish(TEXT, JSONB, JSONB) TO tide_publisher;
 END IF;
 FOR table_name IN
   SELECT p.oid::regprocedure::text
   FROM pg_catalog.pg_proc p
   JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'tide'
     AND p.proname IN (
       'relay_list_configs', 'relay_get_config',
       'relay_pipeline_state_upsert', 'relay_auto_resume_candidates',
       'outbox_delivery_confirm', 'relay_dlq_retry', 'relay_dlq_retry_all',
       'commit_offset', 'consumer_heartbeat', 'outbox_rows_consumed',
       'poll_outbox', 'relay_truncate_delivery_receipts'
     )
 LOOP
   EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO tide_relay', table_name);
 END LOOP;
END $grants$;
