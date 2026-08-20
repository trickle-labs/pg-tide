-- pg_tide v0.48.0 -> v0.49.0: close removed SQL/configuration surfaces.
--
-- This migration is deliberately fail-closed.  It does not disable, rewrite,
-- or delete an enabled pipeline or feature state on the operator's behalf.

DO $preflight$
DECLARE
    affected TEXT;
    reverse_state TEXT;
    fanin_state TEXT;
    backfill_state TEXT;
BEGIN
    SELECT string_agg(surface || ':' || name, ', ' ORDER BY surface, name)
      INTO affected
      FROM (
          SELECT 'outbox'::TEXT AS surface, name
            FROM tide.relay_outbox_config
           WHERE enabled
             AND (
                 COALESCE(config ->> 'source_type', '') <> 'outbox'
                 OR COALESCE(config ->> 'sink_type', '') NOT IN
                    ('inbox', 'pg_outbox', 'nats', 'kafka', 'webhook', 'stdout', 'file')
                 OR COALESCE(config ->> 'wire_format', 'native') NOT IN
                    ('native', 'cloudevents')
             )
          UNION ALL
          SELECT 'reverse'::TEXT, name
            FROM tide.relay_inbox_config
           WHERE enabled
      ) unsupported;

    IF to_regclass('tide.relay_fanin_config') IS NOT NULL THEN
        SELECT string_agg('fan-in:' || name, ', ' ORDER BY name)
          INTO reverse_state
          FROM tide.relay_fanin_config
         WHERE enabled;
        affected := concat_ws(', ', affected, reverse_state);
        affected := NULLIF(affected, '');
    END IF;

    IF affected IS NOT NULL THEN
        RAISE EXCEPTION USING
            MESSAGE = 'PGTIDE_CONFIG_UNSUPPORTED_SURFACE: ' || affected ||
                      '; last_version=0.48.0; alternative=export, disable, replace, or delete each affected pipeline, then retry; see pg-tide migrate-config and the v0.49.0 migration guide',
            ERRCODE = 'P0001';
    END IF;

    IF to_regclass('tide.relay_fanin_config') IS NOT NULL THEN
        SELECT string_agg(name, ', ' ORDER BY name)
          INTO fanin_state
          FROM tide.relay_fanin_config;
    END IF;
    IF fanin_state IS NOT NULL THEN
        RAISE EXCEPTION USING
            MESSAGE = 'PGTIDE_CONFIG_UNSUPPORTED_SURFACE: fan-in state ' || fanin_state ||
                      '; last_version=0.48.0; alternative=export and delete fan-in state before retrying the v0.49.0 migration',
            ERRCODE = 'P0001';
    END IF;

    IF to_regclass('tide.backfill_jobs') IS NOT NULL THEN
        SELECT string_agg(job_name, ', ' ORDER BY job_name)
          INTO backfill_state
          FROM tide.backfill_jobs;
    END IF;
    IF backfill_state IS NOT NULL THEN
        RAISE EXCEPTION USING
            MESSAGE = 'PGTIDE_CONFIG_UNSUPPORTED_SURFACE: managed backfill state ' || backfill_state ||
                      '; last_version=0.48.0; alternative=export and delete backfill jobs before retrying the v0.49.0 migration',
            ERRCODE = 'P0001';
    END IF;
END
$preflight$;

-- Reverse, fan-in, and managed-backfill SQL entry points are retired.  The
-- IF EXISTS guards keep upgrades from older intermediate installations safe.
DROP FUNCTION IF EXISTS tide.relay_set_inbox_v2(JSONB);
DROP FUNCTION IF EXISTS tide.relay_set_inbox_from_template(TEXT, TEXT, JSONB);
DROP FUNCTION IF EXISTS tide.relay_set_fanin(TEXT, TEXT[], TEXT, JSONB);
DROP FUNCTION IF EXISTS tide.relay_fanin_enable(TEXT);
DROP FUNCTION IF EXISTS tide.relay_fanin_disable(TEXT);
DROP FUNCTION IF EXISTS tide.relay_fanin_delete(TEXT);
DROP FUNCTION IF EXISTS tide.relay_fanin_list();
DROP FUNCTION IF EXISTS tide.backfill_create(TEXT, TEXT, TEXT, BIGINT, BIGINT, INTEGER, INTEGER);
DROP FUNCTION IF EXISTS tide.backfill_pause(TEXT);
DROP FUNCTION IF EXISTS tide.backfill_resume(TEXT);
DROP FUNCTION IF EXISTS tide.backfill_status(TEXT);
DROP FUNCTION IF EXISTS tide.backfill_progress(TEXT);
DROP FUNCTION IF EXISTS tide.backfill_cancel(TEXT);

-- The retention view had a fan-in participant branch.  Keep its public shape,
-- but make the supported participant set independent of the retired catalog.
DROP VIEW IF EXISTS tide.outbox_retention_status;
CREATE VIEW tide.outbox_retention_status AS
SELECT
    c.outbox_name,
    c.retention_hours,
    COUNT(m.id)::BIGINT AS retained_rows,
    COALESCE(SUM(pg_column_size(m.*)), 0)::BIGINT AS retained_bytes,
    MIN(m.created_at) AS oldest_retained_at,
    MAX(m.created_at) AS newest_retained_at,
    now() - make_interval(hours => c.retention_hours) AS retention_cutoff,
    COUNT(m.id) FILTER (WHERE p.safe_offset IS NULL OR m.id > p.safe_offset)::BIGINT AS pending_messages,
    COUNT(m.id)::BIGINT AS total_messages,
    EXTRACT(epoch FROM now() - MIN(m.created_at) FILTER (
        WHERE p.safe_offset IS NULL OR m.id > p.safe_offset
    ))::DOUBLE PRECISION AS oldest_pending_age_seconds,
    p.participant_count,
    p.safe_offset,
    (SELECT COUNT(*)::BIGINT
       FROM (SELECT 1 FROM tide.tide_outbox_messages e
              WHERE e.outbox_name = c.outbox_name
                AND e.created_at < now() - make_interval(hours => c.retention_hours)
                AND (p.safe_offset IS NULL OR e.id <= p.safe_offset)
              LIMIT 10001) bounded) AS eligible_rows,
    COALESCE(p.participants, '[]'::JSONB) AS blockers,
    s.highest_deleted_id,
    s.last_success_at,
    s.last_batch_rows,
    s.total_rows_deleted,
    s.last_duration_ms,
    s.last_partition_action,
    sc.storage_layout,
    CASE WHEN sc.storage_layout = 'id_range' THEN COALESCE((
        SELECT COALESCE(pg_stat_get_live_tuples(child.oid), 0)::BIGINT
          FROM pg_inherits i
          JOIN pg_class child ON child.oid = i.inhrelid
         WHERE i.inhparent = 'tide.tide_outbox_messages'::regclass
           AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
    ), 0) ELSE 0 END AS default_partition_rows
FROM tide.tide_outbox_config c
LEFT JOIN tide.tide_outbox_messages m ON m.outbox_name = c.outbox_name
LEFT JOIN tide.outbox_cleanup_state s ON s.outbox_name = c.outbox_name
LEFT JOIN tide.outbox_storage_config sc ON sc.singleton
LEFT JOIN LATERAL (
    WITH relay_participants AS (
        SELECT c2.name::TEXT AS participant, c2.enabled,
               COALESCE(MIN(o.last_change_id), 0)::BIGINT AS safe_offset
          FROM tide.relay_outbox_config c2
          LEFT JOIN tide.relay_consumer_offsets o
            ON o.pipeline_id = c2.name AND o.outbox_name = c.outbox_name
         WHERE c2.config #>> '{source,outbox}' = c.outbox_name
           AND c2.config ->> 'source_type' = 'outbox'
         GROUP BY c2.name, c2.enabled
    ), group_participants AS (
        SELECT g.group_name::TEXT AS participant, TRUE AS enabled,
               COALESCE(MIN(o.committed_offset), 0)::BIGINT AS safe_offset
          FROM tide.tide_consumer_groups g
          LEFT JOIN tide.tide_consumer_offsets o USING (group_name)
         WHERE g.outbox_name = c.outbox_name
         GROUP BY g.group_name
    ), all_participants AS (
        SELECT * FROM relay_participants
        UNION ALL
        SELECT * FROM group_participants
    )
    SELECT COUNT(*)::BIGINT AS participant_count,
           MIN(safe_offset)::BIGINT AS safe_offset,
           jsonb_agg(jsonb_build_object(
               'name', participant, 'enabled', enabled, 'safe_offset', safe_offset
           ) ORDER BY participant) AS participants
      FROM all_participants
) p ON TRUE
GROUP BY c.outbox_name, c.retention_hours, p.participant_count, p.safe_offset,
         p.participants, s.highest_deleted_id, s.last_success_at,
         s.last_batch_rows, s.total_rows_deleted, s.last_duration_ms,
         s.last_partition_action, sc.storage_layout;

COMMENT ON VIEW tide.outbox_retention_status IS
    'v0.49.0: Retention status using native relay and consumer-group participants.';

DROP INDEX IF EXISTS tide.uq_relay_consumer_offsets_fanin;
ALTER TABLE tide.relay_consumer_offsets
    DROP COLUMN IF EXISTS fanin_member;
DROP TABLE IF EXISTS tide.relay_fanin_config;
DROP TABLE IF EXISTS tide.backfill_jobs;

COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.49.0';
