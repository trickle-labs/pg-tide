-- pg_tide v0.39.0 → v0.40.0 migration
-- v0.40.0: "One Real Pipeline" — canonical native outbox → NATS JetStream path.
--
-- See ADR-011 (docs/adr/adr-011-canonical-outbox-storage-and-relay-polling.md).
--
-- This migration:
--   1. Adds the unconditional (outbox_name, id) polling index.
--   2. Adds outbox_name to tide.relay_consumer_offsets and rekeys the offset
--      identity to (relay_group_id, pipeline_id, outbox_name).
--   3. Backfills the new outbox_name column from pipeline config / fanin_member.
--   4. Removes confirmed orphan offset rows and fails on ambiguous backfill.
--   5. Disables all fan-in configs (experimental; quarantined in v0.40.0).
--
-- It never rewrites or deletes outbox messages.

-- ── 1. Canonical unconditional polling index ─────────────────────────────────
-- The native relay polls with:
--   SELECT id, payload, headers, created_at
--   FROM tide.tide_outbox_messages
--   WHERE outbox_name = $1 AND id > $2 ORDER BY id LIMIT $3;
-- The pre-existing partial index (WHERE consumed_at IS NULL) does not support
-- this query because the native relay does not filter by consumed_at.
CREATE INDEX IF NOT EXISTS idx_tide_outbox_messages_poll
    ON tide.tide_outbox_messages (outbox_name, id);

COMMENT ON INDEX tide.idx_tide_outbox_messages_poll IS
    'v0.40.0 (ADR-011): Unconditional (outbox_name, id) index for native relay '
    'polling. The native relay does not filter by consumed_at.';

-- ── 2. Add outbox_name to relay_consumer_offsets ─────────────────────────────
ALTER TABLE tide.relay_consumer_offsets
    ADD COLUMN IF NOT EXISTS outbox_name TEXT;

-- ── 3–7. Backfill, orphan removal, ambiguity + duplicate checks, rekey ──────
DO $$
DECLARE
    orphan_count  BIGINT;
    ambig_count   BIGINT;
    dup_count     BIGINT;
BEGIN
    -- 3a. Backfill fan-in member rows: the contributing outbox is fanin_member.
    UPDATE tide.relay_consumer_offsets
       SET outbox_name = fanin_member
     WHERE fanin_member IS NOT NULL
       AND outbox_name IS NULL;

    -- 3b. Backfill simple-pipeline rows from the pipeline's source outbox.
    UPDATE tide.relay_consumer_offsets o
       SET outbox_name = c.config #>> '{source,outbox}'
      FROM tide.relay_outbox_config c
     WHERE o.fanin_member IS NULL
       AND o.outbox_name IS NULL
       AND o.pipeline_id = c.name
       AND c.config #>> '{source,outbox}' IS NOT NULL
       AND c.config #>> '{source,outbox}' <> '';

    -- 5. Remove only confirmed orphan offset rows: simple-pipeline offsets whose
    --    pipeline_id maps to no relay_outbox_config row at all. These cannot be
    --    associated with any outbox and are safe to drop.
    WITH deleted AS (
        DELETE FROM tide.relay_consumer_offsets o
         WHERE o.fanin_member IS NULL
           AND o.outbox_name IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM tide.relay_outbox_config c
                WHERE c.name = o.pipeline_id
           )
        RETURNING 1
    )
    SELECT COUNT(*) INTO orphan_count FROM deleted;
    IF orphan_count > 0 THEN
        RAISE NOTICE 'v0.40.0: removed % orphan relay offset row(s) with no matching pipeline', orphan_count;
    END IF;

    -- 8 (guard). Fail explicitly on ambiguous backfill rather than guessing: a
    --    row whose pipeline exists in relay_outbox_config but whose config has
    --    no source.outbox cannot be mapped to an outbox.
    SELECT COUNT(*) INTO ambig_count
      FROM tide.relay_consumer_offsets
     WHERE outbox_name IS NULL;
    IF ambig_count > 0 THEN
        RAISE EXCEPTION
            'v0.40.0 migration: % relay offset row(s) cannot be mapped to an outbox '
            '(pipeline exists but config has no source.outbox). Resolve these rows '
            'before upgrading; the offset identity cannot be inferred safely.',
            ambig_count;
    END IF;

    -- 6. Reject duplicate offset identities before installing the new key.
    SELECT COALESCE(SUM(cnt - 1), 0) INTO dup_count
      FROM (
        SELECT COUNT(*) AS cnt
          FROM tide.relay_consumer_offsets
         GROUP BY relay_group_id, pipeline_id, outbox_name
        HAVING COUNT(*) > 1
      ) d;
    IF dup_count > 0 THEN
        RAISE EXCEPTION
            'v0.40.0 migration: % duplicate relay offset identity row(s) for '
            '(relay_group_id, pipeline_id, outbox_name). Deduplicate before upgrading.',
            dup_count;
    END IF;

    -- 7. Make outbox_name authoritative (NOT NULL) after successful backfill.
    ALTER TABLE tide.relay_consumer_offsets
        ALTER COLUMN outbox_name SET NOT NULL;

    -- 8. Replace the old (relay_group_id, pipeline_id) primary key with the
    --    outbox-scoped identity (relay_group_id, pipeline_id, outbox_name).
    ALTER TABLE tide.relay_consumer_offsets
        DROP CONSTRAINT IF EXISTS relay_consumer_offsets_pkey;
    ALTER TABLE tide.relay_consumer_offsets
        ADD CONSTRAINT relay_consumer_offsets_pkey
        PRIMARY KEY (relay_group_id, pipeline_id, outbox_name);
END $$;

COMMENT ON COLUMN tide.relay_consumer_offsets.outbox_name IS
    'v0.40.0 (ADR-011): Logical outbox scope of this offset. The native relay '
    'offset identity is (relay_group_id, pipeline_id, outbox_name). A pipeline '
    'name reused for another outbox does not inherit the previous offset.';

-- 9. Preserve fanin_member for compatibility (retained; no drop).
COMMENT ON COLUMN tide.relay_consumer_offsets.fanin_member IS
    'v0.40.0: Retained for fan-in compatibility. Fan-in is experimental and '
    'disabled in v0.40.0; see relay_fanin_config.';

-- ── 10. Quarantine fan-in: disable all fan-in configs ────────────────────────
-- Fan-in is experimental and disabled in v0.40.0 pending canonical shared-table
-- runtime coverage. Catalog rows and offsets are retained for a later release.
UPDATE tide.relay_fanin_config
   SET enabled = FALSE, updated_at = now()
 WHERE enabled = TRUE;

COMMENT ON TABLE tide.relay_fanin_config IS
    'v0.40.0: Multi-outbox fan-in pipeline catalog. Experimental and disabled '
    'in v0.40.0; rows are retained for a future release.';

-- ── 11. Extension version comment ────────────────────────────────────────────
COMMENT ON TABLE tide.tide_outbox_messages IS
    'TIDE-1 (v0.40.0, ADR-011): Canonical native outbox store. All named '
    'outboxes share this table, discriminated by outbox_name. The native relay '
    'polls it directly; consumed_at is legacy/global-consumer state and is not '
    'authoritative for native per-pipeline delivery.';

COMMENT ON TABLE tide.relay_consumer_offsets IS
    'TIDE-3 (v0.40.0, ADR-011): Durable per-pipeline relay offsets. Native '
    'offset identity is (relay_group_id, pipeline_id, outbox_name). '
    'last_change_id is the greatest tide_outbox_messages.id acknowledged by the '
    'sink; writes are monotonic.';

COMMENT ON EXTENSION pg_tide IS 'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.40.0';
