-- pg_tide 0.1.0 — Transactional Outbox, Idempotent Inbox & Relay Catalog
--
-- Extracted from pg_trickle v0.46.0 into a standalone extension.
-- Works with any PostgreSQL 18+ database — pg_trickle is NOT required.
--
-- Schema: tide
-- All pg_tide objects live in the 'tide' schema.

-- ── Outbox Catalog ─────────────────────────────────────────────────────────

-- Named outbox configurations.
CREATE TABLE IF NOT EXISTS tide.tide_outbox_config (
    outbox_name       TEXT        NOT NULL PRIMARY KEY,
    retention_hours   INT         NOT NULL DEFAULT 24,
    inline_threshold  INT         NOT NULL DEFAULT 10000,
    enabled           BOOLEAN     NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.tide_outbox_config IS
    'TIDE-1 (v0.1.0): Named outbox configurations. One row per logical outbox.';

-- Outbox messages (all named outboxes share this table, discriminated by outbox_name).
CREATE TABLE IF NOT EXISTS tide.tide_outbox_messages (
    id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    outbox_name    TEXT        NOT NULL
                               REFERENCES tide.tide_outbox_config(outbox_name)
                               ON DELETE CASCADE,
    payload        JSONB,
    headers        JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at    TIMESTAMPTZ,
    consumer_group TEXT
);

CREATE INDEX IF NOT EXISTS idx_tide_outbox_messages_pending
    ON tide.tide_outbox_messages (outbox_name, id)
    WHERE consumed_at IS NULL;

COMMENT ON TABLE tide.tide_outbox_messages IS
    'TIDE-1 (v0.1.0): Outbox message store. All named outboxes share this table.';

-- ── Consumer Groups ────────────────────────────────────────────────────────

-- Consumer groups for outbox consumption.
CREATE TABLE IF NOT EXISTS tide.tide_consumer_groups (
    group_name         TEXT        NOT NULL PRIMARY KEY,
    outbox_name        TEXT        NOT NULL
                                   REFERENCES tide.tide_outbox_config(outbox_name)
                                   ON DELETE CASCADE,
    auto_offset_reset  TEXT        NOT NULL DEFAULT 'earliest'
                                   CHECK (auto_offset_reset IN ('earliest', 'latest', 'none')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tide_consumer_groups_outbox
    ON tide.tide_consumer_groups (outbox_name);

COMMENT ON TABLE tide.tide_consumer_groups IS
    'TIDE-B1 (v0.1.0): Named consumer groups for outbox consumption.';

-- Per-consumer committed offsets.
CREATE TABLE IF NOT EXISTS tide.tide_consumer_offsets (
    group_name        TEXT        NOT NULL
                                  REFERENCES tide.tide_consumer_groups(group_name)
                                  ON DELETE CASCADE,
    consumer_id       TEXT        NOT NULL,
    committed_offset  BIGINT      NOT NULL DEFAULT 0,
    last_heartbeat    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_name, consumer_id)
);

COMMENT ON TABLE tide.tide_consumer_offsets IS
    'TIDE-B2 (v0.1.0): Per-consumer committed offsets and heartbeat tracking.';

-- Visibility leases for in-flight batches.
CREATE TABLE IF NOT EXISTS tide.tide_consumer_leases (
    group_name   TEXT        NOT NULL,
    consumer_id  TEXT        NOT NULL,
    lease_start  BIGINT      NOT NULL,
    lease_end    BIGINT      NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (group_name, consumer_id),
    FOREIGN KEY (group_name, consumer_id)
        REFERENCES tide.tide_consumer_offsets(group_name, consumer_id)
        ON DELETE CASCADE
);

COMMENT ON TABLE tide.tide_consumer_leases IS
    'TIDE-B3 (v0.1.0): Visibility leases for in-flight outbox message batches.';

-- ── Inbox Catalog ──────────────────────────────────────────────────────────

-- Named inbox configurations.
CREATE TABLE IF NOT EXISTS tide.tide_inbox_config (
    inbox_name                 TEXT        NOT NULL PRIMARY KEY,
    inbox_schema               TEXT        NOT NULL DEFAULT 'tide',
    max_retries                INT         NOT NULL DEFAULT 3,
    processed_retention_hours  INT         NOT NULL DEFAULT 72,
    dlq_retention_hours        INT         NOT NULL DEFAULT 0,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.tide_inbox_config IS
    'TIDE-2 (v0.1.0): Named inbox configurations.';

-- ── Relay Catalog ──────────────────────────────────────────────────────────

-- Forward relay pipeline definitions (outbox → external sink).
CREATE TABLE IF NOT EXISTS tide.relay_outbox_config (
    name     TEXT    NOT NULL PRIMARY KEY,
    enabled  BOOLEAN NOT NULL DEFAULT true,
    config   JSONB   NOT NULL DEFAULT '{}'
);

COMMENT ON TABLE tide.relay_outbox_config IS
    'TIDE-3 (v0.1.0): Forward relay pipeline definitions (outbox → external sink).';

-- Reverse relay pipeline definitions (external source → inbox).
CREATE TABLE IF NOT EXISTS tide.relay_inbox_config (
    name     TEXT    NOT NULL PRIMARY KEY,
    enabled  BOOLEAN NOT NULL DEFAULT true,
    config   JSONB   NOT NULL DEFAULT '{}'
);

COMMENT ON TABLE tide.relay_inbox_config IS
    'TIDE-3 (v0.1.0): Reverse relay pipeline definitions (external source → inbox).';

-- Durable per-pipeline offset tracking for the relay binary.
CREATE TABLE IF NOT EXISTS tide.relay_consumer_offsets (
    relay_group_id  TEXT        NOT NULL,
    pipeline_id     TEXT        NOT NULL,
    last_offset     TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relay_group_id, pipeline_id)
);

COMMENT ON TABLE tide.relay_consumer_offsets IS
    'TIDE-3 (v0.1.0): Durable per-pipeline offset tracking for the pg-tide relay binary.';

-- Notify function called by triggers on relay config changes.
CREATE OR REPLACE FUNCTION tide.relay_config_notify()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'tide_relay_config',
        json_build_object(
            'direction', TG_TABLE_NAME,
            'op', TG_OP,
            'name', COALESCE(NEW.name, OLD.name)
        )::text
    );
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS relay_outbox_config_notify ON tide.relay_outbox_config;
CREATE TRIGGER relay_outbox_config_notify
    AFTER INSERT OR UPDATE OR DELETE ON tide.relay_outbox_config
    FOR EACH ROW EXECUTE FUNCTION tide.relay_config_notify();

DROP TRIGGER IF EXISTS relay_inbox_config_notify ON tide.relay_inbox_config;
CREATE TRIGGER relay_inbox_config_notify
    AFTER INSERT OR UPDATE OR DELETE ON tide.relay_inbox_config
    FOR EACH ROW EXECUTE FUNCTION tide.relay_config_notify();

-- ── Convenience Views ──────────────────────────────────────────────────────

-- Pending messages per outbox (most common monitoring query).
CREATE OR REPLACE VIEW tide.outbox_pending AS
SELECT
    outbox_name,
    COUNT(*)                                    AS pending_count,
    MIN(created_at)                             AS oldest_at,
    MAX(id)                                     AS max_id
FROM tide.tide_outbox_messages
WHERE consumed_at IS NULL
GROUP BY outbox_name;

COMMENT ON VIEW tide.outbox_pending IS
    'TIDE-1 (v0.1.0): Pending (unconsumed) messages per outbox.';

-- Consumer lag per group.
CREATE OR REPLACE VIEW tide.consumer_lag AS
SELECT
    g.group_name,
    g.outbox_name,
    o.consumer_id,
    o.committed_offset,
    (SELECT COALESCE(MAX(id), 0) FROM tide.tide_outbox_messages m
     WHERE m.outbox_name = g.outbox_name) - o.committed_offset AS lag,
    o.last_heartbeat
FROM tide.tide_consumer_groups g
JOIN tide.tide_consumer_offsets o USING (group_name);

COMMENT ON VIEW tide.consumer_lag IS
    'TIDE-B2 (v0.1.0): Per-consumer lag relative to the latest outbox message.';
