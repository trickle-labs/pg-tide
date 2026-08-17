-- pg_tide v0.44.0 -> v0.45.0: observational relay runtime status.
--
-- The status table is never delivery authority.  Advisory locks, durable
-- offsets, and the v0.42.0 delivery state machine remain authoritative.

CREATE TABLE IF NOT EXISTS tide.relay_runtime_status (
    relay_group_id              TEXT        NOT NULL,
    pipeline_id                 TEXT        NOT NULL,
    direction                   TEXT        NOT NULL
                                           CHECK (direction IN ('forward', 'reverse')),
    tenant_name                 TEXT        NOT NULL DEFAULT 'default',
    owner_token                 TEXT,
    owner_acquired_at           TIMESTAMPTZ,
    last_owner_heartbeat        TIMESTAMPTZ,
    last_checkpoint_success_at  TIMESTAMPTZ,
    last_error_code              TEXT,
    last_error_component         TEXT,
    last_error_class             TEXT
                                           CHECK (last_error_class IS NULL
                                               OR last_error_class IN ('transient', 'permanent')),
    last_error_at                TIMESTAMPTZ,
    retry_attempt                INTEGER     NOT NULL DEFAULT 0
                                           CHECK (retry_attempt >= 0),
    retry_state                  TEXT,
    next_retry_at                TIMESTAMPTZ,
    last_state_update_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relay_group_id, pipeline_id, direction, tenant_name)
);

COMMENT ON TABLE tide.relay_runtime_status IS
    'v0.45.0 observational relay state; advisory locks and durable offsets remain authoritative.';

CREATE INDEX IF NOT EXISTS relay_runtime_status_heartbeat_idx
    ON tide.relay_runtime_status (last_owner_heartbeat)
    WHERE owner_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS relay_runtime_status_error_idx
    ON tide.relay_runtime_status (last_error_at)
    WHERE last_error_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS relay_dlq_unresolved_pipeline_idx
    ON tide.relay_dlq (pipeline_name)
    WHERE resolved = false;

CREATE OR REPLACE VIEW tide.relay_pipeline_status AS
WITH configured AS (
    SELECT
        name AS pipeline_id,
        'forward'::TEXT AS direction,
        enabled,
        COALESCE(tenant_name, 'default') AS tenant_name,
        config #>> '{source,outbox}' AS outbox_name
    FROM tide.relay_outbox_config
    UNION ALL
    SELECT
        name AS pipeline_id,
        'reverse'::TEXT AS direction,
        enabled,
        COALESCE(tenant_name, 'default') AS tenant_name,
        NULL::TEXT AS outbox_name
    FROM tide.relay_inbox_config
),
latest_offsets AS (
    SELECT DISTINCT ON (relay_group_id, pipeline_id, outbox_name, tenant_name)
        relay_group_id,
        pipeline_id,
        outbox_name,
        COALESCE(tenant_name, 'default') AS tenant_name,
        last_change_id,
        updated_at
    FROM tide.relay_consumer_offsets
    ORDER BY relay_group_id, pipeline_id, outbox_name, tenant_name, updated_at DESC
),
dlq_depth AS (
    SELECT pipeline_name, COUNT(*)::BIGINT AS unresolved_dlq_depth
    FROM tide.relay_dlq
    WHERE resolved = false
    GROUP BY pipeline_name
)
SELECT
    c.pipeline_id,
    c.direction,
    c.tenant_name,
    c.enabled,
    COALESCE(s.relay_group_id, 'default') AS relay_group_id,
    CASE
        WHEN NOT c.enabled THEN 'unowned'
        WHEN s.owner_token IS NULL THEN 'unowned'
        WHEN s.last_owner_heartbeat IS NULL
          OR s.last_owner_heartbeat < now() - interval '2 minutes' THEN 'stale'
        ELSE 'owned'
    END AS ownership,
    CASE
        WHEN NOT c.enabled THEN 'disabled'
        WHEN s.last_error_class = 'permanent' THEN 'permanently_failed'
        WHEN s.retry_state IS NOT NULL THEN 'retrying'
        WHEN s.owner_token IS NULL THEN 'unknown'
        ELSE 'healthy'
    END AS health,
    CASE
        WHEN c.outbox_name IS NULL OR o.last_change_id IS NULL THEN NULL::BIGINT
        ELSE (
            SELECT COUNT(*)::BIGINT
            FROM tide.tide_outbox_messages m
            WHERE m.outbox_name = c.outbox_name
              AND m.id > o.last_change_id
        )
    END AS consumer_lag,
    o.last_change_id AS last_offset,
    s.last_checkpoint_success_at,
    s.last_error_code,
    s.last_error_component,
    s.last_error_class,
    s.last_error_at,
    CASE WHEN s.retry_state IS NULL THEN NULL::INTEGER ELSE s.retry_attempt END AS retry_attempt,
    s.retry_state,
    s.next_retry_at,
    COALESCE(d.unresolved_dlq_depth, 0::BIGINT) AS unresolved_dlq_depth,
    s.last_state_update_at
FROM configured c
LEFT JOIN tide.relay_runtime_status s
  ON s.pipeline_id = c.pipeline_id
 AND s.direction = c.direction
 AND s.tenant_name = c.tenant_name
LEFT JOIN latest_offsets o
  ON o.pipeline_id = c.pipeline_id
 AND o.tenant_name = c.tenant_name
 AND (c.outbox_name IS NULL OR o.outbox_name = c.outbox_name)
 AND o.relay_group_id = COALESCE(s.relay_group_id, 'default')
LEFT JOIN dlq_depth d ON d.pipeline_name = c.pipeline_id;

COMMENT ON VIEW tide.relay_pipeline_status IS
    'v0.45.0 sanitized pipeline status; owner tokens and raw errors are intentionally omitted.';

DO $grants$
BEGIN
    REVOKE ALL ON TABLE tide.relay_runtime_status FROM PUBLIC;
    IF to_regrole('tide_reader') IS NOT NULL THEN
        REVOKE ALL ON TABLE tide.relay_runtime_status FROM tide_reader;
    END IF;
    IF to_regrole('tide_operator') IS NOT NULL THEN
        REVOKE ALL ON TABLE tide.relay_runtime_status FROM tide_operator;
    END IF;

    IF to_regrole('tide_admin') IS NOT NULL THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON tide.relay_runtime_status TO tide_admin;
    END IF;
    IF to_regrole('tide_relay') IS NOT NULL THEN
        GRANT SELECT, INSERT, UPDATE ON tide.relay_runtime_status TO tide_relay;
    END IF;
    IF to_regrole('tide_reader') IS NOT NULL THEN
        GRANT SELECT ON tide.relay_pipeline_status TO tide_reader;
    END IF;
    IF to_regrole('tide_operator') IS NOT NULL THEN
        GRANT SELECT ON tide.relay_pipeline_status TO tide_operator;
    END IF;
END $grants$;

COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.45.0';
