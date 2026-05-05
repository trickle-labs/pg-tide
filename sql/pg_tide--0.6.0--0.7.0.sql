-- pg_tide 0.6.0 → 0.7.0
-- Dead-letter queue table + SQL API for production-grade relay operations.
-- v0.7.0 adds: DLQ, schema registry support, JMESPath transforms,
-- content-based routing, rate limiting, circuit breaker, SIGHUP reload,
-- dry-run / replay mode, OpenTelemetry tracing, and webhook signature verification.

-- ── Dead-Letter Queue ──────────────────────────────────────────────────────

CREATE TABLE tide.relay_dlq (
    id              BIGSERIAL PRIMARY KEY,
    relay_mode      TEXT NOT NULL,         -- 'forward' | 'reverse'
    pipeline_name   TEXT NOT NULL,         -- pipeline name from relay config
    source_name     TEXT NOT NULL,         -- e.g. 'outbox:order_events'
    sink_name       TEXT NOT NULL,         -- e.g. 'nats'
    dedup_key       TEXT NOT NULL,
    subject         TEXT,
    payload         JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    error_kind      TEXT NOT NULL,         -- 'decode' | 'sink_permanent' | 'inbox_permanent'
    attempt_count   INT NOT NULL DEFAULT 1,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    retried_at      TIMESTAMPTZ,           -- set when manually retried via SQL API
    resolved        BOOLEAN NOT NULL DEFAULT false
);

COMMENT ON TABLE tide.relay_dlq IS
    'Dead-letter queue: stores relay messages that could not be delivered after all retries.';

CREATE INDEX relay_dlq_unresolved_idx
    ON tide.relay_dlq (pipeline_name, error_kind)
    WHERE resolved = false;

CREATE INDEX relay_dlq_last_failed_idx
    ON tide.relay_dlq (last_failed_at);

-- ── DLQ SQL API ────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_dlq_list()
RETURNS SETOF tide.relay_dlq
LANGUAGE sql STABLE SECURITY INVOKER
AS $$
    SELECT *
      FROM tide.relay_dlq
     WHERE resolved = false
     ORDER BY first_failed_at;
$$;

COMMENT ON FUNCTION tide.relay_dlq_list() IS
    'List all unresolved dead-letter queue entries ordered by time of first failure.';

CREATE OR REPLACE FUNCTION tide.relay_dlq_retry(p_id BIGINT)
RETURNS void
LANGUAGE sql
SECURITY INVOKER
AS $$
    UPDATE tide.relay_dlq
       SET retried_at = now(),
           resolved   = false
     WHERE id = p_id;
$$;

COMMENT ON FUNCTION tide.relay_dlq_retry(BIGINT) IS
    'Mark a specific DLQ entry for retry. The relay will pick it up on the next poll.';

CREATE OR REPLACE FUNCTION tide.relay_dlq_retry_all()
RETURNS bigint
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
    cnt bigint;
BEGIN
    UPDATE tide.relay_dlq
       SET retried_at = now()
     WHERE resolved = false
       AND retried_at IS NULL;
    GET DIAGNOSTICS cnt = ROW_COUNT;
    RETURN cnt;
END;
$$;

COMMENT ON FUNCTION tide.relay_dlq_retry_all() IS
    'Mark all unresolved DLQ entries for retry. Returns the number of entries marked.';

CREATE OR REPLACE FUNCTION tide.relay_dlq_purge(retention_days INT DEFAULT 30)
RETURNS bigint
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
DECLARE
    deleted bigint;
BEGIN
    DELETE FROM tide.relay_dlq
     WHERE resolved = true
       AND last_failed_at < now() - (retention_days || ' days')::interval;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$;

COMMENT ON FUNCTION tide.relay_dlq_purge(INT) IS
    'Purge resolved DLQ entries older than retention_days. Returns the number deleted.';

CREATE OR REPLACE FUNCTION tide.relay_dlq_resolve(p_id BIGINT)
RETURNS void
LANGUAGE sql
SECURITY INVOKER
AS $$
    UPDATE tide.relay_dlq
       SET resolved = true
     WHERE id = p_id;
$$;

COMMENT ON FUNCTION tide.relay_dlq_resolve(BIGINT) IS
    'Mark a DLQ entry as resolved (will be purged by relay_dlq_purge).';

CREATE OR REPLACE FUNCTION tide.relay_dlq_stats()
RETURNS TABLE (
    pipeline_name TEXT,
    error_kind    TEXT,
    total         BIGINT,
    unresolved    BIGINT
)
LANGUAGE sql STABLE SECURITY INVOKER
AS $$
    SELECT pipeline_name,
           error_kind,
           COUNT(*)                                       AS total,
           COUNT(*) FILTER (WHERE resolved = false)      AS unresolved
      FROM tide.relay_dlq
     GROUP BY pipeline_name, error_kind
     ORDER BY pipeline_name, error_kind;
$$;

COMMENT ON FUNCTION tide.relay_dlq_stats() IS
    'Summary of DLQ entries grouped by pipeline and error kind.';
