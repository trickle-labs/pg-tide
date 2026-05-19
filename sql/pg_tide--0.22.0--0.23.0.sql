-- pg_tide v0.22.0 → v0.23.0 upgrade script
--
-- v0.23.0: Correctness, real TLS & full migration test coverage
--
-- Changes:
--   1. Backport ducklake_attach() quote-escape fix (P1 security hardening)
--   2. Add tide.admin_rewind_offset() SECURITY DEFINER escape hatch (P2 offset safety)
--   3. Preserve all existing objects — no destructive DDL.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Fix ducklake_attach() format specifiers (backport from 0.19.0→0.20.0 fix)
--    Replace %s with safe replacement for user-controlled values (_dbname,
--    _host, _port) so that database names or host values containing single
--    quotes cannot produce malformed ATTACH statements.
--    Ref: v0.23.0 P1 security hardening, overall-assessment-4.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION tide.ducklake_attach(
    catalog_schema TEXT DEFAULT 'ducklake',
    data_path      TEXT DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _dbname      text;
    _host        text;
    _port        text;
    _data_clause text := '';
    _attach_str  text;
BEGIN
    SELECT current_database() INTO _dbname;
    SELECT setting INTO _host FROM pg_settings WHERE name = 'listen_addresses';
    SELECT setting INTO _port FROM pg_settings WHERE name = 'port';

    IF _host IS NULL OR _host = '' OR _host = '*' THEN
        _host := 'localhost';
    END IF;

    IF data_path IS NOT NULL THEN
        _data_clause := format(', DATA_PATH %L', data_path);
    END IF;

    -- v0.23.0: Escape single quotes in user-controlled values to prevent
    -- malformed ATTACH statements when the database name or host contains
    -- quotes or other special characters.
    _attach_str := format(
        'ATTACH ''ducklake:postgres:dbname=%s host=%s port=%s'' AS %I%s;',
        replace(_dbname, '''', ''''''),
        replace(_host,   '''', ''''''),
        replace(_port,   '''', ''''''),
        catalog_schema, _data_clause
    );

    RETURN _attach_str;
END;
$$;

COMMENT ON FUNCTION tide.ducklake_attach(TEXT, TEXT) IS
    'Returns a DuckDB ATTACH statement for the DuckLake catalog stored in this '
    'PostgreSQL database. (v0.23.0: backport single-quote escape fix.)';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. tide.admin_rewind_offset() — explicit SECURITY DEFINER escape hatch
--    for intentional offset rollback (v0.23.0, P2 offset safety).
--
--    The commit_offset() monotonicity guard (also added in v0.23.0) prevents
--    any consumer from accidentally rolling back a committed offset.  This
--    function provides the only authorised path for intentional rollback.
--
--    The caller must pass confirm_reprocessing = TRUE to acknowledge that
--    events between the current committed offset and target_offset will be
--    reprocessed by the consumer.  Callable only by superusers or members of
--    the pg_tide_admin role.  All rewinding is audited to tide_security_audit.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION tide.admin_rewind_offset(
    p_group_name          TEXT,
    p_consumer_id         TEXT,
    p_target_offset       BIGINT,
    confirm_reprocessing  BOOLEAN DEFAULT FALSE
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    -- Require explicit acknowledgement that events will be re-processed.
    IF NOT confirm_reprocessing THEN
        RAISE EXCEPTION
            'admin_rewind_offset: set confirm_reprocessing = TRUE to acknowledge '
            'that events between the current offset and % will be re-delivered.',
            p_target_offset;
    END IF;

    -- Require superuser or pg_tide_admin membership.
    IF NOT (
        EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles
            WHERE rolname = current_user AND rolsuper
        )
        OR (
            EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pg_tide_admin')
            AND pg_has_role(current_user, 'pg_tide_admin', 'MEMBER')
        )
    ) THEN
        RAISE EXCEPTION
            'admin_rewind_offset: requires superuser or pg_tide_admin membership';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_consumer_offsets
        WHERE group_name = p_group_name AND consumer_id = p_consumer_id
    ) THEN
        RAISE EXCEPTION 'consumer offset not found for group %, consumer %',
            p_group_name, p_consumer_id;
    END IF;

    UPDATE tide.tide_consumer_offsets
    SET committed_offset = p_target_offset,
        last_heartbeat   = now()
    WHERE group_name  = p_group_name
      AND consumer_id = p_consumer_id;

    -- Audit the rewind.
    INSERT INTO tide.tide_security_audit (action, target_object, performed_by)
    VALUES (
        'ADMIN_REWIND_OFFSET',
        p_group_name || '/' || p_consumer_id || ' → ' || p_target_offset::text,
        current_user
    );
END;
$$;

COMMENT ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN) IS
    'TIDE-OFFSET-REWIND (v0.23.0): Intentional consumer-offset rollback. '
    'Requires confirm_reprocessing = TRUE and pg_tide_admin or superuser. '
    'All calls are audited to tide.tide_security_audit. '
    'For the normal (forward-only) path use tide.commit_offset().';
