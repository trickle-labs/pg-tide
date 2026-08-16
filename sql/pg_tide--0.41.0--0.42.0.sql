-- pg_tide v0.41.0 -> v0.42.0 migration
--
-- v0.42.0 makes normal offset writes monotonic and provides the only supported
-- offset-decrease paths through audited administrative rewind functions.

CREATE OR REPLACE FUNCTION tide.enforce_consumer_offset_monotonic()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF NEW.committed_offset < 0 THEN
        RAISE EXCEPTION 'consumer offset must be non-negative';
    END IF;

    IF TG_OP <> 'UPDATE' THEN
        RETURN NEW;
    END IF;

    IF NEW.committed_offset < OLD.committed_offset THEN
        IF COALESCE(current_setting('tide.offset_rewind', true), '') <> 'on'
           OR NOT (
               EXISTS (
                   SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = session_user AND rolsuper
               )
               OR (
                   EXISTS (
                       SELECT 1 FROM pg_catalog.pg_roles
                        WHERE rolname = 'pg_tide_admin'
                   )
                   AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER')
               )
           )
        THEN
            RAISE EXCEPTION
                'committed_offset is monotonic; use tide.admin_rewind_offset()';
        END IF;

        INSERT INTO tide.tide_security_audit
            (action, target_object, performed_by)
        VALUES (
            'ADMIN_REWIND_OFFSET',
            format(
                'consumer/%s/%s: %s -> %s',
                OLD.group_name,
                OLD.consumer_id,
                OLD.committed_offset,
                NEW.committed_offset
            ),
            session_user
        );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tide_consumer_offsets_monotonic
    ON tide.tide_consumer_offsets;
CREATE TRIGGER tide_consumer_offsets_monotonic
BEFORE INSERT OR UPDATE ON tide.tide_consumer_offsets
FOR EACH ROW
EXECUTE FUNCTION tide.enforce_consumer_offset_monotonic();

CREATE OR REPLACE FUNCTION tide.enforce_relay_offset_monotonic()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF NEW.last_change_id < 0 THEN
        RAISE EXCEPTION 'relay offset must be non-negative';
    END IF;

    IF TG_OP <> 'UPDATE' THEN
        RETURN NEW;
    END IF;

    IF NEW.last_change_id < OLD.last_change_id THEN
        IF COALESCE(current_setting('tide.offset_rewind', true), '') <> 'on'
           OR NOT (
               EXISTS (
                   SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = session_user AND rolsuper
               )
               OR (
                   EXISTS (
                       SELECT 1 FROM pg_catalog.pg_roles
                        WHERE rolname = 'pg_tide_admin'
                   )
                   AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER')
               )
           )
        THEN
            RAISE EXCEPTION
                'last_change_id is monotonic; use tide.admin_rewind_relay_offset()';
        END IF;

        INSERT INTO tide.tide_security_audit
            (action, target_object, performed_by)
        VALUES (
            'ADMIN_REWIND_RELAY_OFFSET',
            format(
                'relay/%s/%s/%s: %s -> %s',
                OLD.relay_group_id,
                OLD.pipeline_id,
                OLD.outbox_name,
                OLD.last_change_id,
                NEW.last_change_id
            ),
            session_user
        );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS relay_consumer_offsets_monotonic
    ON tide.relay_consumer_offsets;
CREATE TRIGGER relay_consumer_offsets_monotonic
BEFORE INSERT OR UPDATE ON tide.relay_consumer_offsets
FOR EACH ROW
EXECUTE FUNCTION tide.enforce_relay_offset_monotonic();

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
DECLARE
    v_current BIGINT;
BEGIN
    IF NOT confirm_reprocessing THEN
        RAISE EXCEPTION
            'admin_rewind_offset: set confirm_reprocessing = TRUE';
    END IF;
    IF p_target_offset < 0 THEN
        RAISE EXCEPTION 'target offset must be non-negative';
    END IF;
    IF NOT (
        EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles
             WHERE rolname = session_user AND rolsuper
        )
        OR (
            EXISTS (
                SELECT 1 FROM pg_catalog.pg_roles
                 WHERE rolname = 'pg_tide_admin'
            )
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER')
        )
    ) THEN
        RAISE EXCEPTION
            'admin_rewind_offset: requires superuser or pg_tide_admin membership';
    END IF;

    SELECT committed_offset
      INTO v_current
      FROM tide.tide_consumer_offsets
     WHERE group_name = p_group_name
       AND consumer_id = p_consumer_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'consumer offset not found for group %, consumer %',
            p_group_name,
            p_consumer_id;
    END IF;
    IF p_target_offset > v_current THEN
        RAISE EXCEPTION
            'target offset % is ahead of current offset %',
            p_target_offset,
            v_current;
    END IF;

    PERFORM set_config('tide.offset_rewind', 'on', true);
    UPDATE tide.tide_consumer_offsets
       SET committed_offset = p_target_offset,
           last_heartbeat = now()
     WHERE group_name = p_group_name
       AND consumer_id = p_consumer_id;
END;
$$;

CREATE OR REPLACE FUNCTION tide.admin_rewind_relay_offset(
    p_relay_group_id      TEXT,
    p_pipeline_id         TEXT,
    p_outbox_name         TEXT,
    p_target_offset       BIGINT,
    confirm_reprocessing  BOOLEAN DEFAULT FALSE
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_current BIGINT;
    v_enabled BOOLEAN;
    v_tenant TEXT;
    v_direction TEXT;
BEGIN
    IF NOT confirm_reprocessing THEN
        RAISE EXCEPTION
            'admin_rewind_relay_offset: set confirm_reprocessing = TRUE';
    END IF;
    IF p_target_offset < 0 THEN
        RAISE EXCEPTION 'target offset must be non-negative';
    END IF;
    IF NOT (
        EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles
             WHERE rolname = session_user AND rolsuper
        )
        OR (
            EXISTS (
                SELECT 1 FROM pg_catalog.pg_roles
                 WHERE rolname = 'pg_tide_admin'
            )
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER')
        )
    ) THEN
        RAISE EXCEPTION
            'admin_rewind_relay_offset: requires superuser or pg_tide_admin membership';
    END IF;

    SELECT enabled, COALESCE(tenant_name, 'default'), 'forward'
      INTO v_enabled, v_tenant, v_direction
      FROM tide.relay_outbox_config
     WHERE name = p_pipeline_id;
    IF NOT FOUND THEN
        SELECT enabled, COALESCE(tenant_name, 'default'), 'reverse'
          INTO v_enabled, v_tenant, v_direction
          FROM tide.relay_inbox_config
         WHERE name = p_pipeline_id;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'relay pipeline % not found', p_pipeline_id;
    END IF;
    IF v_enabled THEN
        RAISE EXCEPTION 'relay pipeline % must be disabled before rewind', p_pipeline_id;
    END IF;

    -- This transaction-scoped lock conflicts with the worker's session lock.
    IF NOT pg_try_advisory_xact_lock(
        hashtext(p_relay_group_id),
        hashtext(v_tenant || ':' || v_direction || ':' || p_pipeline_id)
    ) THEN
        RAISE EXCEPTION
            'relay pipeline % is still owned; drain it before rewinding',
            p_pipeline_id;
    END IF;

    SELECT last_change_id
      INTO v_current
      FROM tide.relay_consumer_offsets
     WHERE relay_group_id = p_relay_group_id
       AND pipeline_id = p_pipeline_id
       AND outbox_name = p_outbox_name
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'relay offset not found for group %, pipeline %, outbox %',
            p_relay_group_id,
            p_pipeline_id,
            p_outbox_name;
    END IF;
    IF p_target_offset > v_current THEN
        RAISE EXCEPTION
            'target offset % is ahead of current offset %',
            p_target_offset,
            v_current;
    END IF;

    PERFORM set_config('tide.offset_rewind', 'on', true);
    UPDATE tide.relay_consumer_offsets
       SET last_change_id = p_target_offset,
           updated_at = now()
     WHERE relay_group_id = p_relay_group_id
       AND pipeline_id = p_pipeline_id
       AND outbox_name = p_outbox_name;
END;
$$;

REVOKE ALL ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN)
    FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pg_tide_admin'
    ) THEN
        GRANT EXECUTE ON FUNCTION
            tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN)
            TO pg_tide_admin;
        GRANT EXECUTE ON FUNCTION
            tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN)
            TO pg_tide_admin;
    END IF;
END;
$$;

COMMENT ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN) IS
    'v0.42.0: Audited, confirmed consumer-group rewind. Uses session_user '
    'authorization and row locking; normal commits remain monotonic.';

COMMENT ON FUNCTION tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN) IS
    'v0.42.0: Audited, confirmed native relay rewind. The pipeline must be '
    'disabled and no worker ownership session may hold its canonical lock.';

COMMENT ON EXTENSION pg_tide IS
    'Transactional outbox, idempotent inbox, and relay catalog for PostgreSQL — v0.42.0';
