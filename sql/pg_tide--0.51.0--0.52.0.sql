-- pg_tide 0.51.0 -> 0.52.0
-- Close the direct publisher write path while preserving guarded publishing.

-- Remove the obsolete SECURITY DEFINER helpers that granted table-wide INSERT.
DROP FUNCTION IF EXISTS tide.grant_publish(TEXT, TEXT);
DROP FUNCTION IF EXISTS tide.revoke_publish(TEXT, TEXT);

-- Existing security-definer functions use only the extension schema and system
-- catalog. Lock that path for upgraded catalogs as well as fresh installs.
DO $security$
DECLARE
    function_name TEXT;
BEGIN
    FOR function_name IN
        SELECT p.oid::regprocedure::TEXT
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'tide'
          AND p.prosecdef
    LOOP
        EXECUTE format(
            'ALTER FUNCTION %s SET search_path = pg_catalog, tide',
            function_name
        );
    END LOOP;
END $security$;

-- The Rust implementation remains the only publish implementation. Its
-- SECURITY DEFINER owner supplies table-write privilege after its guards pass.
DO $publish_security$
BEGIN
    IF to_regprocedure('tide.outbox_publish(text,jsonb,jsonb)') IS NOT NULL THEN
        ALTER FUNCTION tide.outbox_publish(TEXT, JSONB, JSONB)
            SECURITY DEFINER
            SET search_path = pg_catalog, tide;
        REVOKE ALL ON FUNCTION tide.outbox_publish(TEXT, JSONB, JSONB) FROM PUBLIC;
    END IF;
END $publish_security$;

DO $publisher_grant$
BEGIN
    IF to_regrole('tide_publisher') IS NOT NULL
       AND to_regprocedure('tide.outbox_publish(text,jsonb,jsonb)') IS NOT NULL THEN
        GRANT EXECUTE ON FUNCTION tide.outbox_publish(TEXT, JSONB, JSONB)
            TO tide_publisher;
    END IF;
END $publisher_grant$;

DO $legacy_grants$
DECLARE
    grantee TEXT;
BEGIN
    FOR grantee IN
        SELECT DISTINCT CASE
            WHEN a.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(a.grantee))
        END
        FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE c.oid = 'tide.tide_outbox_messages'::pg_catalog.regclass
          AND a.privilege_type = 'INSERT'
    LOOP
        EXECUTE format(
            'REVOKE INSERT ON tide.tide_outbox_messages FROM %s',
            grantee
        );
    END LOOP;

    REVOKE INSERT ON tide.tide_outbox_messages FROM PUBLIC;
    IF to_regclass('tide.tide_outbox_messages_id_seq') IS NOT NULL THEN
        FOR grantee IN
            SELECT DISTINCT CASE
                WHEN a.grantee = 0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(a.grantee))
            END
            FROM pg_catalog.pg_class c
            CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
            WHERE c.oid = 'tide.tide_outbox_messages_id_seq'::pg_catalog.regclass
              AND (a.grantee = 0 OR EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_class m
                  CROSS JOIN LATERAL pg_catalog.aclexplode(m.relacl) i
                  WHERE m.oid = 'tide.tide_outbox_messages'::pg_catalog.regclass
                    AND i.grantee = a.grantee
                    AND i.privilege_type = 'INSERT'
              ))
        LOOP
            EXECUTE format(
                'REVOKE ALL ON SEQUENCE tide.tide_outbox_messages_id_seq FROM %s',
                grantee
            );
        END LOOP;
        REVOKE ALL ON SEQUENCE tide.tide_outbox_messages_id_seq FROM PUBLIC;
    END IF;
END $legacy_grants$;

COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.52.0';
