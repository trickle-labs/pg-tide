-- pg_tide upgrade: 0.32.0 → 0.33.0
--
-- v0.33.0 — Pre-GA Supply-Chain Hardening, KMS Foundation & v1.0 Readiness
--
-- Changes:
--   • Extension version comment updated.
--   • tide.outbox_encryption_config catalog table and SQL skeleton added
--     (ADR-010 — full implementation in v1.0.0).
--   • Deprecation warnings now present in relay_set_outbox() and
--     relay_set_inbox() positional forms (already active via Rust #[pg_extern]).
--   • Stability guarantee: all v_2 SQL API forms are declared stable;
--     positional forms emit WARNING on every call until v1.0.0 removal.

-- ── Envelope encryption catalog table (ADR-010 skeleton) ─────────────────
--
-- Stores per-outbox KMS encryption configuration.
-- v0.33.0: table is created; the relay-side encryption implementation
-- ships in v1.0.0.  The SQL function skeleton raises a NOTICE so operators
-- can reference the API in migration scripts and documentation.

CREATE TABLE IF NOT EXISTS tide.outbox_encryption_config (
    outbox_name  TEXT        NOT NULL PRIMARY KEY,
    kms_provider TEXT        NOT NULL,
    key_id       TEXT        NOT NULL,
    algorithm    TEXT        NOT NULL DEFAULT 'AES256GCM',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.outbox_encryption_config IS
    'Per-outbox KMS envelope encryption configuration (ADR-010). '
    'Full relay implementation ships in v1.0.0.';

-- ── tide.outbox_encryption_config() SQL skeleton ──────────────────────────
--
-- Stores or updates the KMS encryption configuration for an outbox.
-- In v0.33.0 this function records the configuration but the relay does NOT
-- yet encrypt payloads — that implementation ships with v1.0.0.

CREATE OR REPLACE FUNCTION tide.outbox_encryption_config(
    outbox_name  TEXT,
    kms_provider TEXT,
    key_id       TEXT,
    algorithm    TEXT DEFAULT 'AES256GCM'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF outbox_name IS NULL OR outbox_name = '' THEN
        RAISE EXCEPTION 'outbox_name must not be empty';
    END IF;
    IF kms_provider NOT IN ('aws', 'gcp', 'vault', 'local') THEN
        RAISE EXCEPTION
            'kms_provider must be one of: aws, gcp, vault, local (got %)',
            kms_provider;
    END IF;
    IF key_id IS NULL OR key_id = '' THEN
        RAISE EXCEPTION 'key_id must not be empty';
    END IF;

    INSERT INTO tide.outbox_encryption_config
        (outbox_name, kms_provider, key_id, algorithm, updated_at)
    VALUES
        (outbox_name, kms_provider, key_id, algorithm, now())
    ON CONFLICT (outbox_name) DO UPDATE
        SET kms_provider = EXCLUDED.kms_provider,
            key_id       = EXCLUDED.key_id,
            algorithm    = EXCLUDED.algorithm,
            updated_at   = now();

    RAISE NOTICE
        'Encryption configuration stored for outbox %. '
        'NOTE: relay-side encryption is not yet active — '
        'payloads will be stored in plaintext until pg_tide v1.0.0 is installed.',
        outbox_name;
END;
$$;

COMMENT ON FUNCTION tide.outbox_encryption_config(TEXT, TEXT, TEXT, TEXT) IS
    'Configure KMS envelope encryption for an outbox (ADR-010 skeleton). '
    'Full relay-side encryption implementation ships in v1.0.0.';

-- Grant EXECUTE to tide_admin role (created in v0.13.0).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tide_admin') THEN
        GRANT EXECUTE ON FUNCTION tide.outbox_encryption_config(TEXT, TEXT, TEXT, TEXT)
            TO tide_admin;
    END IF;
END;
$$;

-- Update extension version marker.
COMMENT ON EXTENSION pg_tide IS 'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.33.0';
