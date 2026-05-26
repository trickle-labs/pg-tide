-- pg_tide v0.35.0 → v0.36.0 migration
-- v0.36.0: Remove positional-parameter forms of relay_set_outbox() and
--          relay_set_inbox() (breaking change — deprecated since v0.18.0 /
--          v0.16.0).  Callers must migrate to:
--            • tide.relay_set_outbox_v2(config JSONB)
--            • tide.relay_set_inbox_v2(config JSONB)
--
-- This is the final pre-v1.0.0 release and constitutes the stable API freeze
-- for the tide.* SQL namespace.

-- Drop the 6-parameter positional form of relay_set_outbox().
-- Signature: (text, text, text, jsonb DEFAULT '{}', integer DEFAULT 100, boolean DEFAULT true)
DROP FUNCTION IF EXISTS tide.relay_set_outbox(text, text, text, jsonb, integer, boolean);

-- Drop the 8-parameter positional form of relay_set_inbox().
-- Signature: (text, text, jsonb DEFAULT '{}', integer DEFAULT 100, text DEFAULT 'stdout',
--             boolean DEFAULT true, integer DEFAULT 3, boolean DEFAULT true)
DROP FUNCTION IF EXISTS tide.relay_set_inbox(text, text, jsonb, integer, text, boolean, integer, boolean);

-- Note: tide.relay_set_outbox_v2(jsonb) and tide.relay_set_inbox_v2(jsonb) are
-- implemented as Rust #[pg_extern] functions in the pgrx extension and remain
-- the authoritative API.  No DDL change is required here.
