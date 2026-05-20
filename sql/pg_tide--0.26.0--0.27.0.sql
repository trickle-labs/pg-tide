-- pg_tide 0.26.0 → 0.27.0
--
-- v0.27.0: Observability Expansion, CLI Ergonomics & Pre-GA Documentation Polish
--
-- Changes:
--   1. Add optional `description TEXT` column to tide.tide_outbox_config —
--      populated by operators or by future AsyncAPI import; surfaced in the
--      `pg-tide asyncapi export` output for channel descriptions.
--   2. Add optional `description TEXT` column to tide.tide_inbox_config for
--      symmetric catalog completeness.

ALTER TABLE tide.tide_outbox_config
    ADD COLUMN IF NOT EXISTS description TEXT;

ALTER TABLE tide.tide_inbox_config
    ADD COLUMN IF NOT EXISTS description TEXT;
