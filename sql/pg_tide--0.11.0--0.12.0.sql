-- pg_tide 0.11.0 → 0.12.0
--
-- v0.12.0: Contract correctness & operational tooling.
--
-- Changes:
--   1. Fix relay_consumer_offsets schema:
--      - Add last_change_id BIGINT (replaces last_offset TEXT for typed offsets)
--      - Add worker_id TEXT (identifies which relay instance owns the offset)
--      - Preserve relay_group_id / pipeline_id / updated_at columns
--   2. The old last_offset TEXT column is dropped (no existing relay data uses it
--      since the relay was writing last_change_id which did not exist yet).

-- Add last_change_id (typed offset) replacing last_offset (untyped TEXT).
ALTER TABLE tide.relay_consumer_offsets
    ADD COLUMN IF NOT EXISTS last_change_id BIGINT NOT NULL DEFAULT 0;

-- Add worker_id to track which relay instance holds the offset.
ALTER TABLE tide.relay_consumer_offsets
    ADD COLUMN IF NOT EXISTS worker_id TEXT;

-- Drop the untyped last_offset column that the relay never successfully wrote.
ALTER TABLE tide.relay_consumer_offsets
    DROP COLUMN IF EXISTS last_offset;

COMMENT ON TABLE tide.relay_consumer_offsets IS
    'TIDE-3 (v0.12.0): Durable per-pipeline offset tracking for the pg-tide relay binary.
     last_change_id is a BIGINT offset into tide_outbox_messages.id.
     worker_id identifies which relay instance last updated this offset.';

COMMENT ON COLUMN tide.relay_consumer_offsets.last_change_id IS
    'Highest outbox message id that has been successfully delivered by this pipeline.';

COMMENT ON COLUMN tide.relay_consumer_offsets.worker_id IS
    'Relay instance (hostname:pid or custom) that last updated this row.';
