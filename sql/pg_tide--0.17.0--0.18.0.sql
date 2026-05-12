-- pg_tide 0.17.0 → 0.18.0
--
-- Summary of changes:
--
--   • relay_enable(name text) now returns BOOLEAN — TRUE if the pipeline was
--     modified, FALSE if the pipeline was not found.  Callers that ignored the
--     previous void return can ignore the new boolean safely.
--
--   • relay_disable(name text) now returns BOOLEAN — same semantics as above.
--
--   • relay_set_outbox_v2(config JSONB) added — single-JSONB-parameter form
--     symmetric with relay_set_inbox_v2().  Accepted keys:
--       name TEXT, outbox TEXT, sink_type TEXT,
--       config JSONB, batch_size INT, enabled BOOL.
--
--   • SSRF guard for ClickHouse, Elasticsearch, and Arrow Flight sinks is
--     relay-side only — no schema migration required.
--
--   • --postgres-url-file CLI flag is relay-side only — no schema migration
--     required.
--
--   • LISTEN hot-reload (tide_relay_config channel) was introduced in v0.15.0;
--     no additional schema changes required in this release.
--
-- pgrx will replace the SQL function signatures for relay_enable,
-- relay_disable, and relay_set_outbox_v2 when the extension is updated.
-- No explicit DROP/CREATE is needed here because pgrx handles that via
-- CREATE OR REPLACE FUNCTION inside the generated SQL module.
--
-- If you are running a manual (non-pgrx) upgrade, apply the following DDL:

-- relay_enable: void → boolean
CREATE OR REPLACE FUNCTION tide.relay_enable(name text)
  RETURNS boolean
  LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  _rows integer;
BEGIN
  UPDATE tide.relay_outbox_config SET enabled = true WHERE name = relay_enable.name;
  GET DIAGNOSTICS _rows = ROW_COUNT;
  IF _rows = 0 THEN
    UPDATE tide.relay_inbox_config SET enabled = true WHERE name = relay_enable.name;
    GET DIAGNOSTICS _rows = ROW_COUNT;
  END IF;
  RETURN _rows > 0;
END;
$$;

-- relay_disable: void → boolean
CREATE OR REPLACE FUNCTION tide.relay_disable(name text)
  RETURNS boolean
  LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  _rows integer;
BEGIN
  UPDATE tide.relay_outbox_config SET enabled = false WHERE name = relay_disable.name;
  GET DIAGNOSTICS _rows = ROW_COUNT;
  IF _rows = 0 THEN
    UPDATE tide.relay_inbox_config SET enabled = false WHERE name = relay_disable.name;
    GET DIAGNOSTICS _rows = ROW_COUNT;
  END IF;
  RETURN _rows > 0;
END;
$$;

-- relay_set_outbox_v2: new single-JSONB-parameter form
CREATE OR REPLACE FUNCTION tide.relay_set_outbox_v2(config jsonb)
  RETURNS void
  LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  _name      text := config->>'name';
  _outbox    text := config->>'outbox';
  _sink_type text := config->>'sink_type';
  _cfg       jsonb := COALESCE(config->'config', '{}'::jsonb);
  _batch     int  := COALESCE((config->>'batch_size')::int, 100);
  _enabled   bool := COALESCE((config->>'enabled')::bool, true);
BEGIN
  INSERT INTO tide.relay_outbox_config (name, outbox_name, sink_type, config, batch_size, enabled)
    VALUES (_name, _outbox, _sink_type,
            jsonb_build_object(
              'source', jsonb_build_object('outbox', _outbox),
              'sink_type', _sink_type,
              'config', _cfg
            ),
            _batch, _enabled)
  ON CONFLICT (name) DO UPDATE
    SET outbox_name = EXCLUDED.outbox_name,
        sink_type   = EXCLUDED.sink_type,
        config      = EXCLUDED.config,
        batch_size  = EXCLUDED.batch_size,
        enabled     = EXCLUDED.enabled;
END;
$$;
