-- pg_tide upgrade: 0.16.0 → 0.17.0
--
-- v0.17.0 — Catalog Integrity, DLQ Reliability & Contract Correctness
--
-- Changes in this release:
--
--   1. Drop any residual plpgsql duplicates for functions that are now
--      implemented exclusively as Rust #[pg_extern] extensions.
--      (Safe no-op if the plpgsql versions were already absent.)
--
--   2. Harden SECURITY DEFINER helper functions with
--      SET search_path = tide, pg_catalog to prevent search_path injection.
--      Applies to existing databases upgrading from ≤ 0.16.0.
--

-- ── 1. Drop plpgsql duplicates ───────────────────────────────────────────────

-- tide.outbox_truncate_delivered(text) — plpgsql was removed in 0.14.0→0.15.0;
-- this DO block defensively drops it for databases that applied the old script.
DO $$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_proc     p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'tide'
      AND  p.proname = 'outbox_truncate_delivered'
      AND  p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
  ) THEN
    DROP FUNCTION tide.outbox_truncate_delivered(text);
  END IF;
END $$;

-- tide.outbox_create_if_not_exists(text, int, int) — plpgsql removed in 0.15.0→0.16.0.
DO $$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_proc     p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'tide'
      AND  p.proname = 'outbox_create_if_not_exists'
      AND  p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
  ) THEN
    DROP FUNCTION tide.outbox_create_if_not_exists(text, integer, integer);
  END IF;
END $$;

-- tide.relay_set_inbox_v2(jsonb) — plpgsql removed in 0.15.0→0.16.0.
DO $$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_proc     p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'tide'
      AND  p.proname = 'relay_set_inbox_v2'
      AND  p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
  ) THEN
    DROP FUNCTION tide.relay_set_inbox_v2(jsonb);
  END IF;
END $$;

-- tide.outbox_grant_publish(text, text) and tide.outbox_revoke_publish(text, text)
-- were plpgsql in the v0.12.0→0.13.0 migration but are now C-language #[pg_extern]
-- functions provided by the Rust extension.  Drop any plpgsql residual so that
-- the Rust runtime can register the C version cleanly.
DO $$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_proc     p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'tide'
      AND  p.proname = 'outbox_grant_publish'
      AND  p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
  ) THEN
    DROP FUNCTION tide.outbox_grant_publish(text, text);
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_proc     p
    JOIN   pg_namespace n ON n.oid = p.pronamespace
    WHERE  n.nspname = 'tide'
      AND  p.proname = 'outbox_revoke_publish'
      AND  p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
  ) THEN
    DROP FUNCTION tide.outbox_revoke_publish(text, text);
  END IF;
END $$;

-- ── 2. Harden SECURITY DEFINER functions ────────────────────────────────────
--
-- Fresh installs (via pg_tide--0.1.0.sql) already include
-- SET search_path = tide, pg_catalog.  This ALTER ensures upgrades
-- from ≤ 0.16.0 receive the same hardening.

ALTER FUNCTION tide.grant_publish(TEXT, TEXT)
  SET search_path = tide, pg_catalog;

ALTER FUNCTION tide.revoke_publish(TEXT, TEXT)
  SET search_path = tide, pg_catalog;
