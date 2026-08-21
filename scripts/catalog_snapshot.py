#!/usr/bin/env python3
"""Emit a stable logical snapshot of the pg_tide PostgreSQL catalog."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


SQL = r"""
WITH extension_row AS (
    SELECT e.oid, e.extname, e.extversion, e.extcondition, e.extconfig
      FROM pg_extension e
     WHERE e.extname = 'pg_tide'
)
SELECT jsonb_build_object(
    'extension', COALESCE((
        SELECT jsonb_build_object(
            'name', e.extname,
            'version', e.extversion,
            'config', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                           'schema', n.nspname,
                           'table', c.relname,
                           'condition', e.extcondition[s.i]
                       ) ORDER BY n.nspname, c.relname)
                  FROM generate_subscripts(e.extconfig, 1) s(i)
                  JOIN pg_class c ON c.oid = e.extconfig[s.i]
                  JOIN pg_namespace n ON n.oid = c.relnamespace
            ), '[]'::jsonb)
        )
          FROM extension_row e
    ), '{}'::jsonb),
    'schemas', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'name', n.nspname,
                   'owner', pg_get_userbyid(n.nspowner),
                   'acl', COALESCE((
                       SELECT jsonb_agg(jsonb_build_object(
                                  'grantee', CASE WHEN x.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
                                  'privilege', x.privilege_type,
                                  'grantable', x.is_grantable
                              ) ORDER BY x.grantee, x.privilege_type, x.is_grantable)
                         FROM aclexplode(n.nspacl) x
                   ), '[]'::jsonb)
               ) ORDER BY n.nspname)
          FROM pg_namespace n
         WHERE n.nspname = 'tide'
    ), '[]'::jsonb),
    'relations', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'schema', n.nspname,
                   'name', c.relname,
                   'kind', c.relkind,
                   'persistence', c.relpersistence,
                   'owner', pg_get_userbyid(c.relowner),
                   'acl', COALESCE((
                       SELECT jsonb_agg(jsonb_build_object(
                                  'grantee', CASE WHEN x.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee) END,
                                  'privilege', x.privilege_type,
                                  'grantable', x.is_grantable
                              ) ORDER BY x.grantee, x.privilege_type, x.is_grantable)
                         FROM aclexplode(c.relacl) x
                   ), '[]'::jsonb),
                   'comment', obj_description(c.oid, 'pg_class'),
                   'columns', COALESCE((
                       SELECT jsonb_agg(jsonb_build_object(
                                  'name', a.attname,
                                  'position', a.attnum,
                                  'type', format_type(a.atttypid, a.atttypmod),
                                  'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE format('%I.%I', cn.nspname, co.collname) END,
                                  'nullable', NOT a.attnotnull,
                                  'identity', a.attidentity,
                                  'generated', a.attgenerated,
                                  'default', CASE WHEN d.adbin IS NULL THEN NULL ELSE pg_get_expr(d.adbin, d.adrelid) END,
                                  'comment', col_description(a.attrelid, a.attnum)
                              ) ORDER BY a.attnum)
                         FROM pg_attribute a
                         LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                         LEFT JOIN pg_collation co ON co.oid = a.attcollation
                         LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
                        WHERE a.attrelid = c.oid
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                   ), '[]'::jsonb)
               ) ORDER BY n.nspname, c.relname)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'tide'
           AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
    ), '[]'::jsonb),
    'functions', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'schema', n.nspname,
                   'name', p.proname,
                   'identity_arguments', pg_get_function_identity_arguments(p.oid),
                   'result', pg_get_function_result(p.oid),
                   'language', l.lanname,
                   'volatility', CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' ELSE 'volatile' END,
                   'parallel', CASE p.proparallel WHEN 's' THEN 'safe' WHEN 'r' THEN 'restricted' ELSE 'unsafe' END,
                   'strict', p.proisstrict,
                   'security_definer', p.prosecdef,
                   'owner', pg_get_userbyid(p.proowner),
                   'comment', obj_description(p.oid, 'pg_proc')
               ) ORDER BY n.nspname, p.proname, pg_get_function_identity_arguments(p.oid))
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
          JOIN pg_language l ON l.oid = p.prolang
         WHERE n.nspname = 'tide'
    ), '[]'::jsonb),
    'indexes', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'schema', ns.nspname,
                   'table', tbl.relname,
                   'name', idx.relname,
                   'definition', pg_get_indexdef(i.indexrelid),
                   'predicate', pg_get_expr(i.indpred, i.indrelid),
                   'valid', i.indisvalid,
                   'ready', i.indisready,
                   'comment', obj_description(idx.oid, 'pg_class')
               ) ORDER BY ns.nspname, tbl.relname, idx.relname)
          FROM pg_index i
          JOIN pg_class idx ON idx.oid = i.indexrelid
          JOIN pg_class tbl ON tbl.oid = i.indrelid
          JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
         WHERE ns.nspname = 'tide'
    ), '[]'::jsonb),
    'constraints', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'schema', n.nspname,
                   'table', c.relname,
                   'name', con.conname,
                   'type', con.contype,
                   'definition', pg_get_constraintdef(con.oid, true),
                   'validated', con.convalidated,
                   'comment', obj_description(con.oid, 'pg_constraint')
               ) ORDER BY n.nspname, c.relname, con.conname)
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'tide'
    ), '[]'::jsonb),
    'triggers', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
                   'schema', n.nspname,
                   'table', c.relname,
                   'name', t.tgname,
                   'definition', pg_get_triggerdef(t.oid, true),
                   'enabled', t.tgenabled
               ) ORDER BY n.nspname, c.relname, t.tgname)
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'tide'
           AND NOT t.tgisinternal
    ), '[]'::jsonb)
)::text;
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default=os.environ.get("PSQL", "psql"))
    parser.add_argument("--database-url", default=os.environ.get("PGURL"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if shutil.which(args.psql) is None:
        print(f"catalog_snapshot: psql is required: {args.psql}", file=sys.stderr)
        return 2

    command = [args.psql, "--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set", "ON_ERROR_STOP=1"]
    if args.database_url:
        command.append(args.database_url)
    command.extend(["--command", SQL])
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode:
        print("catalog_snapshot: psql failed", file=sys.stderr)
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)
        return result.returncode

    try:
        snapshot = json.loads(result.stdout.strip())
    except json.JSONDecodeError as exc:
        print(f"catalog_snapshot: psql returned invalid JSON: {exc}", file=sys.stderr)
        return 1

    rendered = json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
