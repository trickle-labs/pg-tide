#!/usr/bin/env python3
"""Rewrite pg-trickle references to pg-tide in plan files."""
import re, sys

files = [
    'plans/relay-cli-phase1.md',
    'plans/relay-cli-phase2.md',
    'plans/relay-cli-phase3.md',
]

for fname in files:
    with open(fname, 'r') as f:
        text = f.read()

    # 1. Link fixes (most specific first)
    text = text.replace(
        '[PLAN_TRANSACTIONAL_OUTBOX_HELPER.md](../patterns/PLAN_TRANSACTIONAL_OUTBOX_HELPER.md)',
        '[README.md](../README.md)')
    text = text.replace(
        '[ROADMAP v0.25.0](../../ROADMAP.md#v0250--relay-cli-pgtrickle-relay)',
        '[README.md](../README.md)')
    text = text.replace('PLAN_RELAY_CLI_PHASE_3.md', 'relay-cli-phase3.md')
    text = text.replace('PLAN_RELAY_CLI_PHASE_2.md', 'relay-cli-phase2.md')
    text = text.replace('PLAN_RELAY_CLI.md',          'relay-cli-phase1.md')

    # 2. Docker image org/version
    text = text.replace('grove/pgtrickle-relay:0.25.0', 'trickle-labs/pg-tide:0.1.0')
    text = text.replace('grove/pgtrickle-relay:',       'trickle-labs/pg-tide:')
    text = re.sub(r'(cargo install )pgtrickle-relay\b', r'\1pg-tide', text)
    text = re.sub(r'(brew install [^\s]*/tap/)pgtrickle-relay\b', r'\1pg-tide', text)

    # 3. Directory references (must come before bare binary)
    text = text.replace('pgtrickle-relay/', 'pg-tide-relay/')

    # 4. Cargo.toml example - [[bin]] name then package name
    text = re.sub(r'(\[\[bin\]\]\s*\nname = ")pgtrickle-relay(")', r'\1pg-tide\2', text)
    text = text.replace('name = "pgtrickle-relay"', 'name = "pg-tide-relay"')

    # 5. Env-var prefix
    text = text.replace('PGTRICKLE_RELAY_', 'PG_TIDE_')

    # 6. SQL NOTIFY/LISTEN channel
    text = text.replace('pgtrickle_relay_config', 'tide_relay_config')

    # 7. SQL role name
    text = text.replace('pgtrickle_relay', 'pg_tide_relay')

    # 8. Schema prefix in SQL/TOML  (must be before bare pgtrickle)
    text = text.replace('pgtrickle.', 'tide.')

    # 9. Remaining binary invocations
    text = text.replace('pgtrickle-relay', 'pg-tide')

    # 10. Product name
    text = text.replace('pg-trickle', 'pg-tide')
    text = text.replace('pg_trickle', 'pg_tide')

    # 11. HTTP header names
    text = text.replace('X-PgTrickle-',          'X-PgTide-')
    text = text.replace('X-Pgtrickle-',          'X-PgTide-')
    text = text.replace('Pgtrickle-Full-Refresh', 'PgTide-Full-Refresh')

    # 12. dbt package name
    text = text.replace('dbt-pgtrickle', 'dbt-pg-tide')

    # 13. SQL migration file reference
    text = text.replace('sql/pg_trickle--0.24.0--0.25.0.sql', 'sql/pg_tide--0.1.0.sql')

    # 14. Uppercase identifiers (BigQuery schema, Snowflake stage)
    text = text.replace('"PGTRICKLE"',      '"PGTIDE"')
    text = text.replace('@PGTRICKLE_STAGE', '@PGTIDE_STAGE')

    # 15. Code / config string values
    text = text.replace('salesforce_to_pgtrickle',     'salesforce_to_pg_tide')
    text = text.replace('pgtrickle-tui',               'pg-tide-tui')
    text = text.replace('pgtrickle/${',                'pgtide/${')
    text = text.replace('pgtrickle_kinesis_checkpoints','tide_kinesis_checkpoints')
    # Quoted "pgtrickle-xxx" values
    text = re.sub(r'"pgtrickle-', '"pg-tide-', text)
    text = re.sub(r"'pgtrickle-", "'pg-tide-", text)
    # Standalone "pgtrickle" as a value
    text = re.sub(r'"pgtrickle"', '"pgtide"', text)

    # 16. Kubernetes app/name labels
    text = text.replace('app: pgtrickle-relay', 'app: pg-tide')

    with open(fname, 'w') as f:
        f.write(text)

    remaining = [(i+1, ln.rstrip()) for i, ln in enumerate(text.splitlines())
                 if 'pgtrickle' in ln.lower() or 'pg-trickle' in ln.lower()]
    if remaining:
        print(f'\n{fname}: {len(remaining)} remaining occurrences:')
        for lineno, line in remaining:
            print(f'  L{lineno}: {line}')
    else:
        print(f'{fname}: clean')

print('\nDone.')
