# CLI reference

The `pg-tide` binary runs the PostgreSQL outbox relay and provides focused
diagnostic and recovery commands.

```text
pg-tide run
pg-tide doctor --postgres-url "$DATABASE_URL"
pg-tide status --postgres-url "$DATABASE_URL"
pg-tide config validate --pipeline orders-nats --postgres-url "$DATABASE_URL"
pg-tide config export --postgres-url "$DATABASE_URL"
pg-tide migrate-config --postgres-url "$DATABASE_URL"
pg-tide maintenance sweep --postgres-url "$DATABASE_URL"
pg-tide replay preview --outbox orders --from-id 100 --to-id 200
pg-tide replay dlq-requeue --pipeline orders-nats --dedup-key orders:42:0
```

`--output json` emits the stable v1 envelope for `doctor`, `status`, config
commands, and maintenance sweep. Exit code 0 means success; a non-zero exit
code means the command could not complete.

`migrate-config` is read-only. It inventories catalog rows that would block a
v0.49.0 upgrade and prints `PGTIDE_CONFIG_UNSUPPORTED_SURFACE` with the
supported alternative. Export affected rows before disabling, replacing, or
deleting them.

Global options include `--postgres-url`, `--postgres-url-file`, `--config`,
`--output`, `--log-level`, and `--log-format`. `run` also supports
`--self-test` and `--expect-extension-version` for deployment probes.
