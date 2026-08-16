# Fivetran webhook compatibility

pg_tide does not expose a `fivetran` source type or a first-class Fivetran
destination. Fivetran-shaped webhook payloads and signatures can be handled by
the generic `webhook` source, which remains preview until its full production
evidence gate is complete. See the [connector matrix](../support/connector-compatibility.md).

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'fivetran-crm',
    'inbox', 'crm_inbox',
    'source', 'webhook',
    'config', jsonb_build_object(
      'signature_scheme', 'fivetran',
      'signature_secret', '${env:FIVETRAN_API_SECRET}'
    )
  )
);
```

This documents the generic webhook compatibility path, not a separate
connector or support promise.
