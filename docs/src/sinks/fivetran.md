# Fivetran webhook compatibility

There is no `sink_type = "fivetran"` in the relay. To send Fivetran-shaped
payloads, configure the generic preview `webhook` sink and provide the target
URL. Signature and retry guarantees are those of the webhook connector, not a
separate Fivetran integration.

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-fivetran',
    'outbox', 'events',
    'sink_type', 'webhook',
    'config', jsonb_build_object(
      'url', '${env:FIVETRAN_WEBHOOK_URL}'
    )
  )
);
```

See the [generated connector matrix](../support/connector-compatibility.md)
before treating this path as production-supported.
