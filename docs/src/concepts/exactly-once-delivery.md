# Exactly-Once Delivery

pg_tide provides end-to-end exactly-once delivery semantics by combining three mechanisms: transactional outbox writes, consumer offset tracking, and idempotent inbox deduplication.

---

## The Three Pillars

### 1. Atomic Publishing (No Message Loss)

Messages are written to the outbox in the same database transaction as your business data. If the transaction rolls back, the message disappears too. If it commits, the message is guaranteed to be delivered eventually.

### 2. Consumer Offsets (No Re-processing)

The relay tracks its position in the outbox via committed offsets. After delivering a batch and receiving acknowledgment from the sink, the relay commits the offset. On restart, it resumes from the last committed position.

### 3. Idempotent Inbox (No Duplicates)

Even with offset tracking, edge cases exist: the relay might deliver a message, crash before committing the offset, and re-deliver on restart. The inbox's `UNIQUE(event_id)` constraint catches these duplicates — the second insert is a no-op.

---

## The Delivery Flow

```
1. Application:  BEGIN; INSERT data; outbox_publish(); COMMIT;
2. Relay:        Poll outbox → get messages with id > last_committed_offset
3. Relay:        Deliver to sink (e.g., INSERT into inbox with dedup key)
4. Relay:        On success → commit_offset(last_delivered_id)
5. Relay:        Mark outbox messages as consumed
```

If the relay crashes at step 4, it re-reads from the last committed offset and re-delivers. The inbox dedup key prevents double-processing.

---

## Guarantees by Component

| Component | Guarantee | Mechanism |
|-----------|-----------|-----------|
| Outbox publish | Exactly-once write | Same PostgreSQL transaction |
| Relay delivery | At-least-once | Retry until sink acknowledges |
| Inbox receive | Exactly-once processing | UNIQUE constraint on event_id |
| **End-to-end** | **Effectively exactly-once** | All three combined |

---

## Edge Cases Handled

### Relay crash after delivery, before offset commit

The relay restarts and re-delivers the message. The inbox dedup key rejects the duplicate.

### PostgreSQL failover

The relay reconnects with exponential backoff. Advisory locks are automatically released on disconnect, allowing another relay instance to take over.

### Sink temporarily unavailable

The relay retries with backoff. Messages remain pending in the outbox until delivery succeeds.

### Duplicate outbox_publish calls

If your application logic calls `outbox_publish` twice with the same logical event, consider including a deterministic `event_id` in the headers. The inbox dedup key will catch duplicates at the receiving end.

---

## Limitations

- **Cross-sink atomicity** — if a pipeline delivers to multiple sinks, partial delivery is possible. Use separate pipelines per sink for independent exactly-once guarantees.
- **External system semantics** — exactly-once into pg_tide inboxes is guaranteed. For external sinks (Kafka, NATS), the guarantee depends on the sink's acknowledgment semantics.
- **Clock skew** — retention cleanup uses `created_at` timestamps. Extreme clock skew could cause premature cleanup. Use NTP-synchronized hosts.
