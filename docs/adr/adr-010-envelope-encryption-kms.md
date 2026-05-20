# ADR-010: Envelope Encryption with KMS

**Status:** Accepted  
**Date:** 2026-05-20  
**Deciders:** pg-tide core team  
**Supersedes:** —  
**Superseded by:** —

---

## Context

pg_tide stores event payloads in PostgreSQL as plaintext JSONB.  For regulated
workloads (HIPAA, PCI-DSS, SOC 2 Type II) operators need payload confidentiality
at rest without sacrificing transactional guarantees or message broker compatibility.
The v1.0.0 release introduces optional envelope encryption as the first step toward
meeting those compliance requirements.

Three design constraints must be satisfied simultaneously:

1. **Transactional integrity** — encrypted payloads must be committed atomically with
   the outbox row.  No external key-management call should participate in the
   PostgreSQL transaction.
2. **Performance** — a KMS round-trip per message would be prohibitively slow for
   high-frequency event streams.  The design must cache data-encryption keys (DEKs)
   in relay memory.
3. **Backward compatibility** — existing unencrypted outboxes must continue to work
   without any configuration change.  Encryption is opt-in per outbox.

---

## Decision

### Encryption model: envelope encryption

Each message is encrypted individually with a unique data-encryption key (DEK):

```
plaintext  ──AES-256-GCM──►  ciphertext
                 ▲
             per-message DEK (32 random bytes)
                 │
             KMS CMK encrypt
                 ▼
             encrypted_dek  (stored alongside ciphertext in outbox payload)
```

The outbox payload column stores a versioned JSON envelope:

```jsonc
{
  "_enc": 1,                          // envelope version
  "kms": "aws",                       // KMS provider identifier
  "kid": "arn:aws:kms:...",           // KMS CMK key ID / alias ARN
  "alg": "AES256GCM",                 // data-encryption algorithm
  "iv":  "<base64 12-byte nonce>",    // AES-GCM initialization vector
  "edek": "<base64 encrypted DEK>",   // KMS-wrapped data-encryption key
  "ct":  "<base64 ciphertext>"        // encrypted payload
}
```

Unencrypted messages contain no `_enc` field; the relay detects the absence and
passes the payload through without decryption, preserving full backward compatibility.

### KMS provider interface

The relay defines a `EncryptionEnvelope` trait with four implementations:

| Provider    | Feature flag  | CMK reference format              |
|-------------|---------------|-----------------------------------|
| AWS KMS     | `kms-aws`     | ARN or alias (e.g. `alias/my-key`) |
| GCP Cloud KMS | `kms-gcp`   | Resource name                     |
| HashiCorp Vault | `kms-vault` | Transit engine path / key name   |
| Local key file | `kms-local` | Path to a 32-byte hex key file (development only) |

All providers are gated on the top-level `kms` Cargo feature.  The default build
ships without any KMS dependencies.

### DEK caching

The relay maintains an in-memory DEK cache keyed by `(provider, kid)` with a
configurable TTL (default: 5 minutes).  On a cache hit no KMS call is made; only
the first message after startup or TTL expiry triggers a KMS `GenerateDataKey`
call.  This limits KMS API costs to at most `N_pipelines / TTL_seconds` calls per
second under normal operation.

Cache entries are zeroized on eviction using the `zeroize` crate to prevent
residual plaintext key material in heap memory after eviction.

### Key rotation

Key rotation follows a "double-envelope" strategy:

1. The operator configures a new CMK version (or a new key alias) in the
   `outbox_encryption_config()` catalog entry.
2. The relay detects the new CMK on the next DEK cache refresh and begins
   encrypting new messages with the new CMK.
3. A background `pg-tide rotate-keys` command re-encrypts historical messages
   in place (fetch → decrypt with old DEK → re-encrypt with new DEK → update)
   in bounded chunks without relay downtime.
4. After all historical messages are re-encrypted the old CMK can be scheduled
   for deletion in the KMS console.

### SQL catalog API (v0.33.0 skeleton)

```sql
-- v0.33.0: skeleton only — full implementation in v1.0.0.
SELECT tide.outbox_encryption_config(
  outbox_name  => 'my-outbox',
  kms_provider => 'aws',
  key_id       => 'arn:aws:kms:us-east-1:123456789012:alias/my-key',
  algorithm    => 'AES256GCM'   -- default
);
```

The function stores the encryption configuration in a new
`tide.outbox_encryption_config` catalog table.  In v0.33.0 the function raises
a `NOTICE` explaining that encryption is implemented in v1.0.0; the table and
API surface are established so migration guides and documentation can reference
them before the implementation sprint.

---

## Options considered

### Option A: Column-level encryption via pgcrypto

**Pros:** No external dependency; encryption lives entirely in PostgreSQL.  
**Cons:** Key management is entirely the operator's responsibility; no KMS
integration; key rotation requires a full table rewrite; the DEK is unavoidably
visible to any superuser.  Rejected because it does not meet the compliance
bar for KMS-backed key management.

### Option B: Application-level encryption before `outbox_publish()`

**Pros:** Zero extension changes; operators full control.  
**Cons:** Every producer must implement and rotate keys consistently; there is no
catalog-level visibility into which outboxes are encrypted; the relay cannot
re-encrypt on rotation without producer cooperation.  Rejected because it fragments
the key-management responsibility outside pg-tide's operational surface.

### Option C: Envelope encryption in the relay (chosen)

**Pros:** Encryption is transparent to producers; the relay manages DEK lifecycle;
KMS integration is centralized; unencrypted outboxes require no changes.  
**Cons:** The relay must hold plaintext DEKs in memory (mitigated by zeroize and
short TTL); requires KMS connectivity from the relay host.

---

## Consequences

### Positive

- Payload confidentiality at rest for regulated workloads without schema changes
  on the producer side.
- Centralized key management via industry-standard KMS providers.
- Opt-in per outbox: mixed encrypted/unencrypted deployments are fully supported.
- Envelope versioning (`_enc: 1`) allows future algorithm negotiation without
  breaking changes.

### Negative / trade-offs

- The relay process holds plaintext DEKs in memory for up to the cache TTL.
  Operators who require hardware-level key isolation should use a HSM-backed KMS
  provider.
- `_enc: 1` messages cannot be consumed by pg_tide relay versions older than v1.0.0
  without a schema-aware proxy.  Downgrade to v0.x after enabling encryption
  requires disabling encryption first and re-encrypting (or accepting plaintext
  re-ingestion for a replay window).
- KMS API costs scale with the number of active pipelines and the DEK TTL.

---

## Implementation notes

- `pg-tide-relay/src/encryption.rs` — `EncryptionEnvelope` trait + four provider
  structs (`AwsKms`, `GcpKms`, `VaultKms`, `LocalKeyFile`).  Gated on `feature = "kms"`.
  All provider `impl` blocks contain `todo!()` in v0.33.0; fully implemented in v1.0.0.
- `pg-tide-ext/src/outbox.rs` — `tide.outbox_encryption_config()` SQL skeleton added
  via migration `sql/pg_tide--0.32.0--0.33.0.sql`.
- Wire format encoder checks `_enc` presence before attempting decryption; the
  check is a no-op when the `kms` feature is not compiled.
- `docs/src/operations/encryption.md` — operator guide for enabling and rotating
  encryption keys; written as part of the v1.0.0 implementation sprint.

---

## References

- [ADR-003: Wire Format Abstraction](adr-003-wire-format-abstraction.md)
- [ADR-005: Feature-Gated Binary](adr-005-feature-gated-binary.md)
- [NIST SP 800-57 Key Management Guidelines](https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-5/final)
- [AWS KMS Envelope Encryption](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#enveloping)
- [GCP Cloud KMS Envelope Encryption](https://cloud.google.com/kms/docs/envelope-encryption)
