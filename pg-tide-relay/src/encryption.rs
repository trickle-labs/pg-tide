// v0.33.0: Envelope encryption interface skeleton (ADR-010).
//
// This module defines the `EncryptionEnvelope` trait and the four KMS provider
// structs that will implement it in v1.0.0.  All provider `impl` blocks contain
// `todo!()` in this release; the API surface is established here so that:
//   - Migration guides and docs can reference stable type/function names.
//   - The wire-format encoder can check `_enc` presence at compile time when
//     the `kms` feature is enabled, without any runtime cost when disabled.
//   - The v1.0.0 implementation sprint can fill in the bodies without any
//     public-API changes.
//
// Gated on `feature = "kms"`.  The default build ships without any KMS
// dependencies; enabling `kms` (or a sub-feature such as `kms-aws`) brings
// in this module.
//
// See also: docs/adr/adr-010-envelope-encryption-kms.md

#![cfg(feature = "kms")]

use crate::error::RelayError;

// ── Encrypted payload envelope ────────────────────────────────────────────

/// The encrypted payload envelope stored in the outbox instead of the
/// plaintext JSONB payload when encryption is enabled for an outbox.
///
/// Serialises to / deserialises from the JSON shape:
/// ```json
/// {
///   "_enc": 1,
///   "kms": "aws",
///   "kid": "arn:aws:kms:us-east-1:...:alias/my-key",
///   "alg": "AES256GCM",
///   "iv":  "<base64 12-byte nonce>",
///   "edek": "<base64 encrypted DEK>",
///   "ct":  "<base64 ciphertext>"
/// }
/// ```
///
/// Payloads that do NOT contain an `_enc` field are passed through
/// unmodified — full backward compatibility.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedPayload {
    /// Envelope version; currently always `1`.
    #[serde(rename = "_enc")]
    pub version: u8,

    /// KMS provider identifier: `"aws"`, `"gcp"`, `"vault"`, or `"local"`.
    pub kms: String,

    /// KMS CMK key ID or alias ARN used to wrap the DEK.
    pub kid: String,

    /// Data-encryption algorithm: `"AES256GCM"`.
    pub alg: String,

    /// Base64-encoded 12-byte AES-GCM initialization vector.
    pub iv: String,

    /// Base64-encoded KMS-wrapped data-encryption key (DEK).
    pub edek: String,

    /// Base64-encoded AES-256-GCM ciphertext of the original payload.
    pub ct: String,
}

// ── EncryptionEnvelope trait ──────────────────────────────────────────────

/// Trait implemented by each KMS provider.
///
/// Both methods are `async` because KMS API calls are network round-trips.
/// The relay caches data-encryption keys (DEKs) in memory with a configurable
/// TTL to avoid a KMS call on every message.
#[async_trait::async_trait]
pub trait EncryptionEnvelope: Send + Sync {
    /// Encrypt `plaintext` (a serialised JSONB payload) and return the
    /// [`EncryptedPayload`] envelope that will be stored in the outbox row.
    async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedPayload, RelayError>;

    /// Decrypt an [`EncryptedPayload`] and return the original plaintext bytes.
    async fn decrypt(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError>;
}

// ── AWS KMS provider (v1.0.0 implementation) ─────────────────────────────

/// AWS KMS envelope-encryption provider.
///
/// Uses `aws-sdk-kms` (optional dep, not yet added) to call
/// `GenerateDataKey` and `Decrypt`.  CMK is identified by ARN or alias.
///
/// Full implementation in v1.0.0.
#[cfg(feature = "kms-aws")]
pub struct AwsKms {
    /// KMS CMK key ARN or alias (e.g. `arn:aws:kms:us-east-1:...:alias/key`).
    pub key_id: String,
    /// AWS region inferred from `key_id` ARN or from the environment.
    pub region: Option<String>,
}

#[cfg(feature = "kms-aws")]
#[async_trait::async_trait]
impl EncryptionEnvelope for AwsKms {
    async fn encrypt(&self, _plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        todo!("AwsKms::encrypt — implemented in v1.0.0")
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        todo!("AwsKms::decrypt — implemented in v1.0.0")
    }
}

// ── GCP Cloud KMS provider (v1.0.0 implementation) ───────────────────────

/// GCP Cloud KMS envelope-encryption provider.
///
/// Uses the GCP REST API (`google-cloud-kms`, not yet added) with
/// application-default credentials.  CMK is identified by its resource name.
///
/// Full implementation in v1.0.0.
#[cfg(feature = "kms-gcp")]
pub struct GcpKms {
    /// GCP KMS key resource name.
    /// Format: `projects/P/locations/L/keyRings/KR/cryptoKeys/K/cryptoKeyVersions/V`
    pub key_name: String,
}

#[cfg(feature = "kms-gcp")]
#[async_trait::async_trait]
impl EncryptionEnvelope for GcpKms {
    async fn encrypt(&self, _plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        todo!("GcpKms::encrypt — implemented in v1.0.0")
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        todo!("GcpKms::decrypt — implemented in v1.0.0")
    }
}

// ── HashiCorp Vault KMS provider (v1.0.0 implementation) ─────────────────

/// HashiCorp Vault Transit engine envelope-encryption provider.
///
/// Uses the Vault HTTP API (`vaultrs`, not yet added) with a token or
/// AppRole authentication.  CMK is identified by the transit path and key name.
///
/// Full implementation in v1.0.0.
#[cfg(feature = "kms-vault")]
pub struct VaultKms {
    /// Vault server URL (e.g. `https://vault.example.com`).
    pub vault_addr: String,
    /// Transit engine mount path (e.g. `transit`).
    pub mount: String,
    /// Transit key name.
    pub key_name: String,
}

#[cfg(feature = "kms-vault")]
#[async_trait::async_trait]
impl EncryptionEnvelope for VaultKms {
    async fn encrypt(&self, _plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        todo!("VaultKms::encrypt — implemented in v1.0.0")
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        todo!("VaultKms::decrypt — implemented in v1.0.0")
    }
}

// ── Local key-file provider (development / testing only) ─────────────────

/// Local 32-byte hex key-file encryption provider.
///
/// Reads a 32-byte (64 hex characters) AES-256 key from a file on disk.
/// Intended for local development and integration testing ONLY — it provides
/// confidentiality but no KMS-backed key rotation or hardware isolation.
///
/// Full implementation in v1.0.0.
#[cfg(feature = "kms-local")]
pub struct LocalKeyFile {
    /// Path to the file containing the 64-character hex-encoded 32-byte key.
    pub key_path: std::path::PathBuf,
}

#[cfg(feature = "kms-local")]
#[async_trait::async_trait]
impl EncryptionEnvelope for LocalKeyFile {
    async fn encrypt(&self, _plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        todo!("LocalKeyFile::encrypt — implemented in v1.0.0")
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        todo!("LocalKeyFile::decrypt — implemented in v1.0.0")
    }
}

// ── Helper: detect encrypted envelopes ───────────────────────────────────

/// Returns `true` when a JSONB payload value is an encrypted envelope
/// (contains the `_enc` field).
///
/// Used by the wire-format encoder to decide whether to attempt decryption.
/// When the `kms` feature is not compiled, this function always returns
/// `false` (the compiler eliminates the call entirely).
pub fn is_encrypted_envelope(payload: &serde_json::Value) -> bool {
    payload.get("_enc").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_encrypted_envelope_positive() {
        let enc = serde_json::json!({
            "_enc": 1,
            "kms": "local",
            "kid": "test-key",
            "alg": "AES256GCM",
            "iv": "aGVsbG93b3JsZA==",
            "edek": "ZW5jcnlwdGVk",
            "ct": "Y2lwaGVydGV4dA=="
        });
        assert!(is_encrypted_envelope(&enc));
    }

    #[test]
    fn test_is_encrypted_envelope_negative() {
        let plain = serde_json::json!({ "event_type": "order.created", "id": 42 });
        assert!(!is_encrypted_envelope(&plain));
    }

    #[test]
    fn test_encrypted_payload_roundtrip() {
        let ep = EncryptedPayload {
            version: 1,
            kms: "local".to_string(),
            kid: "test-key-id".to_string(),
            alg: "AES256GCM".to_string(),
            iv: "aGVsbG93b3JsZA==".to_string(),
            edek: "ZW5jcnlwdGVk".to_string(),
            ct: "Y2lwaGVydGV4dA==".to_string(),
        };
        let json = serde_json::to_string(&ep).expect("serialize");
        let decoded: EncryptedPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.kms, "local");
        assert_eq!(decoded.alg, "AES256GCM");
        // Verify the `_enc` field name is serialised correctly.
        let v: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(v["_enc"], 1);
    }
}
