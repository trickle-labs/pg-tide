// v0.33.0: Envelope encryption interface skeleton (ADR-010).
// v0.35.0: Replaced todo!() panics with structured RelayError::NotImplemented
//          errors and added EncryptionEnvelope::is_available() startup guard.
//
// The LocalKeyFile provider has a full AES-256-GCM implementation in v0.35.0.
// Cloud providers (AwsKms, GcpKms, VaultKms) return NotImplemented gracefully;
// they will be fully implemented before v1.0.0.
//
// Gated on `feature = "kms"`.  The default build ships without any KMS
// dependencies; enabling `kms` (or a sub-feature such as `kms-aws`) brings
// in this module.
//
// See also: docs/adr/adr-010-envelope-encryption-kms.md

#![cfg(feature = "kms")]

use crate::error::RelayError;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

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

    /// v0.35.0: Returns `true` when this provider has a real implementation
    /// (i.e., will not return `NotImplemented` on encrypt/decrypt calls).
    ///
    /// The coordinator calls this at pipeline startup to decide whether to
    /// acquire the pipeline or log `PauseReason::NotImplemented` instead.
    fn is_available(&self) -> bool;
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
        Err(RelayError::not_implemented(
            "aws",
            "AwsKms encryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        Err(RelayError::not_implemented(
            "aws",
            "AwsKms decryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    fn is_available(&self) -> bool {
        false
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
        Err(RelayError::not_implemented(
            "gcp",
            "GcpKms encryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        Err(RelayError::not_implemented(
            "gcp",
            "GcpKms decryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    fn is_available(&self) -> bool {
        false
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
        Err(RelayError::not_implemented(
            "vault",
            "VaultKms encryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    async fn decrypt(&self, _payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        Err(RelayError::not_implemented(
            "vault",
            "VaultKms decryption is not yet implemented; full implementation arrives before v1.0.0",
        ))
    }

    fn is_available(&self) -> bool {
        false
    }
}

// ── Local key-file provider (v0.35.0 full implementation) ─────────────────

/// Local 32-byte hex key-file encryption provider.
///
/// Reads a 32-byte (64 hex characters) AES-256 key from a file on disk.
/// Intended for local development and integration testing ONLY — it provides
/// confidentiality but no KMS-backed key rotation or hardware isolation.
///
/// Envelope format (ADR-010):
/// ```json
/// { "_enc": 1, "kms": "local", "kid": "<hex_fingerprint>",
///   "alg": "AES256GCM", "iv": "<base64>", "edek": "<base64>", "ct": "<base64>" }
/// ```
/// The `edek` field stores the key fingerprint (not an encrypted DEK) for
/// the local provider since the key is already stored in plaintext on disk.
///
/// Key rotation: set `key_path_previous` to the old key file; decrypt attempts
/// try the current key first, then fall back to the previous key.
#[cfg(feature = "kms-local")]
pub struct LocalKeyFile {
    /// Path to the file containing the 64-character hex-encoded 32-byte key.
    pub key_path: std::path::PathBuf,
    /// Optional path to the previous key file for seamless rotation.
    pub key_path_previous: Option<std::path::PathBuf>,
}

#[cfg(feature = "kms-local")]
impl LocalKeyFile {
    /// Read and decode a 32-byte AES-256 key from a hex file.
    fn read_key(path: &std::path::Path) -> Result<[u8; 32], RelayError> {
        let content = crate::secret::load_file(path)?.expose().to_owned();
        let hex = content.trim();
        if hex.len() != 64 {
            return Err(RelayError::Config(format!(
                "LocalKeyFile: key file '{}' must contain exactly 64 hex characters (32 bytes); \
                 got {} characters",
                path.display(),
                hex.len()
            )));
        }
        let bytes = hex::decode(hex).map_err(|e| {
            RelayError::Config(format!(
                "LocalKeyFile: invalid hex in key file '{}': {e}",
                path.display()
            ))
        })?;
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(key)
    }

    /// Compute the first 8 bytes of SHA-256 of the key as a hex fingerprint.
    fn key_fingerprint(key: &[u8; 32]) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(key);
        hex::encode(&digest[..8])
    }

    /// Encrypt plaintext with the given 32-byte AES-256-GCM key.
    fn encrypt_with_key(key: &[u8; 32], plaintext: &[u8]) -> Result<(String, String), RelayError> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
        use rand::RngCore;

        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| RelayError::Config(format!("LocalKeyFile: AES-GCM init failed: {e}")))?;

        let mut iv_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut iv_bytes);
        let nonce = Nonce::from_slice(&iv_bytes);

        let ciphertext = cipher.encrypt(nonce, plaintext).map_err(|e| {
            RelayError::Config(format!("LocalKeyFile: AES-GCM encrypt failed: {e}"))
        })?;

        let iv_b64 = BASE64.encode(iv_bytes);
        let ct_b64 = BASE64.encode(&ciphertext);
        Ok((iv_b64, ct_b64))
    }

    /// Decrypt ciphertext with the given 32-byte AES-256-GCM key.
    fn decrypt_with_key(key: &[u8; 32], iv_b64: &str, ct_b64: &str) -> Result<Vec<u8>, RelayError> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| RelayError::Config(format!("LocalKeyFile: AES-GCM init failed: {e}")))?;

        let iv_bytes = BASE64
            .decode(iv_b64)
            .map_err(|e| RelayError::Config(format!("LocalKeyFile: invalid IV base64: {e}")))?;
        let nonce = Nonce::from_slice(&iv_bytes);

        let ct_bytes = BASE64
            .decode(ct_b64)
            .map_err(|e| RelayError::Config(format!("LocalKeyFile: invalid CT base64: {e}")))?;

        cipher
            .decrypt(nonce, ct_bytes.as_ref())
            .map_err(|_| RelayError::Config("LocalKeyFile: AES-GCM authentication tag mismatch — wrong key or corrupted ciphertext".to_string()))
    }
}

#[cfg(feature = "kms-local")]
#[async_trait::async_trait]
impl EncryptionEnvelope for LocalKeyFile {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        let key = Self::read_key(&self.key_path)?;
        let fingerprint = Self::key_fingerprint(&key);
        let (iv_b64, ct_b64) = Self::encrypt_with_key(&key, plaintext)?;

        Ok(EncryptedPayload {
            version: 1,
            kms: "local".to_string(),
            kid: fingerprint,
            alg: "AES256GCM".to_string(),
            iv: iv_b64,
            // edek holds the fingerprint for the local provider (no envelope key wrapping).
            edek: BASE64.encode(b"local"),
            ct: ct_b64,
        })
    }

    async fn decrypt(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        // Try current key first.
        let current_key = Self::read_key(&self.key_path)?;
        let current_fingerprint = Self::key_fingerprint(&current_key);

        if current_fingerprint == payload.kid {
            return Self::decrypt_with_key(&current_key, &payload.iv, &payload.ct);
        }

        // Fall back to previous key if configured and fingerprint matches.
        if let Some(ref prev_path) = self.key_path_previous {
            let prev_key = Self::read_key(prev_path)?;
            let prev_fingerprint = Self::key_fingerprint(&prev_key);
            if prev_fingerprint == payload.kid {
                return Self::decrypt_with_key(&prev_key, &payload.iv, &payload.ct);
            }
        }

        Err(RelayError::Config(format!(
            "LocalKeyFile: no key with fingerprint '{}' found in current or previous key file",
            payload.kid
        )))
    }

    fn is_available(&self) -> bool {
        true
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

    #[cfg(feature = "kms-local")]
    mod local_key_file_tests {
        use super::super::*;
        use std::io::Write;
        use tempfile::NamedTempFile;

        fn write_key_file(key_hex: &str) -> NamedTempFile {
            let mut f = NamedTempFile::new().expect("tempfile");
            f.write_all(key_hex.as_bytes()).expect("write key");
            f
        }

        fn random_key_hex() -> String {
            use rand::RngCore;
            let mut key = [0u8; 32];
            rand::rng().fill_bytes(&mut key);
            hex::encode(key)
        }

        #[tokio::test]
        async fn test_local_key_file_encrypt_decrypt_roundtrip() {
            let key_hex = random_key_hex();
            let key_file = write_key_file(&key_hex);
            let provider = LocalKeyFile {
                key_path: key_file.path().to_path_buf(),
                key_path_previous: None,
            };
            assert!(provider.is_available());

            let plaintext = b"hello, world! this is a test payload.";
            let envelope = provider.encrypt(plaintext).await.expect("encrypt");
            assert_eq!(envelope.kms, "local");
            assert_eq!(envelope.alg, "AES256GCM");
            assert_eq!(envelope.version, 1);

            let decrypted = provider.decrypt(&envelope).await.expect("decrypt");
            assert_eq!(decrypted, plaintext);
        }

        #[tokio::test]
        async fn test_local_key_file_100_messages() {
            let key_hex = random_key_hex();
            let key_file = write_key_file(&key_hex);
            let provider = LocalKeyFile {
                key_path: key_file.path().to_path_buf(),
                key_path_previous: None,
            };

            let mut envelopes = Vec::with_capacity(100);
            for i in 0..100u32 {
                let plaintext = format!(r#"{{"id": {i}, "event": "test"}}"#);
                let env = provider
                    .encrypt(plaintext.as_bytes())
                    .await
                    .expect("encrypt");
                envelopes.push((plaintext, env));
            }

            for (plaintext, env) in &envelopes {
                let decrypted = provider.decrypt(env).await.expect("decrypt");
                assert_eq!(String::from_utf8(decrypted).expect("utf8"), *plaintext);
            }
        }

        #[tokio::test]
        async fn test_local_key_file_key_rotation() {
            // Create two keys.
            let old_key_hex = random_key_hex();
            let new_key_hex = random_key_hex();
            let old_key_file = write_key_file(&old_key_hex);
            let new_key_file = write_key_file(&new_key_hex);

            // Encrypt 50 messages with old key.
            let provider_old = LocalKeyFile {
                key_path: old_key_file.path().to_path_buf(),
                key_path_previous: None,
            };
            let mut envelopes = Vec::with_capacity(50);
            for i in 0..50u32 {
                let plaintext = format!(r#"{{"id": {i}}}"#);
                let env = provider_old
                    .encrypt(plaintext.as_bytes())
                    .await
                    .expect("encrypt with old key");
                envelopes.push((plaintext, env));
            }

            // Rotate: new key becomes current, old key becomes previous.
            let provider_new = LocalKeyFile {
                key_path: new_key_file.path().to_path_buf(),
                key_path_previous: Some(old_key_file.path().to_path_buf()),
            };

            // All 50 old messages must still decrypt via the previous key.
            for (plaintext, env) in &envelopes {
                let decrypted = provider_new
                    .decrypt(env)
                    .await
                    .expect("decrypt after rotation");
                assert_eq!(String::from_utf8(decrypted).expect("utf8"), *plaintext);
            }

            // New messages encrypted with new key also work.
            let new_env = provider_new
                .encrypt(b"new message")
                .await
                .expect("encrypt with new key");
            let decrypted = provider_new.decrypt(&new_env).await.expect("decrypt new");
            assert_eq!(decrypted, b"new message");
        }

        #[tokio::test]
        async fn test_kms_startup_guard_unavailable_provider() {
            // AWS/GCP/Vault providers return is_available() = false.
            // This ensures the coordinator startup guard works correctly.
            #[cfg(feature = "kms-aws")]
            {
                let provider = super::super::AwsKms {
                    key_id: "arn:aws:kms:us-east-1:123456789012:key/test".to_string(),
                    region: None,
                };
                assert!(!provider.is_available());
                let err = provider.encrypt(b"test").await.unwrap_err();
                assert!(matches!(err, RelayError::NotImplemented { .. }));
                assert!(!err.is_transient(), "NotImplemented must not be retried");
            }
        }
    }
}
