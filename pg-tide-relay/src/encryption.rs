//! Local key-file envelope encryption retained for supported deployments.

#![cfg(feature = "kms")]

use crate::error::RelayError;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedPayload {
    #[serde(rename = "_enc")]
    pub version: u8,
    pub kms: String,
    pub kid: String,
    pub alg: String,
    pub iv: String,
    pub edek: String,
    pub ct: String,
}

#[async_trait::async_trait]
pub trait EncryptionEnvelope: Send + Sync {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedPayload, RelayError>;
    async fn decrypt(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError>;
    fn is_available(&self) -> bool;
}

#[cfg(feature = "kms-local")]
pub struct LocalKeyFile {
    pub key_path: std::path::PathBuf,
    pub key_path_previous: Option<std::path::PathBuf>,
}

#[cfg(feature = "kms-local")]
impl LocalKeyFile {
    fn read_key(path: &std::path::Path) -> Result<[u8; 32], RelayError> {
        let content = crate::secret::load_file(path)?.expose().to_owned();
        let encoded = content.trim();
        if encoded.len() != 64 {
            return Err(RelayError::Config(format!(
                "LocalKeyFile: '{}' must contain exactly 64 hex characters",
                path.display()
            )));
        }
        let bytes = hex::decode(encoded).map_err(|error| {
            RelayError::Config(format!(
                "LocalKeyFile: invalid hex in '{}': {error}",
                path.display()
            ))
        })?;
        bytes.try_into().map_err(|_| {
            RelayError::Config(format!(
                "LocalKeyFile: '{}' did not decode to 32 bytes",
                path.display()
            ))
        })
    }

    fn fingerprint(key: &[u8; 32]) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(&Sha256::digest(key)[..8])
    }

    fn encrypt_with_key(key: &[u8; 32], plaintext: &[u8]) -> Result<(String, String), RelayError> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};
        use rand::RngCore;

        let cipher = Aes256Gcm::new_from_slice(key).map_err(|error| {
            RelayError::Config(format!("LocalKeyFile: AES-GCM init failed: {error}"))
        })?;
        let mut iv = [0u8; 12];
        rand::rng().fill_bytes(&mut iv);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&iv), plaintext)
            .map_err(|_| {
                RelayError::Config("LocalKeyFile: AES-GCM encryption failed".to_string())
            })?;
        Ok((BASE64.encode(iv), BASE64.encode(ciphertext)))
    }

    fn decrypt_with_key(key: &[u8; 32], iv: &str, ciphertext: &str) -> Result<Vec<u8>, RelayError> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};

        let iv = BASE64
            .decode(iv)
            .map_err(|error| RelayError::Config(format!("LocalKeyFile: invalid IV: {error}")))?;
        if iv.len() != 12 {
            return Err(RelayError::Config(
                "LocalKeyFile: IV must be 12 bytes".to_string(),
            ));
        }
        let ciphertext = BASE64.decode(ciphertext).map_err(|error| {
            RelayError::Config(format!("LocalKeyFile: invalid ciphertext: {error}"))
        })?;
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|error| {
            RelayError::Config(format!("LocalKeyFile: AES-GCM init failed: {error}"))
        })?;
        cipher
            .decrypt(Nonce::from_slice(&iv), ciphertext.as_ref())
            .map_err(|_| RelayError::Config("LocalKeyFile: authentication failed".to_string()))
    }
}

#[cfg(feature = "kms-local")]
#[async_trait::async_trait]
impl EncryptionEnvelope for LocalKeyFile {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
        let key = Self::read_key(&self.key_path)?;
        let (iv, ct) = Self::encrypt_with_key(&key, plaintext)?;
        Ok(EncryptedPayload {
            version: 1,
            kms: "local".to_string(),
            kid: Self::fingerprint(&key),
            alg: "AES256GCM".to_string(),
            iv,
            edek: BASE64.encode(b"local"),
            ct,
        })
    }

    async fn decrypt(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, RelayError> {
        for path in std::iter::once(&self.key_path).chain(self.key_path_previous.iter()) {
            let key = Self::read_key(path)?;
            if Self::fingerprint(&key) == payload.kid {
                return Self::decrypt_with_key(&key, &payload.iv, &payload.ct);
            }
        }
        Err(RelayError::Config(format!(
            "LocalKeyFile: key fingerprint '{}' is unavailable",
            payload.kid
        )))
    }

    fn is_available(&self) -> bool {
        true
    }
}

pub fn is_encrypted_envelope(payload: &serde_json::Value) -> bool {
    payload.get("_enc").is_some()
}
