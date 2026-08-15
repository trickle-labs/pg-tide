// KMS integration tests (v0.36.0).
//
// Tests:
//   1. `LocalKeyFile` key rotation: encrypt 50 messages with key A, rotate to
//      key B (previous = A), verify all 50 decrypt correctly via new provider.
//   2. Forward-secrecy smoke: new-key ciphertexts must not decrypt with old-only
//      provider.
//   3. GCP KMS startup guard: `is_available() = false`, encrypt/decrypt return
//      `NotImplemented`, and `is_transient() = false` on that error.
//
// No PostgreSQL container is required — the tests operate on the encryption
// library directly.
//
// CI runs with `experimental-full`, so both `kms-local` and `kms-gcp` tests are
// always exercised there.

// ── LocalKeyFile key rotation integration tests ───────────────────────────────

#[cfg(feature = "kms-local")]
mod local_key_file_rotation {
    use pg_tide_relay::encryption::{EncryptionEnvelope, LocalKeyFile};
    use rand::RngCore;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn random_key_file() -> NamedTempFile {
        let mut raw = [0u8; 32];
        rand::rng().fill_bytes(&mut raw);
        let hex = hex::encode(raw);
        let mut f = NamedTempFile::new().expect("tempfile");
        write!(f, "{hex}").expect("write key");
        f
    }

    /// Encrypt 50 messages with key A, rotate to key B (previous = A),
    /// then verify all 50 messages decrypt correctly via the new provider.
    #[tokio::test]
    async fn test_local_key_file_rotation_50_messages() {
        let key_a_file = random_key_file();
        let key_b_file = random_key_file();

        // Encrypt 50 messages with key A.
        let provider_a = LocalKeyFile {
            key_path: key_a_file.path().to_path_buf(),
            key_path_previous: None,
        };
        let mut ciphertexts = Vec::with_capacity(50);
        for i in 0u32..50 {
            let plaintext = format!(r#"{{"index":{i},"payload":"msg-{i}"}}"#);
            let envelope = provider_a
                .encrypt(plaintext.as_bytes())
                .await
                .unwrap_or_else(|e| panic!("encrypt message {i}: {e}"));
            ciphertexts.push((plaintext, envelope));
        }

        // Rotate: key B is current, key A is previous.
        let provider_b = LocalKeyFile {
            key_path: key_b_file.path().to_path_buf(),
            key_path_previous: Some(key_a_file.path().to_path_buf()),
        };

        // All 50 old-key ciphertexts must still decrypt via the previous key.
        for (i, (expected, envelope)) in ciphertexts.iter().enumerate() {
            let decrypted = provider_b
                .decrypt(envelope)
                .await
                .unwrap_or_else(|e| panic!("decrypt message {i} after rotation: {e}"));
            let got = String::from_utf8(decrypted)
                .unwrap_or_else(|e| panic!("utf8 decode message {i}: {e}"));
            assert_eq!(
                got, *expected,
                "message {i} must round-trip after key rotation"
            );
        }

        // New messages encrypted with key B must also decrypt correctly.
        let new_env = provider_b
            .encrypt(b"post-rotation-payload")
            .await
            .expect("encrypt with key B");
        let new_dec = provider_b
            .decrypt(&new_env)
            .await
            .expect("decrypt new-key message");
        assert_eq!(new_dec, b"post-rotation-payload");

        // The old-only provider (key A, no previous) must NOT decrypt key-B ciphertexts.
        let old_fail = provider_a.decrypt(&new_env).await;
        assert!(
            old_fail.is_err(),
            "provider A alone must not decrypt ciphertexts encrypted with key B"
        );
    }

    /// Forward-secrecy smoke: new-key ciphertexts must not be decryptable by
    /// the previous-key-only provider.
    #[tokio::test]
    async fn test_rotation_forward_secrecy() {
        let key_a_file = random_key_file();
        let key_b_file = random_key_file();

        let provider_b = LocalKeyFile {
            key_path: key_b_file.path().to_path_buf(),
            key_path_previous: Some(key_a_file.path().to_path_buf()),
        };
        let envelope = provider_b
            .encrypt(b"forward-secret")
            .await
            .expect("encrypt with key B");

        // Provider A alone cannot decrypt key-B ciphertext.
        let provider_a_only = LocalKeyFile {
            key_path: key_a_file.path().to_path_buf(),
            key_path_previous: None,
        };
        let result = provider_a_only.decrypt(&envelope).await;
        assert!(
            result.is_err(),
            "key-A-only provider must not decrypt ciphertext encrypted with key B"
        );
    }

    /// Round-trip without rotation: same key encrypts and decrypts.
    #[tokio::test]
    async fn test_local_key_file_round_trip() {
        let key_file = random_key_file();
        let provider = LocalKeyFile {
            key_path: key_file.path().to_path_buf(),
            key_path_previous: None,
        };
        let plaintext = b"hello, world";
        let envelope = provider.encrypt(plaintext).await.expect("encrypt");
        let decrypted = provider.decrypt(&envelope).await.expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    /// LocalKeyFile must report is_available() = true.
    #[test]
    fn test_local_key_file_is_available() {
        let provider = LocalKeyFile {
            key_path: std::path::PathBuf::from("/tmp/test.key"),
            key_path_previous: None,
        };
        assert!(
            provider.is_available(),
            "LocalKeyFile must report is_available() = true"
        );
    }
}

// ── GCP KMS startup guard: not-yet-implemented provider ───────────────────────

#[cfg(feature = "kms-gcp")]
mod gcp_kms_startup_guard {
    use pg_tide_relay::encryption::{EncryptedPayload, EncryptionEnvelope, GcpKms};
    use pg_tide_relay::error::RelayError;

    fn test_provider() -> GcpKms {
        GcpKms {
            key_name:
                "projects/test-proj/locations/global/keyRings/test-ring/cryptoKeys/test-key/cryptoKeyVersions/1"
                    .to_string(),
        }
    }

    /// GcpKms.is_available() must return false (not yet implemented).
    #[test]
    fn test_gcp_kms_is_not_available() {
        assert!(
            !test_provider().is_available(),
            "GcpKms must report is_available() = false until v1.0.0"
        );
    }

    /// GcpKms.encrypt() must return NotImplemented, not panic.
    #[tokio::test]
    async fn test_gcp_kms_encrypt_returns_not_implemented() {
        let result = test_provider().encrypt(b"payload").await;
        assert!(result.is_err(), "GcpKms.encrypt() must return an error");
        assert!(
            matches!(result.unwrap_err(), RelayError::NotImplemented { .. }),
            "GcpKms.encrypt() must return RelayError::NotImplemented"
        );
    }

    /// GcpKms.decrypt() must return NotImplemented, not panic.
    #[tokio::test]
    async fn test_gcp_kms_decrypt_returns_not_implemented() {
        let dummy = EncryptedPayload {
            version: 1,
            kms: "gcp".to_string(),
            kid: "test-kid".to_string(),
            alg: "AES256GCM".to_string(),
            iv: "AAAAAAAAAAAAAAAA".to_string(),
            edek: "AAAA".to_string(),
            ct: "AAAA".to_string(),
        };
        let result = test_provider().decrypt(&dummy).await;
        assert!(result.is_err(), "GcpKms.decrypt() must return an error");
        assert!(
            matches!(result.unwrap_err(), RelayError::NotImplemented { .. }),
            "GcpKms.decrypt() must return RelayError::NotImplemented"
        );
    }

    /// NotImplemented errors must not be marked as transient (must not be retried).
    #[tokio::test]
    async fn test_gcp_kms_not_implemented_is_not_transient() {
        let err = test_provider().encrypt(b"x").await.unwrap_err();
        assert!(
            !err.is_transient(),
            "NotImplemented error must not be marked as transient (must not be retried)"
        );
    }
}
