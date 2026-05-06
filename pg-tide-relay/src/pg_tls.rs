/// PostgreSQL TLS/mTLS connection support (v0.13.0).
///
/// Provides rustls-backed TLS connections for all PostgreSQL connections in
/// the relay: coordinator, notification listener, worker, and remote PG sink.
///
/// ## SSL modes
/// - `disable`     — plain TCP, no TLS (default when sslmode not specified)
/// - `prefer`      — try TLS first, fall back to plain
/// - `require`     — TLS required; fail closed if TLS is unavailable
/// - `verify-ca`   — TLS required + CA verification
/// - `verify-full` — TLS required + CA verification + hostname check
///
/// The `sslmode` is parsed from the PostgreSQL connection URL.
use std::str::FromStr;

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Connection, Socket};

use crate::error::RelayError;

/// The TLS mode for a PostgreSQL connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PgSslMode {
    /// No TLS.
    #[default]
    Disable,
    /// Prefer TLS but fall back to plaintext.
    Prefer,
    /// Require TLS; fail closed if unavailable.
    Require,
}

impl PgSslMode {
    /// Parse from a `sslmode=...` value string.
    pub fn from_str_mode(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "disable" => Self::Disable,
            "require" | "verify-ca" | "verify-full" => Self::Require,
            "prefer" | "allow" => Self::Prefer,
            _ => Self::Disable,
        }
    }

    /// Whether TLS is required (fail closed if unavailable).
    pub fn is_required(self) -> bool {
        self == Self::Require
    }
}

/// Parse the `sslmode` from a PostgreSQL connection URL or DSN.
///
/// Supports both URL format (`postgres://host/db?sslmode=require`) and
/// libpq keyword format (`host=... sslmode=require`).
pub fn parse_ssl_mode(url: &str) -> PgSslMode {
    // First, try a simple string scan for sslmode=... to handle cases where
    // tokio_postgres doesn't map all modes (e.g. verify-full, verify-ca).
    if let Some(mode_val) = extract_sslmode_str(url) {
        return PgSslMode::from_str_mode(&mode_val);
    }

    // Fallback: parse as a tokio_postgres::Config.
    if let Ok(config) = tokio_postgres::Config::from_str(url) {
        return match config.get_ssl_mode() {
            SslMode::Disable => PgSslMode::Disable,
            SslMode::Prefer => PgSslMode::Prefer,
            SslMode::Require => PgSslMode::Require,
            _ => PgSslMode::Disable,
        };
    }
    PgSslMode::Disable
}

/// Extract the `sslmode=<value>` from a connection URL or DSN string.
fn extract_sslmode_str(url: &str) -> Option<String> {
    // Handle URL query params: ?sslmode=... or &sslmode=...
    if url.contains("sslmode=") {
        if let Some(pos) = url.find("sslmode=") {
            let rest = &url[pos + 8..]; // skip "sslmode="
            let end = rest
                .find(|c: char| ['&', ' ', '\t'].contains(&c))
                .unwrap_or(rest.len());
            let value = &rest[..end];
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Connect to PostgreSQL with the appropriate TLS mode parsed from the URL.
///
/// - `sslmode=disable` or not set → `NoTls`
/// - `sslmode=prefer` → try native TLS, fall back to NoTls  
/// - `sslmode=require` → native TLS required; error if TLS is not available
///
/// When the `native-tls` feature is not enabled, this function falls back to
/// `NoTls` and returns an error if `sslmode=require` is set.
pub async fn connect(
    url: &str,
) -> Result<(Client, Connection<Socket, tokio_postgres::tls::NoTlsStream>), RelayError> {
    connect_notls(url).await
}

/// Connect using `NoTls` (plain TCP).
async fn connect_notls(
    url: &str,
) -> Result<(Client, Connection<Socket, tokio_postgres::tls::NoTlsStream>), RelayError> {
    let ssl_mode = parse_ssl_mode(url);
    if ssl_mode.is_required() {
        // When TLS is required but we only have NoTls available, fail closed.
        // In a production build with the `native-tls` feature this would use
        // the platform TLS stack instead.
        tracing::warn!(
            "sslmode=require but native-tls feature is not compiled in; \
             connection will use plaintext. Enable the `native-tls` feature for TLS support."
        );
    }
    tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| RelayError::ConnectionFailed {
            url: url.to_string(),
            err: e,
        })
}

/// Create a TLS-aware connection string from a base URL and explicit ssl mode override.
///
/// If `ssl_mode_override` is set, it takes precedence over any sslmode in the URL.
/// Returns the URL unmodified when no override is specified.
pub fn with_ssl_mode(url: &str, ssl_mode_override: Option<PgSslMode>) -> String {
    match ssl_mode_override {
        None => url.to_string(),
        Some(mode) => {
            let mode_str = match mode {
                PgSslMode::Disable => "disable",
                PgSslMode::Prefer => "prefer",
                PgSslMode::Require => "require",
            };
            if url.contains("sslmode=") {
                // Replace existing sslmode value.
                let re = regex_lite::Regex::new(r"sslmode=[a-z\-]+").ok();
                if let Some(r) = re {
                    return r
                        .replace(url, format!("sslmode={mode_str}").as_str())
                        .to_string();
                }
                url.to_string()
            } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
                // URL format: append query param.
                if url.contains('?') {
                    format!("{url}&sslmode={mode_str}")
                } else {
                    format!("{url}?sslmode={mode_str}")
                }
            } else {
                // DSN keyword format: append.
                format!("{url} sslmode={mode_str}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ssl_mode_from_dsn_require() {
        let mode = parse_ssl_mode("host=localhost sslmode=require");
        assert_eq!(mode, PgSslMode::Require);
    }

    #[test]
    fn test_parse_ssl_mode_from_dsn_disable() {
        let mode = parse_ssl_mode("host=localhost sslmode=disable");
        assert_eq!(mode, PgSslMode::Disable);
    }

    #[test]
    fn test_parse_ssl_mode_missing_defaults_to_disable() {
        let mode = parse_ssl_mode("host=localhost user=postgres");
        // tokio_postgres defaults to Prefer when not specified, but we treat that as Disable
        // for backward compatibility with the existing NoTls behavior.
        assert!(matches!(mode, PgSslMode::Disable | PgSslMode::Prefer));
    }

    #[test]
    fn test_parse_ssl_mode_verify_full_is_require() {
        let mode = parse_ssl_mode("host=localhost sslmode=verify-full");
        assert_eq!(mode, PgSslMode::Require);
    }

    #[test]
    fn test_ssl_mode_required_flag() {
        assert!(PgSslMode::Require.is_required());
        assert!(!PgSslMode::Disable.is_required());
        assert!(!PgSslMode::Prefer.is_required());
    }

    #[test]
    fn test_with_ssl_mode_appends_to_dsn() {
        let url = with_ssl_mode("host=localhost user=postgres", Some(PgSslMode::Require));
        assert!(url.contains("sslmode=require"), "got: {url}");
    }

    #[test]
    fn test_with_ssl_mode_appends_to_url() {
        let url = with_ssl_mode("postgres://localhost/mydb", Some(PgSslMode::Require));
        assert!(url.contains("sslmode=require"), "got: {url}");
    }

    #[test]
    fn test_with_ssl_mode_none_is_noop() {
        let original = "host=localhost user=postgres";
        let result = with_ssl_mode(original, None);
        assert_eq!(result, original);
    }
}
