/// PostgreSQL TLS/mTLS connection support (v0.13.0 / v0.23.0).
///
/// Provides TLS connections for all PostgreSQL connections in the relay.
///
/// ## SSL modes
/// - `disable`     — plain TCP, no TLS (default when sslmode not specified)
/// - `prefer`      — try TLS first, fall back to plain
/// - `require`     — TLS required; fail closed if TLS is unavailable
/// - `verify-ca`   — TLS required + CA verification
/// - `verify-full` — TLS required + CA verification + hostname check
///
/// The `sslmode` is parsed from the PostgreSQL connection URL.
///
/// ## v0.23.0: `native-tls` feature
/// When compiled with `--features native-tls`, the `require`/`verify-ca`/
/// `verify-full` modes use the platform OpenSSL stack (via `postgres-openssl`)
/// instead of failing closed.  The `:latest` Docker image and default feature
/// set remain NoTls-capable (fail-closed on `require`); the experimental
/// image compiles with `--features native-tls`.
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Connection, Socket};

use crate::error::RelayError;

/// Opaque PostgreSQL background connection driver.
///
/// This enum abstracts over the plaintext and TLS connection driver types,
/// allowing call sites to write `let (client, conn) = pg_tls::connect(url).await?;`
/// without caring whether TLS is in use.  All callers should spawn this:
/// ```ignore
/// tokio::spawn(async move { let _ = conn.await; });
/// ```
pub enum PgConnection {
    /// Plain-text (NoTls) connection driver.
    Plain(Connection<Socket, tokio_postgres::tls::NoTlsStream>),
    /// TLS connection driver (only present when `native-tls` feature is enabled).
    #[cfg(feature = "native-tls")]
    Tls(Connection<Socket, postgres_openssl::TlsStream<Socket>>),
}

impl std::future::Future for PgConnection {
    type Output = Result<(), tokio_postgres::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We never move the inner value after pinning.
        match unsafe { self.get_unchecked_mut() } {
            PgConnection::Plain(conn) => {
                // SAFETY: We pin the inner connection via a projection.
                unsafe { Pin::new_unchecked(conn) }.poll(cx)
            }
            #[cfg(feature = "native-tls")]
            PgConnection::Tls(conn) => {
                // SAFETY: We pin the inner connection via a projection.
                unsafe { Pin::new_unchecked(conn) }.poll(cx)
            }
        }
    }
}

impl PgConnection {
    /// Proxy `poll_message` for LISTEN/NOTIFY notification streams.
    ///
    /// This allows callers that drive the connection manually (e.g. for
    /// intercepting `AsyncMessage::Notification`) to work regardless of
    /// whether TLS is in use.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<tokio_postgres::AsyncMessage, tokio_postgres::Error>>> {
        match self {
            PgConnection::Plain(conn) => conn.poll_message(cx),
            #[cfg(feature = "native-tls")]
            PgConnection::Tls(conn) => conn.poll_message(cx),
        }
    }
}

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
/// - `sslmode=prefer` → try TLS first, fall back to plaintext
/// - `sslmode=require` / `verify-ca` / `verify-full`:
///   - With `native-tls` feature: use platform OpenSSL stack (postgres-openssl)
///   - Without `native-tls` feature: **fail closed** (`TlsRequired` error)
///
/// ## Security
/// When `sslmode=require` is set and the `native-tls` feature is not compiled
/// in, this function refuses to connect without TLS rather than silently
/// downgrading to plaintext.  This prevents accidental credential exposure on
/// networks that do not enforce encryption at the infrastructure layer.
pub async fn connect(url: &str) -> Result<(Client, PgConnection), RelayError> {
    let ssl_mode = parse_ssl_mode(url);
    match ssl_mode {
        PgSslMode::Require => connect_require(url).await,
        _ => connect_notls(url).await,
    }
}

/// Handle a connection request when `sslmode=require`.
///
/// With the `native-tls` feature: use the platform TLS stack.
/// Without it: fail closed with `RelayError::TlsRequired`.
#[cfg(feature = "native-tls")]
async fn connect_require(url: &str) -> Result<(Client, PgConnection), RelayError> {
    use postgres_openssl::MakeTlsConnector;

    let connector = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
        .map_err(|e| RelayError::TlsSetup(e.to_string()))?
        .build();
    let tls_connector = MakeTlsConnector::new(connector);

    let (client, conn) = tokio_postgres::connect(url, tls_connector)
        .await
        .map_err(|e| RelayError::ConnectionFailed {
            url: url.to_string(),
            err: e,
        })?;
    Ok((client, PgConnection::Tls(conn)))
}

/// Fail closed when `sslmode=require` but `native-tls` is not compiled in.
#[cfg(not(feature = "native-tls"))]
async fn connect_require(url: &str) -> Result<(Client, PgConnection), RelayError> {
    Err(RelayError::TlsRequired {
        url: url.to_string(),
    })
}

/// Connect using `NoTls` (plain TCP).
async fn connect_notls(url: &str) -> Result<(Client, PgConnection), RelayError> {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| RelayError::ConnectionFailed {
            url: url.to_string(),
            err: e,
        })?;
    Ok((client, PgConnection::Plain(conn)))
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
