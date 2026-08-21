/// PostgreSQL TLS/mTLS connection support (v0.13.0 / v0.23.0).
///
/// Provides TLS connections for all PostgreSQL connections in the relay.
///
/// ## SSL modes
/// - `disable`     — plain TCP, no TLS (development-only explicit override)
/// - `prefer`      — rejected because it permits a plaintext fallback
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
use std::task::{Context, Poll};

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

/// Checked policy used by connection setup. The older `PgSslMode` shape is
/// retained for callers that only need the pool's TLS/no-TLS split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedSslMode {
    Disable,
    Require,
    VerifyCa,
    VerifyFull,
}

impl PgSslMode {
    /// Parse from a `sslmode=...` value string.
    pub fn from_str_mode(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "disable" => Self::Disable,
            "require" | "verify-ca" | "verify-full" => Self::Require,
            "prefer" | "allow" => Self::Prefer,
            _ => Self::Prefer,
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

    // tokio-postgres defaults to `prefer`, which can silently fall back to
    // plaintext. Missing sslmode therefore fails closed.
    PgSslMode::Require
}

pub fn parse_ssl_mode_checked(url: &str) -> Result<CheckedSslMode, RelayError> {
    let mode = extract_sslmode_str(url)
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| "verify-full".to_string());
    match mode.as_str() {
        "disable" => Ok(CheckedSslMode::Disable),
        "require" => Ok(CheckedSslMode::Require),
        "verify-ca" => Ok(CheckedSslMode::VerifyCa),
        "verify-full" => Ok(CheckedSslMode::VerifyFull),
        "allow" | "prefer" => Err(RelayError::InsecureTransport {
            mode,
            url: sanitize_connection_url(url),
        }),
        _ => Err(RelayError::config("unsupported PostgreSQL sslmode")),
    }
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
/// - `sslmode=disable` → `NoTls` (explicit development override)
/// - missing `sslmode` or `sslmode=prefer` → fail closed
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
    let mode = parse_ssl_mode_checked(url)?;
    match mode {
        CheckedSslMode::Disable => connect_notls(url).await,
        _ => connect_tls(url, mode).await,
    }
}

/// Handle a connection request when `sslmode=require`.
///
/// With the `native-tls` feature: use the platform TLS stack.
/// Without it: fail closed with `RelayError::TlsRequired`.
#[cfg(feature = "native-tls")]
pub fn make_tls_connector() -> Result<postgres_openssl::MakeTlsConnector, RelayError> {
    make_tls_connector_for_mode(CheckedSslMode::VerifyFull, None)
}

#[cfg(feature = "native-tls")]
pub fn make_tls_connector_for_url(
    url: &str,
) -> Result<postgres_openssl::MakeTlsConnector, RelayError> {
    let mode = parse_ssl_mode_checked(url)?;
    if mode == CheckedSslMode::Disable {
        return Err(RelayError::config(
            "cannot create a TLS connector for sslmode=disable",
        ));
    }
    make_tls_connector_for_mode(mode, extract_parameter(url, "sslrootcert").as_deref())
}

#[cfg(feature = "native-tls")]
fn make_tls_connector_for_mode(
    mode: CheckedSslMode,
    root_cert: Option<&str>,
) -> Result<postgres_openssl::MakeTlsConnector, RelayError> {
    use openssl::ssl::SslVerifyMode;
    use postgres_openssl::MakeTlsConnector;

    let mut builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
        .map_err(|e| RelayError::TlsSetup(e.to_string()))?;
    match mode {
        CheckedSslMode::Require => builder.set_verify(SslVerifyMode::NONE),
        CheckedSslMode::VerifyCa | CheckedSslMode::VerifyFull => {
            builder.set_verify(SslVerifyMode::PEER);
            builder
                .set_default_verify_paths()
                .map_err(|_| RelayError::TlsSetup("system trust roots unavailable".into()))?;
            if let Some(path) = root_cert {
                builder.set_ca_file(path).map_err(|_| {
                    RelayError::TlsSetup("configured sslrootcert is invalid".into())
                })?;
            }
        }
        CheckedSslMode::Disable => {
            return Err(RelayError::config(
                "cannot create a TLS connector for sslmode=disable",
            ));
        }
    }
    let mut connector = MakeTlsConnector::new(builder.build());
    if mode != CheckedSslMode::VerifyFull {
        connector.set_callback(|configuration, _| {
            configuration.set_verify_hostname(false);
            Ok(())
        });
    }
    Ok(connector)
}

#[cfg(feature = "native-tls")]
async fn connect_tls(
    url: &str,
    mode: CheckedSslMode,
) -> Result<(Client, PgConnection), RelayError> {
    let connector =
        make_tls_connector_for_mode(mode, extract_parameter(url, "sslrootcert").as_deref())?;
    let (client, conn) = tokio_postgres::connect(url, connector).await.map_err(|e| {
        RelayError::ConnectionFailed {
            url: sanitize_connection_url(url),
            err: e,
        }
    })?;
    Ok((client, PgConnection::Tls(conn)))
}

/// Fail closed when `sslmode=require` but `native-tls` is not compiled in.
#[cfg(not(feature = "native-tls"))]
async fn connect_tls(
    url: &str,
    _mode: CheckedSslMode,
) -> Result<(Client, PgConnection), RelayError> {
    Err(RelayError::TlsRequired {
        url: sanitize_connection_url(url),
    })
}

fn extract_parameter(url: &str, name: &str) -> Option<String> {
    url.split(['?', '&', ' ', '\t'])
        .find_map(|part| part.strip_prefix(&format!("{name}=")))
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Connect using `NoTls` (plain TCP).
async fn connect_notls(url: &str) -> Result<(Client, PgConnection), RelayError> {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| RelayError::ConnectionFailed {
            url: sanitize_connection_url(url),
            err: e,
        })?;
    Ok((client, PgConnection::Plain(conn)))
}

pub fn sanitize_connection_url(url: &str) -> String {
    if let Ok(mut parsed) = reqwest::Url::parse(url) {
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        parsed.set_query(None);
        parsed.set_fragment(None);
        return parsed.to_string();
    }
    regex_lite::Regex::new(r"(?i)(password=)[^ ]+")
        .map(|re| re.replace_all(url, "$1[REDACTED]").to_string())
        .unwrap_or_else(|_| "[REDACTED CONNECTION STRING]".to_string())
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
    fn test_parse_ssl_mode_missing_defaults_to_verify_full() {
        assert_eq!(
            parse_ssl_mode_checked("host=localhost user=postgres").unwrap(),
            CheckedSslMode::VerifyFull
        );
    }

    #[test]
    fn test_ssl_modes_are_distinct_and_unknown_rejected() {
        assert_eq!(
            parse_ssl_mode_checked("sslmode=require").unwrap(),
            CheckedSslMode::Require
        );
        assert_eq!(
            parse_ssl_mode_checked("sslmode=verify-ca").unwrap(),
            CheckedSslMode::VerifyCa
        );
        assert_eq!(
            parse_ssl_mode_checked("sslmode=verify-full").unwrap(),
            CheckedSslMode::VerifyFull
        );
        assert!(parse_ssl_mode_checked("sslmode=unknown").is_err());
        assert!(parse_ssl_mode_checked("sslmode=prefer").is_err());
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
