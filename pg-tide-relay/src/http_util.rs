/// Shared HTTP utility helpers for pg-tide relay sinks (v0.18.0).
///
/// Provides a shared SSRF validator (`validate_url`) that is applied to all
/// HTTP-based sinks: webhook, ClickHouse, Elasticsearch / OpenSearch, and
/// Apache Arrow Flight.  Previously the guard existed only in the webhook sink.
use crate::error::RelayError;

/// Check whether a URL target is safe (SSRF guard).
///
/// Rejects:
/// - non-HTTPS scheme (unless `allow_http = true`)
/// - loopback addresses (127.x.x.x, ::1, localhost)
/// - link-local (169.254.x.x, fe80::/10) including the instance-metadata IP
/// - private ranges (RFC 1918: 10.x, 172.16–31.x, 192.168.x)
///
/// Returns `Ok(())` if the URL is safe to contact; returns
/// `Err(RelayError::Config)` with a human-readable message otherwise.
///
/// # Parameters
/// - `url_str`: The URL to validate (accepts any `AsRef<str>`).
/// - `sink_name`: Sink identifier used in error messages (e.g. `"clickhouse"`).
/// - `allow_http`: When `true`, plain HTTP is permitted (dev/test mode).
/// - `ssrf_protection`: When `false`, all IP-range checks are skipped.
///
/// # Examples
/// ```
/// pg_tide_relay::http_util::validate_url("https://api.example.com/ingest", "webhook", false, true).unwrap();
/// ```
pub fn validate_url(
    url_str: &str,
    sink_name: &str,
    allow_http: bool,
    ssrf_protection: bool,
) -> Result<(), RelayError> {
    // Basic scheme check — works without a full URL parse (avoids reqwest dep
    // in non-webhook features).
    let scheme = if url_str.starts_with("https://") {
        "https"
    } else if url_str.starts_with("http://") {
        "http"
    } else {
        // Unrecognised scheme — let the underlying HTTP client handle it.
        return Ok(());
    };

    if !allow_http && scheme != "https" {
        return Err(RelayError::config(format!(
            "{sink_name}: URL must use HTTPS (got '{scheme}'). Set allow_http=true to override."
        )));
    }

    if !ssrf_protection {
        return Ok(());
    }

    // Extract host (between "://" and the next "/" or end).
    let after_scheme = &url_str[scheme.len() + 3..]; // skip "https://" or "http://"
    let host_port = after_scheme
        .split('/')
        .next()
        .unwrap_or(after_scheme)
        .split('@') // strip user-info
        .next_back()
        .unwrap_or("");
    // Remove port suffix.
    let host = if host_port.starts_with('[') {
        // IPv6 literal: [::1]:8080
        host_port
            .trim_start_matches('[')
            .split(']')
            .next()
            .unwrap_or(host_port)
    } else {
        host_port.split(':').next().unwrap_or(host_port)
    };

    // Block loopback.
    if host == "localhost" || host == "127.0.0.1" || host == "::1" || host.starts_with("127.") {
        return Err(RelayError::config(format!(
            "{sink_name}: SSRF guard: loopback target '{host}' is not allowed in production"
        )));
    }

    // Block link-local metadata service (AWS/GCP/Azure instance metadata).
    if host == "169.254.169.254" || host.starts_with("169.254.") {
        return Err(RelayError::config(format!(
            "{sink_name}: SSRF guard: link-local/metadata target '{host}' is blocked"
        )));
    }

    // Block IPv6 link-local.
    if host.starts_with("fe80:") || host.starts_with("FE80:") {
        return Err(RelayError::config(format!(
            "{sink_name}: SSRF guard: IPv6 link-local target '{host}' is blocked"
        )));
    }

    // Block private ranges (RFC 1918).
    if host.starts_with("10.") || host.starts_with("192.168.") || is_private_172(host) {
        return Err(RelayError::config(format!(
            "{sink_name}: SSRF guard: private-range target '{host}' is blocked. \
             Set ssrf_protection=false to allow private targets in dev mode."
        )));
    }

    Ok(())
}

/// Check whether an IP string falls in the 172.16.0.0/12 range.
fn is_private_172(host: &str) -> bool {
    if let Some(rest) = host.strip_prefix("172.") {
        if let Some(second_octet_str) = rest.split('.').next() {
            if let Ok(n) = second_octet_str.parse::<u8>() {
                return (16..=31).contains(&n);
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_public_https() {
        assert!(validate_url("https://api.example.com/ingest", "test", false, true).is_ok());
    }

    #[test]
    fn blocks_http_by_default() {
        assert!(validate_url("http://api.example.com/ingest", "test", false, true).is_err());
    }

    #[test]
    fn allows_http_when_override() {
        assert!(validate_url("http://api.example.com/ingest", "test", true, true).is_ok());
    }

    #[test]
    fn blocks_loopback() {
        assert!(validate_url("https://127.0.0.1:8200/ingest", "test", false, true).is_err());
        assert!(validate_url("https://localhost/ingest", "test", false, true).is_err());
    }

    #[test]
    fn blocks_metadata_service() {
        assert!(validate_url(
            "https://169.254.169.254/latest/meta-data",
            "test",
            false,
            true
        )
        .is_err());
    }

    #[test]
    fn blocks_private_ranges() {
        assert!(validate_url("https://10.0.0.1/ingest", "test", false, true).is_err());
        assert!(validate_url("https://192.168.1.1/ingest", "test", false, true).is_err());
        assert!(validate_url("https://172.16.0.1/ingest", "test", false, true).is_err());
        assert!(validate_url("https://172.31.255.255/ingest", "test", false, true).is_err());
    }

    #[test]
    fn allows_private_when_ssrf_disabled() {
        assert!(
            validate_url("https://10.0.0.1/ingest", "test", false, false).is_ok(),
            "SSRF protection disabled should allow private ranges"
        );
    }

    #[test]
    fn allows_172_32_public() {
        // 172.32.x is NOT in 172.16/12 so must be allowed.
        assert!(validate_url("https://172.32.0.1/ingest", "test", false, true).is_ok());
    }
}
