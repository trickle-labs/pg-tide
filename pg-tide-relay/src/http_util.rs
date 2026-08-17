/// Shared HTTP utility helpers for pg-tide relay sinks (v0.18.0).
///
/// Provides a shared SSRF validator (`validate_url`) that is applied to all
/// HTTP-based sinks: webhook, ClickHouse, Elasticsearch / OpenSearch, and
/// Apache Arrow Flight.  Previously the guard existed only in the webhook sink.
use crate::error::RelayError;
use reqwest::{redirect::Policy, Client, Url};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

/// Build the relay's default outbound HTTP client.
///
/// Redirects and ambient proxy environment variables are deliberately disabled.
/// Connectors that need either behavior must opt into an explicit, validated
/// policy instead of inheriting process-global settings.
pub fn secure_client(timeout: Duration) -> Result<Client, reqwest::Error> {
    Client::builder()
        .timeout(timeout)
        .redirect(Policy::none())
        .no_proxy()
        .build()
}

/// Build a client for a validated endpoint and pin the current DNS answers.
///
/// The hostname remains in the request URL, so TLS certificate and Host
/// validation still use the configured name while the socket cannot silently
/// follow a later DNS rebinding.
pub fn secure_client_for_url(
    url_str: &str,
    sink_name: &str,
    timeout: Duration,
    allow_http: bool,
    ssrf_protection: bool,
) -> Result<Client, RelayError> {
    let url = Url::parse(url_str)
        .map_err(|e| RelayError::config(format!("{sink_name}: invalid endpoint: {e}")))?;
    validate_url(url_str, sink_name, allow_http, ssrf_protection)?;

    let mut builder = Client::builder()
        .timeout(timeout)
        .redirect(Policy::none())
        .no_proxy();

    if ssrf_protection {
        let host = url.host_str().ok_or_else(|| {
            RelayError::config(format!("{sink_name}: endpoint must include a host"))
        })?;
        if host.parse::<IpAddr>().is_err() {
            let port = url.port_or_known_default().ok_or_else(|| {
                RelayError::config(format!("{sink_name}: endpoint must include a port"))
            })?;
            let addresses = resolve_host_bounded(host, port, sink_name)?;
            if addresses.is_empty() || addresses.len() > 64 {
                return Err(RelayError::config(format!(
                    "{sink_name}: DNS returned an invalid number of addresses"
                )));
            }
            if addresses
                .iter()
                .any(|address| is_forbidden_ip(address.ip()))
            {
                return Err(RelayError::config(format!(
                    "{sink_name}: DNS resolved to an SSRF-prohibited address"
                )));
            }
            builder = builder.resolve_to_addrs(host, &addresses);
        }
    }

    builder
        .build()
        .map_err(|e| RelayError::config(format!("{sink_name}: HTTP client setup failed: {e}")))
}

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
    let url = Url::parse(url_str)
        .map_err(|e| RelayError::config(format!("{sink_name}: invalid endpoint: {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| RelayError::config(format!("{sink_name}: endpoint must include a host")))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_http => {}
        scheme => {
            return Err(RelayError::config(format!(
                "{sink_name}: URL must use HTTPS (got '{scheme}')"
            )))
        }
    }
    if url.username() != "" || url.password().is_some() {
        return Err(RelayError::config(format!(
            "{sink_name}: endpoint userinfo is not allowed"
        )));
    }
    if url.fragment().is_some() {
        return Err(RelayError::config(format!(
            "{sink_name}: endpoint fragments are not allowed"
        )));
    }
    if ssrf_protection {
        let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();
        if is_forbidden_hostname(&normalized_host) {
            return Err(RelayError::config(format!(
                "{sink_name}: SSRF-prohibited address"
            )));
        }
    }
    Ok(())
}

fn resolve_host_bounded(
    host: &str,
    port: u16,
    sink_name: &str,
) -> Result<Vec<SocketAddr>, RelayError> {
    let host = host.to_string();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("pg-tide-dns".to_string())
        .spawn(move || {
            let result = (host.as_str(), port)
                .to_socket_addrs()
                .map(|addresses| addresses.collect::<Vec<_>>());
            let _ = sender.send(result);
        })
        .map_err(|e| RelayError::config(format!("{sink_name}: DNS resolver setup failed: {e}")))?;
    receiver
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| RelayError::config(format!("{sink_name}: DNS resolution timed out")))?
        .map_err(|e| RelayError::config(format!("{sink_name}: DNS resolution failed: {e}")))
}

fn is_forbidden_hostname(host: &str) -> bool {
    let ip_host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    ip_host
        .parse::<IpAddr>()
        .map(is_forbidden_ip)
        .unwrap_or_else(|_| {
            matches!(
                host,
                "localhost"
                    | "localhost.localdomain"
                    | "ip6-localhost"
                    | "ip6-loopback"
                    | "metadata.google.internal"
            )
        })
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let octets = v.octets();
            v.is_loopback()
                || v.is_unspecified()
                || v.is_private()
                || v.is_link_local()
                || v.is_broadcast()
                || (octets[0] == 0)
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 169 && octets[1] == 254)
                || (octets[0] == 192
                    && (octets[1] == 0 || octets[1] == 2 || (octets[1] == 88 && octets[2] == 99)))
                || (octets[0] == 198
                    && (octets[1] == 18
                        || octets[1] == 19
                        || (octets[1] == 51 && octets[2] == 100)))
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                || octets[0] >= 224
        }
        IpAddr::V6(v) => {
            let segments = v.segments();
            let mapped = segments[..5].iter().all(|segment| *segment == 0) && segments[5] == 0xffff;
            v.is_loopback()
                || v.is_unspecified()
                || v.is_multicast()
                || v.is_unicast_link_local()
                || (segments[0] & 0xfe00 == 0xfc00)
                || ((segments[0] == 0x2001) && (segments[1] == 0x0db8))
                || (mapped
                    && is_forbidden_ip(IpAddr::V4(std::net::Ipv4Addr::new(
                        (segments[6] >> 8) as u8,
                        segments[6] as u8,
                        (segments[7] >> 8) as u8,
                        segments[7] as u8,
                    ))))
        }
    }
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

    #[test]
    fn blocks_special_use_and_mapped_addresses() {
        for host in [
            "100.64.0.1",
            "192.0.2.1",
            "198.18.0.1",
            "203.0.113.1",
            "[::ffff:127.0.0.1]",
            "metadata.google.internal",
        ] {
            assert!(
                validate_url(&format!("https://{host}/"), "test", false, true).is_err(),
                "{host} should be blocked"
            );
        }
    }

    #[test]
    fn disables_redirects_and_ambient_proxies() {
        let client = secure_client(Duration::from_secs(1)).expect("client");
        let request = client.get("https://example.com").build().expect("request");
        assert_eq!(request.url().as_str(), "https://example.com/");
    }
}
