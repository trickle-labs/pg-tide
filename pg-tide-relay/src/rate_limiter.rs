/// Rate limiting & back-pressure (RELAY-P2-15).
///
/// Configurable token-bucket rate limiter applied between the relay loop and
/// the sink.  When the rate limiter is full, the relay loop sleeps, propagating
/// back-pressure upstream: the outbox poller leaves rows in the outbox and
/// source brokers retain messages until the relay is ready.
///
/// Configuration in the pipeline's `config` JSONB column:
///
/// ```json
/// {
///   "rate_limit": {
///     "enabled": true,
///     "max_messages_per_second": 1000,
///     "burst_size": 2000
///   }
/// }
/// ```
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};

/// Rate limiting configuration for a pipeline.
#[derive(Debug, Clone, Default)]
pub struct RateLimitConfig {
    /// Whether rate limiting is enabled.
    pub enabled: bool,
    /// Maximum messages per second (0 = unlimited).
    pub max_messages_per_second: u32,
    /// Burst capacity above the steady-state rate.
    pub burst_size: u32,
}

impl RateLimitConfig {
    /// Parse rate limit config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let rl = match config.get("rate_limit") {
            Some(r) => r,
            None => return Self::default(),
        };

        let enabled = rl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        let mps = rl
            .get("max_messages_per_second")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let burst = rl
            .get("burst_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(mps as u64 * 2) as u32;

        Self {
            enabled,
            max_messages_per_second: mps,
            burst_size: burst,
        }
    }
}

/// A per-pipeline rate limiter.
pub struct PipelineRateLimiter {
    inner: Option<Arc<DefaultDirectRateLimiter>>,
    config: RateLimitConfig,
}

impl PipelineRateLimiter {
    /// Build a rate limiter from config.
    pub fn new(config: RateLimitConfig) -> Self {
        if !config.enabled || config.max_messages_per_second == 0 {
            return Self {
                inner: None,
                config,
            };
        }

        let mps = match NonZeroU32::new(config.max_messages_per_second) {
            Some(n) => n,
            None => {
                return Self {
                    inner: None,
                    config,
                }
            }
        };

        let burst =
            NonZeroU32::new(config.burst_size.max(config.max_messages_per_second)).unwrap_or(mps);

        let quota = Quota::per_second(mps).allow_burst(burst);
        let limiter = Arc::new(RateLimiter::direct(quota));

        Self {
            inner: Some(limiter),
            config,
        }
    }

    /// Wait until `count` tokens are available.
    /// Returns immediately if rate limiting is disabled.
    pub async fn acquire(&self, count: u32) {
        let limiter = match &self.inner {
            Some(l) => l,
            None => return,
        };

        if count == 0 {
            return;
        }

        // Use individual token acquisition in a loop for simplicity.
        // For high-throughput paths, batch acquisition can be added.
        // v0.24.0: Use NonZeroU32::MIN (stable since Rust 1.79) instead of
        // .expect("1 is non-zero") to remove the last production-reachable
        // expect() in the rate-limiter path.
        let cells = NonZeroU32::new(count).unwrap_or(NonZeroU32::MIN);
        // governor's `until_n_ready` is the correct API for bulk acquire.
        if let Err(_insufficient) = limiter.check_n(cells) {
            // Not enough burst capacity — wait until all tokens are available.
            limiter.until_n_ready(cells).await.ok();
        }
    }

    /// Check if rate limiting is active.
    pub fn is_active(&self) -> bool {
        self.inner.is_some()
    }

    /// Return the configured rate (messages/sec), or 0 if unlimited.
    pub fn messages_per_second(&self) -> u32 {
        self.config.max_messages_per_second
    }
}

/// Build a rate limiter from a pipeline's JSON config.
pub fn build_rate_limiter(config: &serde_json::Value) -> PipelineRateLimiter {
    let rl_config = RateLimitConfig::from_pipeline_config(config);
    PipelineRateLimiter::new(rl_config)
}

/// Estimated time to process a batch at the given rate (for back-pressure).
pub fn estimated_delay(messages_per_second: u32, batch_size: usize) -> Duration {
    if messages_per_second == 0 || batch_size == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis((batch_size as u64 * 1000) / messages_per_second as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_rate_limiter_is_inactive() {
        let config = RateLimitConfig::default();
        let limiter = PipelineRateLimiter::new(config);
        assert!(!limiter.is_active());
    }

    #[test]
    fn test_enabled_rate_limiter_is_active() {
        let config = RateLimitConfig {
            enabled: true,
            max_messages_per_second: 100,
            burst_size: 200,
        };
        let limiter = PipelineRateLimiter::new(config);
        assert!(limiter.is_active());
        assert_eq!(limiter.messages_per_second(), 100);
    }

    #[test]
    fn test_zero_rate_is_inactive() {
        let config = RateLimitConfig {
            enabled: true,
            max_messages_per_second: 0,
            burst_size: 0,
        };
        let limiter = PipelineRateLimiter::new(config);
        assert!(!limiter.is_active());
    }

    #[test]
    fn test_parse_rate_limit_config() {
        let config = serde_json::json!({
            "rate_limit": {
                "enabled": true,
                "max_messages_per_second": 500,
                "burst_size": 1000
            }
        });
        let rl = RateLimitConfig::from_pipeline_config(&config);
        assert!(rl.enabled);
        assert_eq!(rl.max_messages_per_second, 500);
        assert_eq!(rl.burst_size, 1000);
    }

    #[test]
    fn test_estimated_delay() {
        assert_eq!(estimated_delay(1000, 100), Duration::from_millis(100));
        assert_eq!(estimated_delay(0, 100), Duration::ZERO);
        assert_eq!(estimated_delay(1000, 0), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_acquire_with_unlimited_does_not_block() {
        let limiter = PipelineRateLimiter::new(RateLimitConfig::default());
        // Should complete immediately without hanging.
        limiter.acquire(1000).await;
    }

    #[tokio::test]
    async fn test_acquire_single_token() {
        let config = RateLimitConfig {
            enabled: true,
            max_messages_per_second: 10_000,
            burst_size: 10_000,
        };
        let limiter = PipelineRateLimiter::new(config);
        // A single token should be available instantly from burst.
        limiter.acquire(1).await;
    }
}
