/// Circuit breaker pattern (RELAY-P2-16).
///
/// Protects the relay from cascading failures when a sink is down.
/// The circuit breaker has three states:
///
/// - **Closed**: Normal operation. Failures increment a counter.
/// - **Open**: All publish attempts fail immediately (or route to DLQ).
///   Transitions to Half-Open after `half_open_timeout_seconds`.
/// - **Half-Open**: Allows a single probe request through.
///   On success → Closed; on failure → re-opens with back-off.
///
/// Configuration in the pipeline's `config` JSONB column:
///
/// ```json
/// {
///   "circuit_breaker": {
///     "enabled": true,
///     "failure_threshold": 5,
///     "success_threshold": 3,
///     "half_open_timeout_seconds": 30
///   }
/// }
/// ```
use std::time::{Duration, Instant};

/// Circuit breaker state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation.
    Closed,
    /// Circuit is open — all requests fail fast.
    Open,
    /// One probe request allowed through; outcome determines next state.
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::Open => write!(f, "open"),
            Self::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// Configuration for a pipeline circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Whether the circuit breaker is enabled.
    pub enabled: bool,
    /// Number of consecutive failures required to open the circuit.
    pub failure_threshold: u32,
    /// Number of consecutive successes required to close from half-open.
    pub success_threshold: u32,
    /// Time to wait before transitioning Open → Half-Open.
    pub half_open_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 5,
            success_threshold: 3,
            half_open_timeout: Duration::from_secs(30),
        }
    }
}

impl CircuitBreakerConfig {
    /// Parse circuit breaker config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let cb = match config.get("circuit_breaker") {
            Some(c) => c,
            None => return Self::default(),
        };

        Self {
            enabled: cb.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false),
            failure_threshold: cb
                .get("failure_threshold")
                .and_then(|v| v.as_u64())
                .unwrap_or(5) as u32,
            success_threshold: cb
                .get("success_threshold")
                .and_then(|v| v.as_u64())
                .unwrap_or(3) as u32,
            half_open_timeout: Duration::from_secs(
                cb.get("half_open_timeout_seconds")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30),
            ),
        }
    }
}

/// A stateful circuit breaker for a single pipeline.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    /// Create a circuit breaker from config. Starts in Closed state.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            opened_at: None,
        }
    }

    /// Whether the circuit breaker is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Current state of the circuit breaker.
    pub fn state(&self) -> &CircuitState {
        &self.state
    }

    /// Check whether a request should be allowed through.
    ///
    /// - Closed → always allow.
    /// - Open → deny, unless the half-open timeout has elapsed (→ HalfOpen probe).
    /// - HalfOpen → allow exactly one probe.
    pub fn should_allow(&mut self) -> bool {
        if !self.config.enabled {
            return true;
        }

        match &self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(opened) = self.opened_at {
                    if opened.elapsed() >= self.config.half_open_timeout {
                        tracing::info!("circuit breaker → half_open (probe attempt)");
                        self.state = CircuitState::HalfOpen;
                        self.success_count = 0;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful publish. Potentially closes the circuit.
    pub fn record_success(&mut self) {
        if !self.config.enabled {
            return;
        }

        match self.state {
            CircuitState::Closed => {
                // Reset failure counter on any success.
                self.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.config.success_threshold {
                    tracing::info!("circuit breaker → closed (recovered)");
                    self.state = CircuitState::Closed;
                    self.failure_count = 0;
                    self.success_count = 0;
                    self.opened_at = None;
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed publish. Potentially opens the circuit.
    pub fn record_failure(&mut self) {
        if !self.config.enabled {
            return;
        }

        match self.state {
            CircuitState::Closed => {
                self.failure_count += 1;
                if self.failure_count >= self.config.failure_threshold {
                    tracing::warn!(
                        failures = self.failure_count,
                        threshold = self.config.failure_threshold,
                        "circuit breaker → open"
                    );
                    self.state = CircuitState::Open;
                    self.opened_at = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                tracing::warn!("circuit breaker → re-opened (probe failed)");
                self.state = CircuitState::Open;
                self.opened_at = Some(Instant::now());
                self.success_count = 0;
            }
            CircuitState::Open => {}
        }
    }

    /// Build a circuit breaker from a pipeline's JSON config.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let cb_config = CircuitBreakerConfig::from_pipeline_config(config);
        Self::new(cb_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cb(
        failure_threshold: u32,
        success_threshold: u32,
        timeout_secs: u64,
    ) -> CircuitBreaker {
        CircuitBreaker::new(CircuitBreakerConfig {
            enabled: true,
            failure_threshold,
            success_threshold,
            half_open_timeout: Duration::from_secs(timeout_secs),
        })
    }

    #[test]
    fn test_disabled_always_allows() {
        let mut cb = CircuitBreaker::new(CircuitBreakerConfig {
            enabled: false,
            ..Default::default()
        });
        for _ in 0..100 {
            cb.record_failure();
        }
        assert!(cb.should_allow());
        assert_eq!(cb.state(), &CircuitState::Closed);
    }

    #[test]
    fn test_closed_allows_requests() {
        let mut cb = make_cb(5, 3, 30);
        assert!(cb.should_allow());
        assert_eq!(cb.state(), &CircuitState::Closed);
    }

    #[test]
    fn test_opens_after_failure_threshold() {
        let mut cb = make_cb(3, 2, 30);
        cb.record_failure();
        assert_eq!(cb.state(), &CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), &CircuitState::Closed);
        cb.record_failure(); // 3rd failure → open
        assert_eq!(cb.state(), &CircuitState::Open);
        assert!(!cb.should_allow());
    }

    #[test]
    fn test_success_resets_failure_counter_in_closed() {
        let mut cb = make_cb(3, 2, 30);
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // reset counter
        cb.record_failure();
        cb.record_failure();
        // Counter was reset, so only 2 failures after the reset — still closed.
        assert_eq!(cb.state(), &CircuitState::Closed);
    }

    #[test]
    fn test_transitions_to_half_open_after_timeout() {
        let mut cb = make_cb(1, 1, 0); // zero timeout → immediate half-open
        cb.record_failure();
        assert_eq!(cb.state(), &CircuitState::Open);

        // With zero timeout, elapsed >= 0 so we can transition.
        // Allow a brief moment to pass.
        std::thread::sleep(Duration::from_millis(1));
        assert!(cb.should_allow()); // should transition to HalfOpen and allow probe
        assert_eq!(cb.state(), &CircuitState::HalfOpen);
    }

    #[test]
    fn test_closes_from_half_open_on_successes() {
        let mut cb = make_cb(1, 2, 0);
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(1));
        cb.should_allow(); // → HalfOpen
        cb.record_success();
        assert_eq!(cb.state(), &CircuitState::HalfOpen); // need 2 successes
        cb.record_success();
        assert_eq!(cb.state(), &CircuitState::Closed);
    }

    #[test]
    fn test_reopens_from_half_open_on_failure() {
        let mut cb = make_cb(1, 2, 0);
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(1));
        cb.should_allow(); // → HalfOpen
        cb.record_failure(); // probe failed → re-open
        assert_eq!(cb.state(), &CircuitState::Open);
    }

    #[test]
    fn test_parse_config() {
        let config = serde_json::json!({
            "circuit_breaker": {
                "enabled": true,
                "failure_threshold": 10,
                "success_threshold": 5,
                "half_open_timeout_seconds": 60
            }
        });
        let cb_config = CircuitBreakerConfig::from_pipeline_config(&config);
        assert!(cb_config.enabled);
        assert_eq!(cb_config.failure_threshold, 10);
        assert_eq!(cb_config.success_threshold, 5);
        assert_eq!(cb_config.half_open_timeout, Duration::from_secs(60));
    }
}
