use std::{num::NonZeroU32, time::Duration};

const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_OUTPUT_ATTEMPTS: NonZeroU32 = NonZeroU32::new(6).unwrap();

/// Limit on total attempts, including the initial operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryLimit {
    Unlimited,
    Attempts(NonZeroU32),
}

/// Explicit retry timing and attempt limit shared by transport owners.
///
/// Failure classification remains transport-local. This type only describes
/// what to do after an owner has classified an error as retryable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    limit: RetryLimit,
    initial_backoff: Duration,
    max_backoff: Duration,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryWire {
    #[serde(default)]
    max_attempts: Option<NonZeroU32>,
    #[serde(default = "default_initial_delay_ms")]
    initial_delay_ms: u64,
    #[serde(default = "default_max_delay_ms")]
    max_delay_ms: u64,
}

fn default_initial_delay_ms() -> u64 {
    DEFAULT_INITIAL_BACKOFF.as_millis() as u64
}

fn default_max_delay_ms() -> u64 {
    DEFAULT_MAX_BACKOFF.as_millis() as u64
}

impl<'de> serde::Deserialize<'de> for RetryPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = <RetryWire as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(
            wire.max_attempts
                .map_or(RetryLimit::Unlimited, RetryLimit::Attempts),
            Duration::from_millis(wire.initial_delay_ms),
            Duration::from_millis(wire.max_delay_ms),
        )
        .map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for RetryPolicy {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let milliseconds = |duration: Duration| -> Result<u64, S::Error> {
            if duration.subsec_nanos() % 1_000_000 != 0 {
                return Err(serde::ser::Error::custom(
                    "retry delays must be whole milliseconds",
                ));
            }
            duration
                .as_millis()
                .try_into()
                .map_err(serde::ser::Error::custom)
        };
        serde::Serialize::serialize(
            &RetryWire {
                max_attempts: match self.limit {
                    RetryLimit::Unlimited => None,
                    RetryLimit::Attempts(attempts) => Some(attempts),
                },
                initial_delay_ms: milliseconds(self.initial_backoff)?,
                max_delay_ms: milliseconds(self.max_backoff)?,
            },
            serializer,
        )
    }
}

impl RetryPolicy {
    pub fn new(
        limit: RetryLimit,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, InvalidRetryPolicy> {
        if initial_backoff.is_zero() {
            return Err(InvalidRetryPolicy::ZeroInitialBackoff);
        }
        if max_backoff < initial_backoff {
            return Err(InvalidRetryPolicy::MaxBackoffBeforeInitial);
        }
        Ok(Self {
            limit,
            initial_backoff,
            max_backoff,
        })
    }

    /// Proposed default for long-lived input transports: retry indefinitely.
    pub fn input_default() -> Self {
        Self {
            limit: RetryLimit::Unlimited,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }

    /// Output transport default: the initial attempt plus five retries.
    pub fn output_default() -> Self {
        Self {
            limit: RetryLimit::Attempts(DEFAULT_OUTPUT_ATTEMPTS),
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }

    pub const fn limit(self) -> RetryLimit {
        self.limit
    }

    pub const fn initial_backoff(self) -> Duration {
        self.initial_backoff
    }

    pub const fn max_backoff(self) -> Duration {
        self.max_backoff
    }

    pub const fn tracker(self) -> RetryTracker {
        RetryTracker {
            policy: self,
            consecutive_failures: 0,
        }
    }

    fn backoff_after_failure(self, failure: u32) -> Duration {
        debug_assert!(failure > 0);
        let shift = failure.saturating_sub(1).min(127);
        let multiplier = 1_u128 << shift;
        let nanos = self.initial_backoff.as_nanos().saturating_mul(multiplier);
        let capped = nanos.min(self.max_backoff.as_nanos());
        let seconds = (capped / 1_000_000_000) as u64;
        let subsecond_nanos = (capped % 1_000_000_000) as u32;
        Duration::new(seconds, subsecond_nanos)
    }
}

/// Retry budget for one logical operation or connection recovery sequence.
///
/// Construct it immediately before the initial attempt. Each retryable failure
/// calls [`RetryTracker::record_failure`]. Only an explicit successful
/// operation calls [`RetryTracker::record_success`] and resets the budget.
#[derive(Debug, PartialEq, Eq)]
pub struct RetryTracker {
    policy: RetryPolicy,
    consecutive_failures: u32,
}

impl RetryTracker {
    pub const fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Record a retryable failure and return the delay before the next attempt.
    /// `None` means the total-attempt limit has been exhausted.
    pub fn record_failure(&mut self) -> Option<Duration> {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if let RetryLimit::Attempts(max_attempts) = self.policy.limit {
            if self.consecutive_failures >= max_attempts.get() {
                return None;
            }
        }
        Some(self.policy.backoff_after_failure(self.consecutive_failures))
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Await a previously returned delay without blocking an executor thread.
    pub async fn backoff(delay: Duration) {
        smol::Timer::after(delay).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRetryPolicy {
    #[error("retry initial backoff must be greater than zero")]
    ZeroInitialBackoff,
    #[error("retry maximum backoff must be at least its initial backoff")]
    MaxBackoffBeforeInitial,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_none_has_no_attempt_limit_and_invalid_delays_are_rejected() {
        let unlimited: RetryPolicy = serde_json::from_str(r#"{"max_attempts":null}"#).unwrap();
        assert_eq!(unlimited, RetryPolicy::input_default());
        assert!(serde_json::from_str::<RetryPolicy>(r#"{"max_attempts":0}"#).is_err());
        assert!(serde_json::from_str::<RetryPolicy>(r#"{"initial_delay_ms":0}"#).is_err());
        assert!(
            serde_json::from_str::<RetryPolicy>(r#"{"initial_delay_ms":30,"max_delay_ms":20}"#)
                .is_err()
        );
        let encoded = serde_json::to_string(&RetryPolicy::output_default()).unwrap();
        assert_eq!(
            serde_json::from_str::<RetryPolicy>(&encoded).unwrap(),
            RetryPolicy::output_default()
        );
    }

    #[test]
    fn finite_limit_counts_the_initial_attempt() {
        let mut retries = RetryPolicy::new(
            RetryLimit::Attempts(NonZeroU32::new(3).unwrap()),
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .unwrap()
        .tracker();

        assert_eq!(retries.record_failure(), Some(Duration::from_millis(10)));
        assert_eq!(retries.record_failure(), Some(Duration::from_millis(20)));
        assert_eq!(retries.record_failure(), None);
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        let mut retries = RetryPolicy::new(
            RetryLimit::Unlimited,
            Duration::from_millis(30),
            Duration::from_millis(100),
        )
        .unwrap()
        .tracker();

        assert_eq!(retries.record_failure(), Some(Duration::from_millis(30)));
        assert_eq!(retries.record_failure(), Some(Duration::from_millis(60)));
        assert_eq!(retries.record_failure(), Some(Duration::from_millis(100)));
        assert_eq!(retries.record_failure(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn only_explicit_success_resets_the_budget() {
        let mut retries = RetryPolicy::output_default().tracker();
        assert_eq!(retries.record_failure(), Some(DEFAULT_INITIAL_BACKOFF));
        assert_eq!(retries.consecutive_failures(), 1);
        assert_eq!(retries.consecutive_failures(), 1);

        retries.record_success();
        assert_eq!(retries.consecutive_failures(), 0);
        assert_eq!(retries.record_failure(), Some(DEFAULT_INITIAL_BACKOFF));
    }

    #[test]
    fn proposed_transport_defaults_have_explicit_limits() {
        assert_eq!(RetryPolicy::input_default().limit(), RetryLimit::Unlimited);
        assert_eq!(
            RetryPolicy::output_default().limit(),
            RetryLimit::Attempts(NonZeroU32::new(6).unwrap())
        );
    }

    #[test]
    fn rejects_invalid_backoff_bounds() {
        assert_eq!(
            RetryPolicy::new(
                RetryLimit::Unlimited,
                Duration::ZERO,
                Duration::from_secs(1)
            ),
            Err(InvalidRetryPolicy::ZeroInitialBackoff)
        );
        assert_eq!(
            RetryPolicy::new(
                RetryLimit::Unlimited,
                Duration::from_secs(2),
                Duration::from_secs(1)
            ),
            Err(InvalidRetryPolicy::MaxBackoffBeforeInitial)
        );
    }
}
