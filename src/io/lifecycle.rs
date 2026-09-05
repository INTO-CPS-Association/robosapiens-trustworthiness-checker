use std::{
    fmt,
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use futures::{future::Either, pin_mut};

static NEXT_PIPELINE_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

fn next_identity(counter: &AtomicU64, kind: &str) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("exhausted {kind} identities"))
}

/// Identity of one immutable pipeline configuration.
///
/// Cloning a configuration preserves its generation. A durable configuration
/// edit must create a fresh generation with [`PipelineGeneration::new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PipelineGeneration(u64);

impl PipelineGeneration {
    pub fn new() -> Self {
        Self(next_identity(
            &NEXT_PIPELINE_GENERATION,
            "pipeline generation",
        ))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl Default for PipelineGeneration {
    fn default() -> Self {
        Self::new()
    }
}

/// Identity of one opened input/output session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(u64);

impl SessionId {
    pub fn new() -> Self {
        Self(next_identity(&NEXT_SESSION_ID, "session"))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Monotonic revision of a live session's committed bindings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionRevision(u64);

impl SessionRevision {
    pub const INITIAL: Self = Self(0);

    pub const fn initial() -> Self {
        Self::INITIAL
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// Return the revision to publish after a coordinated change commits.
    pub fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("exhausted session revisions"))
    }
}

/// One absolute deadline shared by every layer of a graceful shutdown.
///
/// `ShutdownDeadline::none()` permits an unlimited graceful shutdown. Passing
/// this value down the ownership stack avoids restarting a relative timeout at
/// each layer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShutdownDeadline(Option<Instant>);

impl ShutdownDeadline {
    pub const fn new(deadline: Option<Instant>) -> Self {
        Self(deadline)
    }

    pub const fn none() -> Self {
        Self(None)
    }

    pub const fn at(deadline: Instant) -> Self {
        Self(Some(deadline))
    }

    pub fn after(duration: Duration) -> Self {
        Self(Some(
            Instant::now()
                .checked_add(duration)
                .expect("shutdown deadline exceeds Instant's range"),
        ))
    }

    pub const fn instant(self) -> Option<Instant> {
        self.0
    }

    pub fn remaining(self) -> Option<Duration> {
        self.0
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn is_expired(self) -> bool {
        self.0.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Run `operation` until completion or this deadline.
    ///
    /// Dropping the returned future also drops both the operation and timer.
    /// Callers retain ownership of cancellation and wrap [`ShutdownTimeout`]
    /// in their direction-specific error type.
    pub async fn timeout<F: Future>(self, operation: F) -> Result<F::Output, ShutdownTimeout> {
        let Some(deadline) = self.0 else {
            return Ok(operation.await);
        };

        if Instant::now() >= deadline {
            return Err(ShutdownTimeout { deadline });
        }

        let timer = smol::Timer::at(deadline);
        pin_mut!(operation, timer);
        match futures::future::select(operation, timer).await {
            Either::Left((result, _)) => Ok(result),
            Either::Right((_, _)) => Err(ShutdownTimeout { deadline }),
        }
    }
}

impl From<Option<Instant>> for ShutdownDeadline {
    fn from(deadline: Option<Instant>) -> Self {
        Self::new(deadline)
    }
}

/// Indicates that graceful shutdown did not complete by its absolute deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShutdownTimeout {
    deadline: Instant,
}

impl ShutdownTimeout {
    pub const fn deadline(self) -> Instant {
        self.deadline
    }
}

impl fmt::Display for ShutdownTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "graceful shutdown deadline expired")
    }
}

impl std::error::Error for ShutdownTimeout {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_identity_is_stable_and_fresh_identity_is_distinct() {
        let generation = PipelineGeneration::new();
        assert_eq!(generation, generation.clone());
        assert_ne!(generation, PipelineGeneration::new());

        let session = SessionId::new();
        assert_eq!(session, session.clone());
        assert_ne!(session, SessionId::new());
    }

    #[test]
    fn session_revision_advances_without_changing_the_old_value() {
        let initial = SessionRevision::initial();
        let committed = initial.next();
        assert_eq!(initial.get(), 0);
        assert_eq!(committed.get(), 1);
        assert_ne!(initial, committed);
    }

    #[test]
    fn unlimited_deadline_waits_for_completion() {
        let result = smol::block_on(ShutdownDeadline::none().timeout(async { 42 }));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn expired_deadline_does_not_poll_the_operation() {
        use std::{cell::Cell, rc::Rc, task::Poll};

        let polled = Rc::new(Cell::new(false));
        let operation_polled = polled.clone();
        let operation = std::future::poll_fn(move |_| {
            operation_polled.set(true);
            Poll::<()>::Pending
        });
        let deadline = Instant::now();
        let error = smol::block_on(ShutdownDeadline::at(deadline).timeout(operation)).unwrap_err();

        assert_eq!(error.deadline(), deadline);
        assert!(!polled.get());
    }
}
