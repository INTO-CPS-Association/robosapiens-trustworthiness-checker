//! Language constructs that not every runtime evaluates.
//!
//! A specification states what it needs through
//! [`Specification::first_unsupported`](super::Specification::first_unsupported).
//! Each evaluator declares what it provides where it is implemented (a
//! semantics through `MonitoringSemantics::CAPABILITIES`), and each runtime
//! calls [`admit`] at its own entry point. Nothing lists runtimes centrally:
//! `tests/runtime_capabilities.rs` observes what each runtime does and
//! generates the documented table from that.

use std::fmt;

use super::Specification;
use crate::lang::dsrv::span::Span;

/// A group of constructs a runtime may or may not evaluate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    /// The distribution primitives `dist` and `monitored_at`.
    Distribution,
    /// Building and reading tagged union values.
    TaggedUnions,
    /// Selecting a `match` arm and binding what its pattern matched.
    PatternMatching,
}

impl Capability {
    /// Every capability, in a stable order for tables and tests.
    pub const ALL: &'static [Self] = &[
        Self::Distribution,
        Self::TaggedUnions,
        Self::PatternMatching,
    ];

    const fn bit(self) -> u32 {
        match self {
            Self::Distribution => 1 << 0,
            Self::TaggedUnions => 1 << 1,
            Self::PatternMatching => 1 << 2,
        }
    }

    /// The name used in messages and in the documentation table.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Distribution => "distribution",
            Self::TaggedUnions => "tagged unions",
            Self::PatternMatching => "pattern matching",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of capabilities, usable in constants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Capabilities(u32);

impl Capabilities {
    pub const NONE: Self = Self(0);

    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }

    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    /// What two semantics both provide, for a runtime that may use either.
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
}

/// A construct a specification uses, and the capability it needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Requirement {
    pub capability: Capability,
    pub construct: &'static str,
    pub span: Span,
}

/// A specification uses a construct the runtime cannot evaluate. The runtime
/// reports it before building any stream, instead of failing while it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsupportedConstruct {
    pub requirement: Requirement,
    pub runtime: &'static str,
}

impl std::error::Error for UnsupportedConstruct {}

impl fmt::Display for UnsupportedConstruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Requirement {
            capability,
            construct,
            span,
        } = self.requirement;
        write!(
            f,
            "{construct} at {span:?} cannot run on the {} runtime, which does not support \
             {capability}; see \"Runtime capabilities\" in the documentation",
            self.runtime
        )
    }
}

/// Admit `model` on the runtime named `runtime` if it needs nothing outside
/// the `capabilities` that runtime's evaluator declares.
pub fn admit<S: Specification>(
    model: &S,
    capabilities: Capabilities,
    runtime: &'static str,
) -> Result<(), UnsupportedConstruct> {
    match model.first_unsupported(capabilities) {
        Some(requirement) => Err(UnsupportedConstruct {
            requirement,
            runtime,
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_names_the_construct_runtime_and_capability() {
        let error = UnsupportedConstruct {
            requirement: Requirement {
                capability: Capability::Distribution,
                construct: "`dist`",
                span: Span::new(3, 9),
            },
            runtime: "semi-sync",
        };
        assert_eq!(
            error.to_string(),
            "`dist` at Span { start: 3, end: 9 } cannot run on the semi-sync runtime, which does \
             not support distribution; see \"Runtime capabilities\" in the documentation"
        );
    }
}
