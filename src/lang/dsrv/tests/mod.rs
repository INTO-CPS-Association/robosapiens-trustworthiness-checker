//! DSRV language suites spanning parsing, expansion, checking and execution.
//! Checker-internal tests live under `type_checker::tests`; these exercise the
//! language features through the public pipeline and shared execution fixtures.

mod casts;
mod constants;
mod generics;
mod lexical;
mod matches;
mod modules;
mod std_option;
mod tagged_unions;
mod validation;
mod warnings;
