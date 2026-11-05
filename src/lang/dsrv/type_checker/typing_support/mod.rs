//! Source typing policies shared by expression cases .
//!
//! Collections, literals, operators and patterns supply the
//! supporting algorithms for `rules`. The recursive checking protocol belongs to
//! `checking`; source-type translation belongs to the instantiation library.

pub(super) mod collections;
pub(super) mod literals;
pub(super) mod operators;
pub(super) mod patterns;
