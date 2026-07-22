//! Pure, fallible operations on runtime [`Value`]s.
//!
//! Runtime adapters decide how stream markers and evaluation failures propagate.

mod collections;
mod dispatch;
mod numeric;

use super::Value;

pub use collections::{
    list_append, list_concat, list_head, list_index, list_len, list_tail, map_get, map_has_key,
    map_insert, map_remove,
};
pub use dispatch::{BinaryValueOp, UnaryValueOp, ValueOpError, binary, unary};

#[cfg(test)]
mod tests;
