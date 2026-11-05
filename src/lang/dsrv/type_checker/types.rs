//! Concrete stream types indexed by declared stream names.
use crate::{VarName, core::StreamType};
use std::collections::BTreeMap;
/// Global stream declarations and inferred root types.
pub type StreamTypes = BTreeMap<VarName, StreamType>;
