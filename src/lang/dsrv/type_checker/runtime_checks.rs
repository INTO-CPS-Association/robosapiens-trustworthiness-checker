//! Check runtime values against declared types.

use crate::Value;
use crate::core::{StreamType, check_value_conformance};

/// Whether `value` is of type `typ` as it stands, and otherwise a message
/// saying that it is not.
pub fn check_value_stream_type(typ: &StreamType, value: &Value) -> Result<(), String> {
    check_value_conformance(value, typ)
        .map_err(|_| format!("Type mismatch between {typ} and {value}"))
}
