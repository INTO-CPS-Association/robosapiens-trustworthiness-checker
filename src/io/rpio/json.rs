//! The producer's canonical JSON codec, reproduced exactly.
//!
//! Producers digest knowledge values as `sha256` over canonical JSON text:
//! sorted object keys, no insignificant whitespace, non-ASCII characters left
//! unescaped, and numbers written the way Python writes them. A digest can only
//! be checked if the value is re-encoded byte for byte, so values are held as
//! [`JsonValue`], which keeps every number as its canonical lexeme. Integers of
//! any size therefore survive; `serde_json::Value` would round large integers
//! through `f64`.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ecow::EcoString;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::core::{NEWTYPE_MARKER, NEWTYPE_VALUE, NewtypeValue, UnionValue, Value};

/// Codec identity of values digested with [`JsonValue::digest`].
pub const CODEC_ID: &str = "rpio-canonical-json/1";

const DIGEST_PREFIX: &str = "sha256:";
const UNION_TAG: &str = "$tag";
const UNION_PAYLOAD: &str = "payload";
// RawValue preserves integer lexemes but skips serde's recursive value guard.
// Bound the actual recursive conversion before building an unbounded value.
const MAX_NESTING: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JsonError {
    #[error("malformed JSON: {0}")]
    Syntax(String),
    #[error("number {0} is not a finite canonical JSON number")]
    NonFinite(String),
    #[error("object {0} uses the reserved `$tag` member without being a tagged union")]
    ReservedTag(String),
    #[error("object {0} uses the reserved `$newtype` member without being a newtype value")]
    ReservedNewtype(String),
}

impl From<serde_json::Error> for JsonError {
    fn from(error: serde_json::Error) -> Self {
        Self::Syntax(error.to_string())
    }
}

/// A JSON number, kept as its canonical text.
///
/// Integers are written in plain decimal with no size limit. Every other number
/// is a finite `f64` written as Python's `repr` writes it, so `1` and `1.0`
/// stay distinct, as they are in the producer's digests.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JsonNumber(EcoString);

impl JsonNumber {
    pub fn from_i64(value: i64) -> Self {
        Self(EcoString::from(value.to_string()))
    }

    pub fn from_u64(value: u64) -> Self {
        Self(EcoString::from(value.to_string()))
    }

    /// A float number, or `None` if `value` is not finite.
    pub fn from_f64(value: f64) -> Option<Self> {
        value
            .is_finite()
            .then(|| Self(EcoString::from(python_float_repr(value))))
    }

    /// Canonicalise a lexeme that is already valid JSON number syntax.
    fn from_lexeme(lexeme: &str) -> Result<Self, JsonError> {
        if lexeme.contains(['.', 'e', 'E']) {
            let value: f64 = lexeme
                .parse()
                .map_err(|_| JsonError::Syntax(format!("bad number {lexeme}")))?;
            return Self::from_f64(value).ok_or_else(|| JsonError::NonFinite(lexeme.to_owned()));
        }
        let digits = lexeme.strip_prefix('-').unwrap_or(lexeme);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(JsonError::Syntax(format!("bad number {lexeme}")));
        }
        // JSON forbids leading zeros, so only negative zero needs rewriting.
        Ok(Self(if digits == "0" {
            "0".into()
        } else {
            lexeme.into()
        }))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_integer(&self) -> bool {
        !self.0.contains(['.', 'e'])
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.is_integer().then(|| self.0.parse().ok()).flatten()
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.is_integer().then(|| self.0.parse().ok()).flatten()
    }

    /// The nearest `f64`. Large integers lose precision.
    pub fn as_f64(&self) -> f64 {
        self.0.parse().expect("canonical numbers parse as f64")
    }
}

/// A JSON value that re-encodes to the producer's canonical text.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum JsonValue {
    #[default]
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(EcoString),
    Array(Vec<JsonValue>),
    Object(BTreeMap<EcoString, JsonValue>),
}

impl JsonValue {
    /// Parse JSON text. Duplicate object keys keep the last value, as the
    /// producer's decoder does.
    pub fn parse(text: &str) -> Result<Self, JsonError> {
        let raw: &RawValue = serde_json::from_str(text)?;
        Self::from_raw(raw)
    }

    pub(crate) fn from_raw(raw: &RawValue) -> Result<Self, JsonError> {
        Self::from_raw_at_depth(raw, 0)
    }

    fn from_raw_at_depth(raw: &RawValue, depth: usize) -> Result<Self, JsonError> {
        let text = raw.get().trim();
        if depth >= MAX_NESTING && matches!(text.as_bytes().first(), Some(b'{' | b'[')) {
            return Err(JsonError::Syntax(format!(
                "recursion limit exceeded: JSON nesting exceeds {MAX_NESTING} containers"
            )));
        }
        Ok(match text.as_bytes().first() {
            Some(b'{') => {
                let members: BTreeMap<String, &RawValue> = serde_json::from_str(text)?;
                Self::Object(
                    members
                        .into_iter()
                        .map(|(key, value)| {
                            Ok((
                                EcoString::from(key),
                                Self::from_raw_at_depth(value, depth + 1)?,
                            ))
                        })
                        .collect::<Result<_, JsonError>>()?,
                )
            }
            Some(b'[') => {
                let items: Vec<&RawValue> = serde_json::from_str(text)?;
                Self::Array(
                    items
                        .into_iter()
                        .map(|value| Self::from_raw_at_depth(value, depth + 1))
                        .collect::<Result<_, _>>()?,
                )
            }
            Some(b'"') => Self::String(serde_json::from_str::<String>(text)?.into()),
            Some(b't' | b'f') => Self::Bool(serde_json::from_str(text)?),
            Some(b'n') => {
                serde_json::from_str::<()>(text)?;
                Self::Null
            }
            _ => Self::Number(JsonNumber::from_lexeme(text)?),
        })
    }

    pub fn string(text: impl Into<EcoString>) -> Self {
        Self::String(text.into())
    }

    pub fn object<K: Into<EcoString>>(members: impl IntoIterator<Item = (K, JsonValue)>) -> Self {
        Self::Object(
            members
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Canonical text: sorted keys, compact separators, Python number forms.
    pub fn canonical_text(&self) -> String {
        let mut text = String::new();
        self.write_canonical(&mut text);
        text
    }

    /// `sha256:<hex>` over the canonical text, as the producer computes it.
    pub fn digest(&self) -> EcoString {
        let hash = self.digest_bytes();
        let mut digest = String::with_capacity(DIGEST_PREFIX.len() + 64);
        digest.push_str(DIGEST_PREFIX);
        for byte in hash {
            write!(digest, "{byte:02x}").expect("writing to a String cannot fail");
        }
        digest.into()
    }

    /// Compact canonical fingerprint for source integrity checks.
    pub(crate) fn digest_bytes(&self) -> [u8; 32] {
        Sha256::digest(self.canonical_text().as_bytes()).into()
    }

    fn write_canonical(&self, text: &mut String) {
        match self {
            Self::Null => text.push_str("null"),
            Self::Bool(true) => text.push_str("true"),
            Self::Bool(false) => text.push_str("false"),
            Self::Number(number) => text.push_str(number.as_str()),
            Self::String(string) => write_string(text, string),
            Self::Array(items) => {
                text.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        text.push(',');
                    }
                    item.write_canonical(text);
                }
                text.push(']');
            }
            Self::Object(members) => {
                text.push('{');
                for (index, (key, value)) in members.iter().enumerate() {
                    if index > 0 {
                        text.push(',');
                    }
                    write_string(text, key);
                    text.push(':');
                    value.write_canonical(text);
                }
                text.push('}');
            }
        }
    }

    /// Convert to a runtime value, following the shared JSON codec.
    ///
    /// `null` becomes `Unit`, and integers outside `i64` become the nearest
    /// float. `$tag` is reserved as in every other checker input: an object
    /// holding a string `$tag` and at most a `payload` member becomes a tagged
    /// union, and any other object that uses `$tag` is rejected. `$newtype`
    /// is reserved likewise: an object of exactly a string `$newtype` and a
    /// `value` becomes a newtype value, unchecked, as every input is.
    pub fn to_value(&self) -> Result<Value, JsonError> {
        Ok(match self {
            Self::Null => Value::Unit,
            Self::Bool(value) => Value::Bool(*value),
            Self::Number(number) => number
                .as_i64()
                .map_or_else(|| Value::Float(number.as_f64()), Value::Int),
            Self::String(text) => Value::Str(text.clone()),
            Self::Array(items) => {
                Value::List(items.iter().map(Self::to_value).collect::<Result<_, _>>()?)
            }
            Self::Object(members) if members.contains_key(NEWTYPE_MARKER) => {
                let (Some(Self::String(brand)), Some(value), 2) = (
                    members.get(NEWTYPE_MARKER),
                    members.get(NEWTYPE_VALUE),
                    members.len(),
                ) else {
                    return Err(JsonError::ReservedNewtype(self.canonical_text()));
                };
                NewtypeValue::new(brand.clone(), value.to_value()?).into()
            }
            Self::Object(members) => match members.get(UNION_TAG) {
                Some(tag) => {
                    let (Self::String(tag), true) = (
                        tag,
                        members
                            .keys()
                            .all(|key| key == UNION_TAG || key == UNION_PAYLOAD),
                    ) else {
                        return Err(JsonError::ReservedTag(self.canonical_text()));
                    };
                    let payload = members.get(UNION_PAYLOAD).map(Self::to_value).transpose()?;
                    UnionValue::new(tag.clone(), payload).into()
                }
                None => Value::Map(
                    members
                        .iter()
                        .map(|(key, value)| Ok((key.clone(), value.to_value()?)))
                        .collect::<Result<_, JsonError>>()?,
                ),
            },
        })
    }
}

impl From<bool> for JsonValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for JsonValue {
    fn from(value: i64) -> Self {
        Self::Number(JsonNumber::from_i64(value))
    }
}

impl From<u64> for JsonValue {
    fn from(value: u64) -> Self {
        Self::Number(JsonNumber::from_u64(value))
    }
}

impl From<&str> for JsonValue {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<EcoString> for JsonValue {
    fn from(value: EcoString) -> Self {
        Self::String(value)
    }
}

impl From<&serde_json::Value> for JsonValue {
    /// Integers keep their exact value; other numbers go through `f64`.
    fn from(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(number) => {
                Self::Number(if let Some(integer) = number.as_i64() {
                    JsonNumber::from_i64(integer)
                } else if let Some(integer) = number.as_u64() {
                    JsonNumber::from_u64(integer)
                } else {
                    JsonNumber::from_f64(number.as_f64().expect("serde_json numbers are finite"))
                        .expect("serde_json numbers are finite")
                })
            }
            serde_json::Value::String(text) => Self::String(text.as_str().into()),
            serde_json::Value::Array(items) => Self::Array(items.iter().map(Self::from).collect()),
            serde_json::Value::Object(members) => Self::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.as_str().into(), Self::from(value)))
                    .collect(),
            ),
        }
    }
}

impl From<serde_json::Value> for JsonValue {
    fn from(value: serde_json::Value) -> Self {
        Self::from(&value)
    }
}

/// Escape a string as Python's `json.dumps(..., ensure_ascii=False)` does.
fn write_string(text: &mut String, string: &str) {
    text.push('"');
    for character in string.chars() {
        match character {
            '"' => text.push_str("\\\""),
            '\\' => text.push_str("\\\\"),
            '\n' => text.push_str("\\n"),
            '\r' => text.push_str("\\r"),
            '\t' => text.push_str("\\t"),
            '\u{8}' => text.push_str("\\b"),
            '\u{c}' => text.push_str("\\f"),
            control if u32::from(control) < 0x20 => {
                write!(text, "\\u{:04x}", u32::from(control))
                    .expect("writing to a String cannot fail");
            }
            other => text.push(other),
        }
    }
    text.push('"');
}

/// Python's `repr` of a finite float: shortest round-trip digits, positional
/// for decimal exponents in `-4..16`, otherwise `d.ddde±XX`.
pub(crate) fn python_float_repr(value: f64) -> String {
    debug_assert!(value.is_finite());
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .to_owned();
    }
    // Rust's `{:e}` also prints the shortest round-trip digits.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` output has an exponent");
    let exponent: i32 = exponent.parse().expect("`{:e}` exponent is an integer");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(mantissa) => ("-", mantissa),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|&c| c != '.').collect();

    let mut text = String::from(sign);
    if (-4..16).contains(&exponent) {
        if exponent >= 0 {
            let integer_digits = exponent as usize + 1;
            if digits.len() <= integer_digits {
                text.push_str(&digits);
                text.push_str(&"0".repeat(integer_digits - digits.len()));
                text.push_str(".0");
            } else {
                text.push_str(&digits[..integer_digits]);
                text.push('.');
                text.push_str(&digits[integer_digits..]);
            }
        } else {
            text.push_str("0.");
            text.push_str(&"0".repeat((-exponent - 1) as usize));
            text.push_str(&digits);
        }
    } else {
        text.push_str(&digits[..1]);
        if digits.len() > 1 {
            text.push('.');
            text.push_str(&digits[1..]);
        }
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        write!(text, "e{exponent_sign}{:02}", exponent.abs())
            .expect("writing to a String cannot fail");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    // R17.1: a value re-encodes to the exact text it was read from, because
    // a digest can only be checked over the same bytes.
    #[test]
    fn canonical_text_sorts_keys_and_keeps_numbers_as_written() {
        let text = r#"{"b":1,"a":[2,3.5],"c":{"z":null,"y":true}}"#;
        let value = JsonValue::parse(text).expect("canonical JSON parses");
        assert_eq!(
            value.canonical_text(),
            r#"{"a":[2,3.5],"b":1,"c":{"y":true,"z":null}}"#
        );
    }

    // R17.2: an integer wider than f64 can hold survives, because it is kept
    // as the text the producer wrote.
    #[test]
    fn wide_integers_survive_re_encoding() {
        let text = r#"{"n":123456789012345678901234567890}"#;
        let value = JsonValue::parse(text).expect("canonical JSON parses");
        assert_eq!(value.canonical_text(), text);
    }

    // R17.3: the digest is over the canonical text, so the same value reached
    // by different spellings digests the same.
    #[test]
    fn the_digest_is_over_the_canonical_text() {
        let spelled = JsonValue::parse(r#"{ "b" : 1 , "a" : 2 }"#).expect("parses");
        let canonical = JsonValue::parse(r#"{"a":2,"b":1}"#).expect("parses");
        assert_eq!(spelled.digest(), canonical.digest());
        assert!(spelled.digest().starts_with("sha256:"));
        assert_eq!(spelled.digest().len(), "sha256:".len() + 64);
    }

    // R17.4: `$tag` is a union everywhere a checker reads JSON, and an object
    // that uses the member for anything else is rejected rather than read as
    // a map that happens to have it.
    #[test]
    fn tagged_objects_become_unions_and_misuse_is_rejected() {
        let nullary = JsonValue::parse(r#"{"$tag":"Stopped"}"#).expect("parses");
        assert_eq!(
            nullary.to_value().expect("a nullary union"),
            Value::from(UnionValue::new("Stopped", None))
        );
        let carried = JsonValue::parse(r#"{"$tag":"Moving","payload":3}"#).expect("parses");
        assert_eq!(
            carried.to_value().expect("a union carrying a payload"),
            Value::from(UnionValue::new("Moving", Some(Value::Int(3))))
        );
        // An absent payload and a null one are different values.
        let null_payload = JsonValue::parse(r#"{"$tag":"Ack","payload":null}"#).expect("parses");
        assert_eq!(
            null_payload.to_value().expect("a union carrying null"),
            Value::from(UnionValue::new("Ack", Some(Value::Unit)))
        );
        for misuse in [
            r#"{"$tag":"Moving","payload":3,"extra":1}"#,
            r#"{"$tag":7}"#,
        ] {
            let value = JsonValue::parse(misuse).expect("parses as JSON");
            assert!(
                matches!(value.to_value(), Err(JsonError::ReservedTag(_))),
                "{misuse}"
            );
        }
    }

    #[test]
    fn complete_deep_arrays_and_objects_are_rejected_before_recursive_conversion() {
        // These are valid, terminated JSON documents: EOF rejection alone
        // would not preserve the former reader's protection from deep input.
        for (open, close) in [("[", "]"), (r#"{"child":"#, "}")] {
            let text = format!("{}0{}", open.repeat(256), close.repeat(256));
            let error = JsonValue::parse(&text).expect_err("over-depth JSON is refused");
            assert!(
                matches!(error, JsonError::Syntax(ref message)
                if message.contains("recursion limit exceeded")),
                "{error}"
            );
        }
    }

    #[test]
    fn maximum_nesting_preserves_wide_integer_lexemes_and_canonical_digest() {
        let text = format!(
            "{}123456789012345678901234567890{}",
            "[".repeat(128),
            "]".repeat(128)
        );
        let value = JsonValue::parse(&text).expect("128 containers are admitted");
        assert_eq!(value.canonical_text(), text);
        assert_eq!(
            value.digest(),
            format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
        );
        let excessive = format!("[{text}]");
        assert!(matches!(
            JsonValue::parse(&excessive),
            Err(JsonError::Syntax(_))
        ));
    }

    // R17.5: what the producer cannot have written is refused.
    #[test]
    fn malformed_and_non_finite_input_is_refused() {
        assert!(matches!(
            JsonValue::parse("{not json}"),
            Err(JsonError::Syntax(_))
        ));
        assert_eq!(JsonNumber::from_f64(f64::NAN), None);
        assert_eq!(JsonNumber::from_f64(f64::INFINITY), None);
    }
}
