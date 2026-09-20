use std::{any::Any, collections::BTreeMap, fmt::Debug, fmt::Display, rc::Rc};

use anyhow::anyhow;
use ecow::{EcoString, EcoVec};

#[cfg(feature = "redis")]
use redis::FromRedisValue;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value as JValue;

use super::{ClosedUnion, UnionValue};
use std::fmt;

use crate::core::{JsonStreamValue, LocalStream};

pub type RuntimeFunctionCallable =
    Rc<dyn Fn(EcoVec<Value>) -> anyhow::Result<LocalStream<Value>> + 'static>;
pub type RuntimeFunctionValueCallable =
    Rc<dyn Fn(EcoVec<Value>) -> anyhow::Result<Value> + 'static>;
pub type RuntimeFunctionValueFactory = Rc<dyn Fn() -> RuntimeFunctionValueCallable + 'static>;

/// A cheaply cloned handle to a function definition.
///
/// Keeping the implementation behind one pointer prevents the function variant from enlarging
/// every [`Value`]. The shared allocation also provides stable definition identity.
#[derive(Clone)]
pub struct RuntimeFunction {
    inner: Rc<RuntimeFunctionInner>,
}

#[derive(Clone)]
struct RuntimeFunctionInner {
    display: EcoString,
    callable: Option<RuntimeFunctionCallable>,
    value_callable: Option<RuntimeFunctionValueCallable>,
    value_factory: Option<RuntimeFunctionValueFactory>,
    call_site_stateful: bool,
    language_payload: Option<Rc<dyn Any>>,
}

impl RuntimeFunction {
    pub fn opaque(display: impl Into<EcoString>) -> Self {
        Self {
            inner: Rc::new(RuntimeFunctionInner {
                display: display.into(),
                callable: None,
                value_callable: None,
                value_factory: None,
                call_site_stateful: false,
                language_payload: None,
            }),
        }
    }

    pub fn native(
        display: impl Into<EcoString>,
        callable: impl Fn(EcoVec<Value>) -> anyhow::Result<LocalStream<Value>> + 'static,
    ) -> Self {
        Self {
            inner: Rc::new(RuntimeFunctionInner {
                display: display.into(),
                callable: Some(Rc::new(callable)),
                value_callable: None,
                value_factory: None,
                call_site_stateful: false,
                language_payload: None,
            }),
        }
    }

    pub fn native_value(
        display: impl Into<EcoString>,
        callable: impl Fn(EcoVec<Value>) -> anyhow::Result<Value> + 'static,
    ) -> Self {
        let callable: RuntimeFunctionValueCallable = Rc::new(callable);
        let stream_callable = callable.clone();
        Self {
            inner: Rc::new(RuntimeFunctionInner {
                display: display.into(),
                callable: Some(Rc::new(move |args| {
                    let value = stream_callable(args)?;
                    Ok(Box::pin(futures::stream::iter(vec![value])) as LocalStream<Value>)
                })),
                value_callable: Some(callable.clone()),
                value_factory: Some(Rc::new(move || callable.clone())),
                call_site_stateful: false,
                language_payload: None,
            }),
        }
    }

    /// Construct a function whose mutable execution state belongs to each call site.
    pub(crate) fn value_factory(
        display: impl Into<EcoString>,
        call_site_stateful: bool,
        factory: impl Fn() -> RuntimeFunctionValueCallable + 'static,
    ) -> Self {
        Self {
            inner: Rc::new(RuntimeFunctionInner {
                display: display.into(),
                callable: None,
                value_callable: None,
                value_factory: Some(Rc::new(factory)),
                call_site_stateful,
                language_payload: None,
            }),
        }
    }

    pub(crate) fn with_language_payload<T: Any>(mut self, payload: Rc<T>) -> Self {
        Rc::make_mut(&mut self.inner).language_payload = Some(payload);
        self
    }

    pub(crate) fn language_payload<T: Any>(&self) -> Option<Rc<T>> {
        Rc::clone(self.inner.language_payload.as_ref()?)
            .downcast()
            .ok()
    }

    pub fn display_source(&self) -> &EcoString {
        &self.inner.display
    }

    pub fn call(&self, args: EcoVec<Value>) -> anyhow::Result<LocalStream<Value>> {
        let Some(callable) = &self.inner.callable else {
            return Err(anyhow!(
                "Function {} is display-only and cannot be called",
                self.inner.display
            ));
        };
        callable(args)
    }

    pub fn call_value(&self, args: EcoVec<Value>) -> anyhow::Result<Value> {
        let Some(callable) = &self.inner.value_callable else {
            return Err(anyhow!(
                "Function {} has no direct value callable",
                self.inner.display
            ));
        };
        callable(args)
    }

    pub fn has_value_callable(&self) -> bool {
        self.inner.value_callable.is_some()
    }

    pub(crate) fn supports_value_calls(&self) -> bool {
        self.inner.value_callable.is_some() || self.inner.value_factory.is_some()
    }

    pub(crate) fn instantiate_value(&self) -> Option<RuntimeFunctionValueCallable> {
        self.inner.value_factory.as_ref().map(|factory| factory())
    }

    pub(crate) fn same_definition(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn requires_call_site_instance(&self) -> bool {
        self.inner.call_site_stateful
    }

    pub fn is_callable(&self) -> bool {
        self.inner.callable.is_some()
            || self.inner.value_callable.is_some()
            || self.inner.value_factory.is_some()
    }
}

impl Debug for RuntimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeFunction")
            .field("display", &self.inner.display)
            .field("callable", &self.inner.callable.is_some())
            .field("value_callable", &self.inner.value_callable.is_some())
            .field("value_factory", &self.inner.value_factory.is_some())
            .field("call_site_stateful", &self.inner.call_site_stateful)
            .field("language_payload", &self.inner.language_payload.is_some())
            .finish()
    }
}

impl Display for RuntimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.display)
    }
}

impl PartialEq for RuntimeFunction {
    fn eq(&self, other: &Self) -> bool {
        self.same_definition(other)
    }
}

// Anything inside a stream should be clonable in O(1) time in order for the
// runtimes to be efficiently implemented. This is why we use EcoString and
// EcoVec instead of String and Vec. These types are essentially references
// which allow mutation in place if there is only one reference to the data or
// copy-on-write if there is more than one reference.
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(EcoString),
    Bool(bool),
    Function(RuntimeFunction),
    /// A tagged union value. Schema-free at run time: the schema lives in the
    /// elaborated tree, which is what resolves a constructor.
    Union(Rc<UnionValue>),
    List(EcoVec<Value>),
    Tuple(EcoVec<Value>),
    Map(BTreeMap<EcoString, Value>),
    Unit,     // Indicates the absence of a value
    Deferred, // Indicates a value that cannot yet be computed due to lack of history
    NoVal,    // Indicates no value for the current stream step (due to async stream inputs)
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Function(a), Self::Function(b)) => a == b,
            (Self::List(a), Self::List(b)) | (Self::Tuple(a), Self::Tuple(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            // Not `Rc` identity: a shared payload holding NaN is not equal to itself.
            (Self::Union(a), Self::Union(b)) => a.as_ref() == b.as_ref(),
            (Self::Unit, Self::Unit)
            | (Self::Deferred, Self::Deferred)
            | (Self::NoVal, Self::NoVal) => true,
            _ => false,
        }
    }
}

impl StreamData for Value {
    fn is_no_val(&self) -> bool {
        matches!(self, Value::NoVal)
    }
}

impl Value {
    pub(crate) fn requires_json5_encoding(&self) -> bool {
        match self {
            Value::Float(value) => !value.is_finite(),
            Value::List(values) | Value::Tuple(values) => {
                values.iter().any(Value::requires_json5_encoding)
            }
            Value::Map(values) => values.values().any(Value::requires_json5_encoding),
            Value::Union(value) => value.payload().is_some_and(Value::requires_json5_encoding),
            _ => false,
        }
    }
}

impl JsonStreamValue for Value {
    fn decode_json(payload: &[u8]) -> anyhow::Result<Self> {
        let text = std::str::from_utf8(payload)
            .map_err(|error| anyhow::anyhow!(error).context("JSON5 payload is not UTF-8"))?;
        json5::from_str(text).map_err(|error| {
            anyhow::anyhow!(error).context("failed to decode stream value as JSON5")
        })
    }

    fn encode_json(&self) -> anyhow::Result<String> {
        crate::core::json::encode_json_or_json5(self, self.requires_json5_encoding())
    }

    fn decode_mqtt_payload(payload: &[u8]) -> anyhow::Result<Self> {
        let value = Self::decode_json(payload)?;
        match value {
            Value::Map(mut map) => match map.remove("value") {
                Some(value) => Ok(value),
                None => Ok(Value::Map(map)),
            },
            value => Ok(value),
        }
    }

    fn encode_stdout(&self) -> anyhow::Result<String> {
        Ok(self.dsrv_source())
    }
}

impl DeferrableStreamData for Value {
    fn is_deferred(&self) -> bool {
        matches!(self, Value::Deferred)
    }
    fn deferred_value() -> Self {
        Value::Deferred
    }
    fn no_val_value() -> Self {
        Value::NoVal
    }
}

// Deliberately no ToRedisArgs: encoding can fail (for example, NoVal).
// Redis writers must call the fallible JsonStreamValue::encode_json first.
#[cfg(feature = "redis")]
impl FromRedisValue for Value {
    fn from_redis_value(v: redis::Value) -> Result<Self, redis::ParsingError> {
        match v {
            redis::Value::BulkString(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|e| {
                    redis::ParsingError::from(format!("Invalid UTF-8 in BulkString: {:?}", e))
                })?;

                json5::from_str(s).map_err(|e| {
                    redis::ParsingError::from(format!(
                        "BulkString not deserializable to Value as JSON5: {}",
                        e
                    ))
                })
            }
            redis::Value::Array(values) => {
                let list: Result<Vec<Value>, _> =
                    values.iter().map(Value::from_redis_value_ref).collect();
                Ok(Value::List(list?.into()))
            }
            redis::Value::Nil => Ok(Value::Unit),
            redis::Value::Int(i) => Ok(Value::Int(i)),
            redis::Value::SimpleString(s) => Ok(Value::Str(s.clone().into())),
            _ => Err(redis::ParsingError::from(std::format!(
                "Unsupported Redis value type for Value deserialization: {:?}",
                v
            ))),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for f64 {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(x) => Ok(x),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for String {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(i) => Ok(i.to_string()),
            Value::Function(i) => Ok(i.display_source().to_string()),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for bool {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for EcoVec<Value> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(i) | Value::Tuple(i) => Ok(i),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for () {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Unit => Ok(()),
            _ => Err(()),
        }
    }
}
impl TryFrom<JValue> for Value {
    type Error = anyhow::Error;

    fn try_from(value: JValue) -> Result<Self, Self::Error> {
        match value {
            JValue::Null => Ok(Value::Unit),
            JValue::Bool(val) => Ok(Value::Bool(val)),
            JValue::Number(num) => {
                if num.is_i64() {
                    Ok(Value::Int(num.as_i64().unwrap()))
                } else if num.is_u64() {
                    Err(anyhow!("u64 too large for Value::Int"))
                } else {
                    // Guaranteed to be f64 at this point
                    Ok(Value::Float(num.as_f64().unwrap()))
                }
            }
            JValue::String(val) if val == "⊥" => Ok(Value::Deferred),
            JValue::String(val) => Ok(Value::Str(val.into())),
            // If any element returns Err then this propagates it (because of collect)
            JValue::Array(vals) => vals
                .iter()
                .map(|v| v.clone().try_into())
                .collect::<Result<EcoVec<Value>, Self::Error>>()
                .map(Value::List),
            JValue::Object(mut vals) if vals.contains_key("$tag") => {
                let tag = vals.remove("$tag").expect("union object contains $tag");
                let JValue::String(tag) = tag else {
                    return Err(anyhow!("union $tag must be a string"));
                };
                let payload = vals.remove("payload").map(Value::try_from).transpose()?;
                if !vals.is_empty() {
                    return Err(anyhow!("union object contains extra fields"));
                }
                Ok(UnionValue::new(tag, payload).into())
            }
            JValue::Object(vals) => {
                // Convert JValue::Object to Value::Map
                let btree = vals
                    .iter()
                    .map(|(k, v)| {
                        let x = v.clone().try_into()?;
                        Ok((k.clone().into(), x))
                    })
                    .collect::<Result<BTreeMap<EcoString, Value>, Self::Error>>()?;
                Ok(Value::Map(btree))
            }
        }
    }
}
impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Str(value.into())
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Str(value.into())
    }
}
impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl From<EcoVec<Value>> for Value {
    fn from(value: EcoVec<Value>) -> Self {
        Value::List(value)
    }
}
impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Value::List(value.into()) // Into = from Vec -> EcoVec
    }
}
impl From<BTreeMap<EcoString, Value>> for Value {
    fn from(value: BTreeMap<EcoString, Value>) -> Self {
        Value::Map(value)
    }
}
impl From<()> for Value {
    fn from(_value: ()) -> Self {
        Value::Unit
    }
}

impl Value {
    /// This value written as the DSRV expression that builds it, which is
    /// what a monitor writes to stdout: a reader can paste a reported value
    /// back into a specification.
    ///
    /// A constructor is written bare, without naming its union, because the
    /// value carries no schema and none is needed: the type expected where
    /// the value is used resolves the tag (`features.md` §12).
    ///
    /// Two values have no source form, because they are states a running
    /// monitor is in rather than things a specification can say: `NoVal` and
    /// `Deferred`. They keep their diagnostic marks, as do a non-finite
    /// `Float` and a function with no recorded source.
    pub fn dsrv_source(&self) -> String {
        let mut source = String::new();
        self.write_dsrv_source(&mut source)
            .expect("writing to a String does not fail");
        source
    }

    fn write_dsrv_source(&self, out: &mut impl std::fmt::Write) -> std::fmt::Result {
        match self {
            Value::Int(value) => write!(out, "{value}"),
            Value::Float(value) if value.is_finite() && value.fract() == 0.0 => {
                write!(out, "{value:.1}")
            }
            Value::Float(value) => write!(out, "{value}"),
            Value::Bool(value) => write!(out, "{value}"),
            Value::Unit => write!(out, "()"),
            Value::Str(value) => write_dsrv_string(out, value),
            Value::Function(function) => write!(out, "{}", function.display_source()),
            Value::Union(value) => {
                write!(out, "{}", value.tag())?;
                match value.payload() {
                    Some(payload) => {
                        out.write_char('(')?;
                        payload.write_dsrv_source(out)?;
                        out.write_char(')')
                    }
                    None => Ok(()),
                }
            }
            Value::List(values) => {
                out.write_char('[')?;
                write_dsrv_sequence(out, values)?;
                out.write_char(']')
            }
            Value::Tuple(values) => {
                out.write_str("Tuple(")?;
                write_dsrv_sequence(out, values)?;
                out.write_char(')')
            }
            Value::Map(entries) => {
                out.write_str("Map(")?;
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index != 0 {
                        out.write_str(", ")?;
                    }
                    write_dsrv_string(out, key)?;
                    out.write_str(": ")?;
                    value.write_dsrv_source(out)?;
                }
                out.write_char(')')
            }
            Value::Deferred => out.write_char('⊥'),
            Value::NoVal => out.write_str("no_val"),
        }
    }
}

fn write_dsrv_sequence(out: &mut impl std::fmt::Write, values: &[Value]) -> std::fmt::Result {
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            out.write_str(", ")?;
        }
        value.write_dsrv_source(out)?;
    }
    Ok(())
}

/// A string literal, escaped the way the grammar spells escapes. A character
/// DSRV has no escape for is written as it is, which is what the string
/// literal rule accepts for everything but a quote, a backslash and a line
/// break.
fn write_dsrv_string(out: &mut impl std::fmt::Write, value: &str) -> std::fmt::Result {
    out.write_char('"')?;
    for character in value.chars() {
        match character {
            '"' => out.write_str("\\\"")?,
            '\\' => out.write_str("\\\\")?,
            '\n' => out.write_str("\\n")?,
            '\t' => out.write_str("\\t")?,
            other => out.write_char(other)?,
        }
    }
    out.write_char('"')
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) if fl.is_finite() && fl.fract() == 0.0 => write!(f, "{fl:.1}"),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Str(s) => write!(f, "{:?}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Function(function) => write!(f, "{}", function.display_source()),
            Value::List(vals) => {
                let vals = vals
                    .iter()
                    .map(|val| format!("{}", val))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{}]", vals)
            }
            Value::Tuple(vals) => {
                let vals = vals
                    .iter()
                    .map(|val| format!("{}", val))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Tuple({})", vals)
            }
            Value::Map(map) => {
                let entries = map
                    .iter()
                    .map(|(key, val)| format!("{:?}: {}", key, val))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Map({})", entries)
            }
            Value::Deferred => write!(f, "⊥"),
            // Diagnostic only: a schema-free value cannot name a constructor.
            Value::Union(value) => {
                write!(f, "Union({:?}", value.tag())?;
                if let Some(payload) = value.payload() {
                    write!(f, ", {payload}")?;
                }
                write!(f, ")")
            }
            Value::NoVal => write!(f, "no_val"),
            Value::Unit => write!(f, "()"),
        }
    }
}

/* Trait for the values being sent along streams. This could be just Value for
 * untimed heterogeneous streams, more specific types for homogeneous (typed)
 * streams, or time-stamped values for timed streams. This traits allows
 * for the implementation of runtimes to be agnostic of the types of stream
 * values used. */
pub trait StreamData: Clone + Debug + 'static {
    fn is_no_val(&self) -> bool {
        false
    }
}

/* Trait for stream data with a statically known stream type */
pub trait TypedStreamData: StreamData {
    fn stream_data_type() -> StreamType;
}

/* Trait for stream data types that can represent deferred values as a placeholder
* for when an expression cannot be computed due to a lack of context */
pub trait DeferrableStreamData: StreamData {
    fn is_deferred(&self) -> bool;
    fn deferred_value() -> Self;
    fn no_val_value() -> Self;
}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl StreamData for i32 {}
impl StreamData for u64 {}
impl StreamData for f64 {}
impl StreamData for String {}
impl StreamData for bool {}
impl StreamData for () {}
impl StreamData for EcoVec<Value> {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum StreamType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
    List(Box<StreamType>),
    Tuple(EcoVec<StreamType>),
    Map(Box<StreamType>),
    Expr(Box<StreamType>),
    Struct(EcoVec<(EcoString, StreamType)>, bool), // ordered typed fields, true allows extra fields
    Function(EcoVec<StreamType>, Box<StreamType>),
    /// Gradual/dynamic stream type. Values are represented as `Value` and checked at runtime when
    /// cast to a stricter type.
    Any,
    Union(ClosedUnion<StreamType>),
}

impl Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Int => write!(f, "Int"),
            StreamType::Float => write!(f, "Float"),
            StreamType::Str => write!(f, "Str"),
            StreamType::Bool => write!(f, "Bool"),
            StreamType::Unit => write!(f, "Unit"),
            StreamType::List(inner) => write!(f, "List<{}>", inner),
            StreamType::Tuple(inner) => {
                let len = inner.len();
                let inner = inner
                    .iter()
                    .map(|typ| format!("{}", typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                if len == 1 {
                    write!(f, "({},)", inner)
                } else {
                    write!(f, "({})", inner)
                }
            }
            StreamType::Map(inner) => write!(f, "Map<{}>", inner),
            StreamType::Expr(inner) => write!(f, "Expr<{}>", inner),
            StreamType::Struct(inner, allow_extra) => {
                let mut fields = inner
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<_>>();
                if *allow_extra {
                    fields.push("...".into());
                }
                write!(f, "Struct<{}>", fields.join(", "))
            }
            StreamType::Function(args, ret) => {
                let args = args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "({} -> {})", args, ret)
            }
            StreamType::Any => write!(f, "Any"),
            StreamType::Union(schema) => write!(f, "{schema}"),
        }
    }
}

impl TypedStreamData for i64 {
    fn stream_data_type() -> StreamType {
        StreamType::Int
    }
}

impl TypedStreamData for u64 {
    fn stream_data_type() -> StreamType {
        StreamType::Int
    }
}

impl TypedStreamData for f64 {
    fn stream_data_type() -> StreamType {
        StreamType::Float
    }
}

impl TypedStreamData for String {
    fn stream_data_type() -> StreamType {
        StreamType::Str
    }
}

impl TypedStreamData for bool {
    fn stream_data_type() -> StreamType {
        StreamType::Bool
    }
}

impl TypedStreamData for () {
    fn stream_data_type() -> StreamType {
        StreamType::Unit
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum StreamTypeAscription {
    Ascribed(StreamType),
    Unascribed,
}

impl StreamTypeAscription {
    /// A function parameter as runtimes display it: `x: Int`, or `x` when its
    /// type was left to inference.
    pub fn parameter_display(&self, name: &crate::VarName) -> String {
        match self {
            Self::Ascribed(typ) => format!("{name}: {typ}"),
            Self::Unascribed => name.to_string(),
        }
    }
}

impl Serialize for Value {
    // Certain edge cases were not covered by derived Serialize, such as serializing List
    // symmetrically, hence manual impl
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            // Should never need to serialize a NoVal, since it indicates no value received
            Value::NoVal => Err(serde::ser::Error::custom("Cannot serialize Value::NoVal")),

            Value::Unit => serializer.serialize_none(),

            Value::Deferred => serializer.serialize_str("⊥"),

            Value::Bool(b) => serializer.serialize_bool(*b),

            Value::Int(i) => serializer.serialize_i64(*i),

            Value::Float(f) => serializer.serialize_f64(*f),

            Value::Str(s) => serializer.serialize_str(s),

            Value::Function(function) => serializer.serialize_str(function.display_source()),

            Value::List(vals) | Value::Tuple(vals) => {
                let mut seq = serializer.serialize_seq(Some(vals.len()))?;
                for v in vals.iter() {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Value::Map(map) => {
                if map.contains_key("$tag") {
                    return Err(serde::ser::Error::custom(
                        "Value::Map contains reserved union key $tag",
                    ));
                }
                let mut m = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map.iter() {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }

            Value::Union(value) => {
                let mut map = serializer.serialize_map(Some(if value.payload().is_some() {
                    2
                } else {
                    1
                }))?;
                map.serialize_entry("$tag", value.tag())?;
                if let Some(payload) = value.payload() {
                    map.serialize_entry("payload", payload)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    // Certain edge cases were not covered by derived Serialize, such as handling Deferred
    // symmetrically, hence manual impl
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "any valid JSON value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
                Ok(Value::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
                Ok(Value::Int(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Value, E>
            where
                E: de::Error,
            {
                // clamp or reject: here we reject if > i64::MAX
                if v <= i64::MAX as u64 {
                    Ok(Value::Int(v as i64))
                } else {
                    Err(E::custom("u64 too large for Value::Int"))
                }
            }

            fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
                Ok(Value::Float(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Value, E> {
                if v == "⊥" {
                    Ok(Value::Deferred)
                } else {
                    Ok(Value::Str(v.into()))
                }
            }

            fn visit_string<E>(self, v: String) -> Result<Value, E> {
                if v == "⊥" {
                    Ok(Value::Deferred)
                } else {
                    Ok(Value::Str(v.into()))
                }
            }

            fn visit_none<E>(self) -> Result<Value, E> {
                Ok(Value::Unit)
            }

            fn visit_unit<E>(self) -> Result<Value, E> {
                Ok(Value::Unit)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vals = EcoVec::new();
                while let Some(elem) = seq.next_element()? {
                    vals.push(elem);
                }
                Ok(Value::List(vals))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut out = BTreeMap::new();
                let mut duplicate = false;
                let mut tag = None;
                while let Some(key) = map.next_key::<EcoString>()? {
                    if key == "$tag" {
                        if tag.is_some() {
                            duplicate = true;
                        }
                        tag = Some(map.next_value::<EcoString>()?);
                    } else {
                        let value = map.next_value::<Value>()?;
                        if out.insert(key, value).is_some() {
                            duplicate = true;
                        }
                    }
                }
                if let Some(tag) = tag {
                    if duplicate {
                        return Err(de::Error::custom("duplicate member in union object"));
                    }
                    let payload = out.remove("payload");
                    if !out.is_empty() {
                        return Err(de::Error::custom("union object contains extra fields"));
                    }
                    return Ok(UnionValue::new(tag, payload).into());
                }
                Ok(Value::Map(out))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod dsrv_source_tests {
    use super::*;
    use crate::core::UnionValue;
    use crate::lang::dsrv::parser::parse_str;
    use ecow::eco_vec;

    /// A reported value, read back as the expression of a stream. The header
    /// is the one the specification it came from must already carry for a
    /// union value to have been built at all.
    fn parse_reported(source: &str) -> String {
        let specification =
            format!("use experimental::{{tagged_unions}}\nout reported\nreported = {source}\n");
        let parsed = parse_str(&specification)
            .unwrap_or_else(|error| panic!("`{source}` should parse as DSRV: {error}"));
        parsed
            .var_expr_ref(&crate::VarName::from("reported"))
            .expect("reported is defined")
            .to_string()
    }

    fn round_trips(value: Value) {
        let source = value.dsrv_source();
        assert_eq!(
            parse_reported(&source),
            source,
            "`{source}` did not print back as itself"
        );
    }

    #[test]
    fn reported_values_are_dsrv_source() {
        for value in [
            Value::Int(-3),
            Value::Float(1.0),
            Value::Float(1.5),
            Value::Bool(true),
            Value::Unit,
            Value::Str("plain".into()),
            Value::List(eco_vec![Value::Int(1), Value::Int(2)]),
            Value::Tuple(eco_vec![Value::Int(1), Value::Bool(false)]),
            Value::from(UnionValue::new("Stopped", None)),
            Value::from(UnionValue::new("Moving", Some(Value::Int(3)))),
            // A tag whose payload is itself a union, as a library writes it.
            Value::from(UnionValue::new(
                "Moved",
                Some(Value::from(UnionValue::new("Idle", None))),
            )),
        ] {
            round_trips(value);
        }
    }

    // A reported string is escaped the way the grammar spells escapes, so it
    // parses. It does not yet read back as the same string: the grammar
    // keeps an escape as the characters that spell it rather than decoding
    // it, so `\n` is a backslash and an `n`. Decoding is a separate fix;
    // until it lands, a string carrying one of these characters is reported
    // in a form that parses but means something else.
    #[test]
    fn a_reported_string_escapes_what_dsrv_escapes() {
        let value = Value::Str("a\"b\\c\nd\te".into());
        assert_eq!(value.dsrv_source(), r#""a\"b\\c\nd\te""#);
        parse_reported(&value.dsrv_source());
    }

    #[test]
    fn a_reported_map_names_its_keys_as_strings() {
        let mut entries = BTreeMap::new();
        entries.insert(EcoString::from("count"), Value::Int(2));
        entries.insert(EcoString::from("on"), Value::Bool(true));
        let value = Value::Map(entries);
        assert_eq!(value.dsrv_source(), r#"Map("count": 2, "on": true)"#);
        round_trips(value);
    }

    // The states a running monitor can be in are not things a specification
    // can say, so they keep their marks rather than pretending to be source.
    #[test]
    fn runtime_states_keep_their_marks() {
        assert_eq!(Value::Deferred.dsrv_source(), "⊥");
        assert_eq!(Value::NoVal.dsrv_source(), "no_val");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use json5::from_str;
    use serde_json::{json, to_string, to_value};

    #[test]
    fn stream_decoder_accepts_strict_json_and_json5() {
        assert_eq!(
            Value::decode_json(br#"{"mode":"safe"}"#).unwrap(),
            Value::Map(BTreeMap::from([("mode".into(), Value::Str("safe".into()))]))
        );
        assert_eq!(
            Value::decode_json(br#"{/* state */ mode: "safe", enabled: true,}"#).unwrap(),
            Value::Map(BTreeMap::from([
                ("enabled".into(), Value::Bool(true)),
                ("mode".into(), Value::Str("safe".into())),
            ])),
        );
    }

    #[test]
    fn stream_decoder_reports_json5_and_utf8_errors() {
        assert!(
            Value::decode_json(br#"{mode:}"#)
                .unwrap_err()
                .to_string()
                .contains("JSON5")
        );
        assert!(
            Value::decode_json(&[0xff])
                .unwrap_err()
                .to_string()
                .contains("UTF-8")
        );
    }

    #[test]
    fn stream_encoder_keeps_finite_values_strict_and_preserves_non_finite_values() {
        let finite = Value::Map(BTreeMap::from([("value".into(), Value::Float(1.5))]));
        let encoded = finite.encode_json().unwrap();
        assert_eq!(encoded, r#"{"value":1.5}"#);
        assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_ok());
        assert_eq!(Value::decode_json(encoded.as_bytes()).unwrap(), finite);

        let infinite = Value::Float(f64::INFINITY);
        let encoded = infinite.encode_json().unwrap();
        assert!(encoded.contains("Infinity"));
        assert_eq!(Value::decode_json(encoded.as_bytes()).unwrap(), infinite);

        let aggregate = Value::Map(BTreeMap::from([(
            "values".into(),
            Value::List(vec![Value::Float(f64::NEG_INFINITY), Value::Float(f64::NAN)].into()),
        )]));
        let encoded = aggregate.encode_json().unwrap();
        let decoded = Value::decode_json(encoded.as_bytes()).unwrap();
        let Value::Map(decoded) = decoded else {
            panic!("expected a decoded map");
        };
        let Value::List(values) = &decoded["values"] else {
            panic!("expected a decoded list");
        };
        assert_eq!(values[0], Value::Float(f64::NEG_INFINITY));
        assert!(matches!(values[1], Value::Float(value) if value.is_nan()));
    }

    #[test]
    fn test_json_try_into_null() {
        let jv = json!(null);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Unit);
    }

    #[test]
    fn test_json_try_into_bool() {
        let jv = json!(true);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn test_json_try_into_int() {
        let jv = json!(42);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn test_json_try_into_deferred_matches_deserialization() {
        let v: Value = json!("⊥").try_into().unwrap();
        assert_eq!(v, Value::Deferred);
        assert_eq!(serde_json::from_str::<Value>("\"⊥\"").unwrap(), v);
    }

    #[test]
    fn test_json_try_into_float() {
        let jv = json!(3.14);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Float(3.14));
    }

    #[test]
    fn test_json_try_into_string() {
        let jv = json!("hello");
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Str("hello".into()));
    }

    #[test]
    fn test_json_try_into_array() {
        let jv = json!([1, 2, 3]);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(
            v,
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into())
        );
    }

    #[test]
    fn test_json_try_into_object() {
        let jv = json!({
            "x": 42,
            "y": true,
            "z": "hello"
        });
        let v: Value = jv.try_into().unwrap();

        let mut expected = BTreeMap::new();
        expected.insert("x".into(), Value::Int(42));
        expected.insert("y".into(), Value::Bool(true));
        expected.insert("z".into(), Value::Str("hello".into()));

        assert_eq!(v, Value::Map(expected));
    }

    #[test]
    fn test_json_try_into_nested() {
        let jv = json!({
            "nums": [1, 2, 3],
            "nested": { "a": false }
        });
        let v: Value = jv.try_into().unwrap();

        let mut nested = BTreeMap::new();
        nested.insert("a".into(), Value::Bool(false));

        let mut expected = BTreeMap::new();
        expected.insert(
            "nums".into(),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into()),
        );
        expected.insert("nested".into(), Value::Map(nested));

        assert_eq!(v, Value::Map(expected));
    }

    #[test]
    fn test_json_try_into_too_large_number() {
        let jv = serde_json::Value::Number(serde_json::Number::from(u64::MAX));
        let result: Result<Value, _> = jv.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_try_into_empty_string() {
        let jv = json!("");
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Str("".into()));
    }

    #[test]
    fn test_json_try_into_unicode_string() {
        let jv = json!("こんにちは🌏");
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Str("こんにちは🌏".into()));
    }

    #[test]
    fn test_json_try_into_large_int_bounds() {
        let jv = json!(i64::MAX);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Int(i64::MAX));

        let jv = serde_json::json!(i64::MIN);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Int(i64::MIN));
    }

    #[test]
    fn test_json_try_into_empty_array() {
        let jv = json!([]);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::List(vec![].into()));
    }

    #[test]
    fn test_json_try_into_mixed_array() {
        let jv = json!([1, "two", false]);
        let v: Value = jv.try_into().unwrap();
        assert_eq!(
            v,
            Value::List(vec![Value::Int(1), Value::Str("two".into()), Value::Bool(false)].into())
        );
    }

    #[test]
    fn test_json_try_into_empty_object() {
        let jv = json!({});
        let v: Value = jv.try_into().unwrap();
        assert_eq!(v, Value::Map(BTreeMap::new()));
    }

    #[test]
    fn test_json_try_into_nested_empty_object() {
        let jv = json!({ "nested": {} });
        let v: Value = jv.try_into().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("nested".into(), Value::Map(BTreeMap::new()));
        assert_eq!(v, Value::Map(expected));
    }

    #[test]
    fn test_json_try_into_object_case_sensitive_keys() {
        let jv = json!({ "Key": 1, "key": 2 });
        let v: Value = jv.try_into().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("Key".into(), Value::Int(1));
        expected.insert("key".into(), Value::Int(2));
        assert_eq!(v, Value::Map(expected));
    }

    #[test]
    fn test_json_try_into_deeply_nested_object() {
        let jv = json!({
            "a": { "b": { "c": { "d": 1 } } }
        });
        let v: Value = jv.try_into().unwrap();

        let mut dmap = BTreeMap::new();
        dmap.insert("d".into(), Value::Int(1));

        let mut cmap = BTreeMap::new();
        cmap.insert("c".into(), Value::Map(dmap));

        let mut bmap = BTreeMap::new();
        bmap.insert("b".into(), Value::Map(cmap));

        let mut amap = BTreeMap::new();
        amap.insert("a".into(), Value::Map(bmap));

        assert_eq!(v, Value::Map(amap));
    }

    #[test]
    fn test_json_try_into_round_trip() {
        let original = Value::List(
            vec![
                Value::Int(1),
                Value::Str("abc".into()),
                Value::Map({
                    let mut m = BTreeMap::new();
                    m.insert("k".into(), Value::Bool(true));
                    m
                }),
            ]
            .into(),
        );

        // Serialize to JSON
        let j = to_value(&original).unwrap();
        dbg!(&j);
        // Deserialize back to Value
        let back: Value = j.try_into().unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn test_json_serialize_unit() {
        let v = Value::Unit;
        let json = to_string(&v).unwrap();
        assert_eq!(json, "null");
    }

    #[test]
    fn test_json_serialize_deferred() {
        let v = Value::Deferred;
        let json = to_string(&v).unwrap();
        assert_eq!(json, "\"⊥\"");
    }

    #[test]
    fn test_json_serialize_bool() {
        let v = Value::Bool(true);
        let json = to_string(&v).unwrap();
        assert_eq!(json, "true");
    }

    #[test]
    fn test_json_serialize_int() {
        let v = Value::Int(123);
        let json = to_string(&v).unwrap();
        assert_eq!(json, "123");
    }

    #[test]
    fn test_json_serialize_float() {
        let v = Value::Float(3.14);
        let json = to_string(&v).unwrap();
        assert_eq!(json, "3.14");
    }

    #[test]
    fn test_json_serialize_string() {
        let v = Value::Str("hello".into());
        let json = to_string(&v).unwrap();
        assert_eq!(json, "\"hello\"");
    }

    #[test]
    fn test_json_serialize_function_as_source_string() {
        let v = Value::Function(RuntimeFunction::opaque("\\x: Int -> x + 1"));
        let json = to_string(&v).unwrap();
        assert_eq!(json, "\"\\\\x: Int -> x + 1\"");
    }

    #[test]
    fn test_display_function_as_source_string() {
        let v = Value::Function(RuntimeFunction::opaque("\\x: Int -> x + 1"));
        assert_eq!(format!("{}", v), "\\x: Int -> x + 1");
    }

    #[test]
    fn test_runtime_function_can_carry_callable() {
        let function = RuntimeFunction::native("identity", |args| {
            Ok(Box::pin(futures::stream::iter(args.into_iter())))
        });
        assert!(function.is_callable());

        let mut stream = function.call(vec![Value::Int(7)].into()).unwrap();
        let value = futures::executor::block_on(stream.next()).unwrap();
        assert_eq!(value, Value::Int(7));

        let v = Value::Function(function);
        let json = to_string(&v).unwrap();
        assert_eq!(json, "\"identity\"");
    }

    #[test]
    fn test_json_serialize_list() {
        let v = Value::List(vec![Value::Int(1), Value::Bool(false)].into());
        let json = to_string(&v).unwrap();
        assert_eq!(json, "[1,false]");
    }

    #[test]
    fn test_json_serialize_map() {
        let mut m = BTreeMap::new();
        m.insert("x".into(), Value::Int(42));
        m.insert("y".into(), Value::Bool(true));
        let v = Value::Map(m);

        let json = to_string(&v).unwrap();
        // Because BTreeMap orders keys, we know the order in the JSON string.
        assert_eq!(json, "{\"x\":42,\"y\":true}");
    }

    #[test]
    fn test_json_round_trip_simple() {
        let v = Value::List(vec![Value::Str("abc".into()), Value::Int(5)].into());
        let json = to_string(&v).unwrap();
        let back: Value = from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn test_json_round_trip_nested_map() {
        let mut inner = BTreeMap::new();
        inner.insert("a".into(), Value::Float(1.5));

        let mut outer = BTreeMap::new();
        outer.insert("inner".into(), Value::Map(inner));

        let v = Value::Map(outer);
        let json = to_string(&v).unwrap();
        let back: Value = from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn test_json_empty_array_and_map() {
        let v_arr = Value::List(vec![].into());
        let v_map = Value::Map(BTreeMap::new());

        let json_arr = to_string(&v_arr).unwrap();
        let json_map = to_string(&v_map).unwrap();

        assert_eq!(json_arr, "[]");
        assert_eq!(json_map, "{}");

        let back_arr: Value = from_str(&json_arr).unwrap();
        let back_map: Value = from_str(&json_map).unwrap();

        assert_eq!(v_arr, back_arr);
        assert_eq!(v_map, back_map);
    }

    #[test]
    fn test_json_null_maps_to_unit() {
        let json = "null";
        let v: Value = from_str(json).unwrap();
        assert_eq!(v, Value::Unit);
    }

    #[test]
    fn test_json_bot_maps_to_deferred() {
        let v = "\"⊥\"";
        let json: Value = from_str(&v).unwrap();
        assert_eq!(json, Value::Deferred);
    }

    #[test]
    fn test_format_expression_type() {
        assert_eq!(
            StreamType::Expr(Box::new(StreamType::Bool)).to_string(),
            "Expr<Bool>"
        );
        assert_eq!(
            StreamType::List(Box::new(StreamType::Expr(Box::new(StreamType::Int)))).to_string(),
            "List<Expr<Int>>"
        );
    }

    #[test]
    fn test_format_struct_type() {
        let typ = StreamType::Struct(
            vec![
                ("name".into(), StreamType::Str),
                ("count".into(), StreamType::Int),
            ]
            .into(),
            false,
        );
        assert_eq!(typ.to_string(), "Struct<name: Str, count: Int>");

        let typ = StreamType::Struct(vec![("name".into(), StreamType::Str)].into(), true);
        assert_eq!(typ.to_string(), "Struct<name: Str, ...>");
    }

    #[test]
    fn partial_stream_value_implements_marker_contract_for_generic_payloads() {
        #[derive(Clone, Debug)]
        struct Payload;

        assert!(!PartialStreamValue::Known(Payload).is_no_val());
        assert!(PartialStreamValue::<Payload>::NoVal.is_no_val());
        assert!(!PartialStreamValue::<Payload>::Deferred.is_no_val());

        assert!(!PartialStreamValue::Known(Payload).is_deferred());
        assert!(!PartialStreamValue::<Payload>::NoVal.is_deferred());
        assert!(PartialStreamValue::<Payload>::Deferred.is_deferred());
        assert!(PartialStreamValue::<Payload>::no_val_value().is_no_val());
        assert!(PartialStreamValue::<Payload>::deferred_value().is_deferred());
    }
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum PartialStreamValue<T> {
    Known(T),
    NoVal,
    Deferred,
}

impl<T: Display> Display for PartialStreamValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartialStreamValue::Known(val) => write!(f, "{}", val),
            PartialStreamValue::NoVal => write!(f, "no_val"),
            PartialStreamValue::Deferred => write!(f, "⊥"),
        }
    }
}

impl<T: Clone + Debug + 'static> StreamData for PartialStreamValue<T> {
    fn is_no_val(&self) -> bool {
        matches!(self, PartialStreamValue::NoVal)
    }
}

impl<T: Clone + Debug + 'static> DeferrableStreamData for PartialStreamValue<T> {
    fn is_deferred(&self) -> bool {
        matches!(self, PartialStreamValue::Deferred)
    }

    fn deferred_value() -> Self {
        PartialStreamValue::Deferred
    }

    fn no_val_value() -> Self {
        PartialStreamValue::NoVal
    }
}

impl TryFrom<Value> for PartialStreamValue<i64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<String> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(x) => Ok(PartialStreamValue::Known(x.into())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<f64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<bool> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<()> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Unit => Ok(PartialStreamValue::Known(())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl TryFrom<Value> for PartialStreamValue<EcoVec<Value>> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl From<PartialStreamValue<i64>> for Value {
    fn from(value: PartialStreamValue<i64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Int(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<f64>> for Value {
    fn from(value: PartialStreamValue<f64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Float(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<String>> for Value {
    fn from(value: PartialStreamValue<String>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Str(v.into()),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<bool>> for Value {
    fn from(value: PartialStreamValue<bool>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Bool(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<()>> for Value {
    fn from(value: PartialStreamValue<()>) -> Self {
        match value {
            PartialStreamValue::Known(()) => Value::Unit,
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<EcoVec<Value>>> for Value {
    fn from(value: PartialStreamValue<EcoVec<Value>>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::List(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<Value>> for Value {
    fn from(value: PartialStreamValue<Value>) -> Self {
        match value {
            PartialStreamValue::Known(v) => v,
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
