use std::fmt::{Debug, Display};

use anyhow::anyhow;
use ecow::{EcoString, EcoVec, eco_vec};
use redis::{FromRedisValue, RedisResult, ToRedisArgs};
use serde::{Deserialize, Serialize};
use serde_json::Value as JValue;

// Anything inside a stream should be clonable in O(1) time in order for the
// runtimes to be efficiently implemented. This is why we use EcoString and
// EcoVec instead of String and Vec. These types are essentially references
// which allow mutation in place if there is only one reference to the data or
// copy-on-write if there is more than one reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(EcoString),
    Bool(bool),
    List(EcoVec<Value>),
    Unknown,
    Unit,
}
impl StreamData for Value {}

impl ToRedisArgs for Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match serde_json5::to_string(self) {
            Ok(json_str) => json_str.write_redis_args(out),
            Err(_) => "null".write_redis_args(out),
        }
    }
}

impl FromRedisValue for Value {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        match v {
            redis::Value::BulkString(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| {
                    redis::RedisError::from((redis::ErrorKind::TypeError, "Invalid UTF-8"))
                })?;

                serde_json5::from_str(s).map_err(|_e| {
                    redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Response type not deserializable to Value with serde_json5",
                        format!(
                            "(response was {:?})",
                            redis::Value::BulkString(bytes.clone())
                        ),
                    ))
                })
            }
            redis::Value::Array(values) => {
                let list: Result<Vec<Value>, _> =
                    values.iter().map(Value::from_redis_value).collect();
                Ok(Value::List(list?.into()))
            }
            redis::Value::Nil => Ok(Value::Unit),
            redis::Value::Int(i) => Ok(Value::Int(*i)),
            redis::Value::SimpleString(s) => Ok(Value::Str(s.clone().into())),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Response type not deserializable to Value",
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
                    Err(anyhow!("Number too large"))
                } else {
                    // Guaranteed to be f64 at this point
                    Ok(Value::Float(num.as_f64().unwrap()))
                }
            }
            JValue::String(val) => Ok(Value::Str(val.into())),
            // If any element returns Err then this propagates it (because of collect)
            JValue::Array(vals) => vals
                .iter()
                .map(|v| v.clone().try_into())
                .collect::<Result<EcoVec<Value>, Self::Error>>()
                .map(Value::List),
            // Objects currently represented of list of key-value pairs. Since we don't have pairs
            // it becomes Lists of 2-value Lists
            JValue::Object(vals) => vals
                .iter()
                .map(|(k, v)| {
                    let x: Result<Value, Self::Error> = v.clone().try_into();
                    match x {
                        Ok(x) => Ok(Value::List(eco_vec![k.clone().into(), x])),
                        Err(e) => Err(e),
                    }
                })
                .collect::<Result<EcoVec<Value>, Self::Error>>()
                .map(Value::List),
            // Ok(Value::List(
            //     vals.iter()
            //         .map(|(k, v)| Value::List(eco_vec![k.clone().into(), v.clone().to_value()]))
            //         .collect(),
            // )),
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
impl From<()> for Value {
    fn from(_value: ()) -> Self {
        Value::Unit
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Str(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::List(vals) => {
                write!(f, "[")?;
                for val in vals.iter() {
                    write!(f, "{}, ", val)?;
                }
                write!(f, "]")
            }
            Value::Unknown => write!(f, "unknown"),
            Value::Unit => write!(f, "()"),
        }
    }
}

/* Trait for the values being sent along streams. This could be just Value for
 * untimed heterogeneous streams, more specific types for homogeneous (typed)
 * streams, or time-stamped values for timed streams. This traits allows
 * for the implementation of runtimes to be agnostic of the types of stream
 * values used. */
pub trait StreamData: Clone + Debug + 'static {}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl StreamData for i32 {}
impl StreamData for u64 {}
impl StreamData for f64 {}
impl StreamData for String {}
impl StreamData for bool {}
impl StreamData for () {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
}
