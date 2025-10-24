use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
};

use anyhow::anyhow;
use ecow::{EcoString, EcoVec};
use redis::{FromRedisValue, RedisResult, ToRedisArgs};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value as JValue;
use std::fmt;

// Anything inside a stream should be clonable in O(1) time in order for the
// runtimes to be efficiently implemented. This is why we use EcoString and
// EcoVec instead of String and Vec. These types are essentially references
// which allow mutation in place if there is only one reference to the data or
// copy-on-write if there is more than one reference.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(EcoString),
    Bool(bool),
    List(EcoVec<Value>),
    Map(BTreeMap<EcoString, Value>),
    Unit,     // Indicates the absence of a value
    Deferred, // Indicates a value that cannot yet be computed due to lack of history
    NoVal,    // Indicates no value for the current stream step (due to async stream inputs)
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
impl TryFrom<Value> for EcoVec<Value> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(i) => Ok(i),
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
            JValue::String(val) => Ok(Value::Str(val.into())),
            // If any element returns Err then this propagates it (because of collect)
            JValue::Array(vals) => vals
                .iter()
                .map(|v| v.clone().try_into())
                .collect::<Result<EcoVec<Value>, Self::Error>>()
                .map(Value::List),
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
            Value::Map(map) => {
                write!(f, "{{")?;
                for (key, val) in map.iter() {
                    write!(f, "{}: {}, ", key, val)?;
                }
                write!(f, "}}")
            }
            Value::Deferred => write!(f, "⊥"),
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
pub trait StreamData: Clone + Debug + 'static {}

// Trait defining the allowed types for expression values
impl StreamData for i64 {}
impl StreamData for i32 {}
impl StreamData for u64 {}
impl StreamData for f64 {}
impl StreamData for String {}
impl StreamData for bool {}
impl StreamData for () {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum StreamType {
    Int,
    Float,
    Str,
    Bool,
    Unit,
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

            Value::List(vals) => {
                let mut seq = serializer.serialize_seq(Some(vals.len()))?;
                for v in vals.iter() {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Value::Map(map) => {
                let mut m = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map.iter() {
                    m.serialize_entry(k, v)?;
                }
                m.end()
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
                while let Some((k, v)) = map.next_entry::<String, Value>()? {
                    out.insert(k.into(), v);
                }
                Ok(Value::Map(out))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, to_value};
    use serde_json5::{from_str, to_string};

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
}
