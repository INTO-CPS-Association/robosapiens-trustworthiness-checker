use crate::{InputProvider, OutputStream, Value, VarName};
use futures::stream;
use smol::stream::StreamExt;
use std::collections::BTreeMap;

// Dead code is allowed in this file since cargo does not correctly
// track when functions are used in tests or with specific features.

pub fn input_empty() -> BTreeMap<VarName, OutputStream<Value>> {
    BTreeMap::new()
}

pub fn input_streams1() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)])) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams2() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)])) as OutputStream<Value>,
    );
    input_streams.insert(
        "s".into(),
        Box::pin(stream::iter(vec![
            Value::Str("x+y".into()),
            Value::Str("x+y".into()),
        ])) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams3() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![Value::Int(1), Value::Int(3)])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![Value::Int(2), Value::Int(4)])),
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams4() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![
            Value::Str("a".into()),
            Value::Str("c".into()),
        ])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![
            Value::Str("b".into()),
            Value::Str("d".into()),
        ])),
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams5() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
        ])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
        ])),
    );
    input_streams
}

#[allow(dead_code)]
pub fn input_streams_float() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter(vec![Value::Float(1.3), Value::Float(3.4)])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter(vec![Value::Float(2.4), Value::Float(4.3)])) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn spec_empty() -> &'static str {
    ""
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_simple_modulo_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = y % x"
}

#[allow(dead_code)]
pub fn spec_simple_modulo_monitor_typed() -> &'static str {
    "in x: Int\n\
     in y: Int\n\
     out z: Int\n\
     z = y % x"
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor_typed() -> &'static str {
    "in x: Int\n\
     in y: Int\n\
     out z: Int\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor_typed_float() -> &'static str {
    "in x: Float\n\
     in y: Float\n\
     out z: Float\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_typed_string_concat() -> &'static str {
    "in x: Str\n\
     in y: Str\n\
     out z: Str\n\
     z = x ++ y"
}

#[allow(dead_code)]
pub fn spec_typed_count_monitor() -> &'static str {
    "out x: Int\n\
     x = 1 + default(x[-1], 0)"
}

#[allow(dead_code)]
pub fn spec_typed_dynamic_monitor() -> &'static str {
    "in x: Int\n\
    in y: Int\n\
    in s: Str\n\
    out z: Int\n\
    out w: Int\n\
    z = x + y\n\
    w = dynamic(s)"
}

#[allow(dead_code)]
pub fn spec_count_monitor() -> &'static str {
    "out x\n\
     x = 1 + default(x[-1], 0)"
}

#[allow(dead_code)]
pub fn spec_dynamic_monitor() -> &'static str {
    "in x\n\
    in y\n\
    in s\n\
    out z\n\
    out w\n\
    z = x + y\n\
    w = dynamic(s)"
}

#[allow(dead_code)]
pub fn spec_dynamic_restricted_monitor() -> &'static str {
    "in x\n\
    in y\n\
    in s\n\
    out z\n\
    out w\n\
    z = x + y\n\
    w = dynamic(s, {x,y})"
}

#[allow(dead_code)]
pub fn spec_maple_sequence() -> &'static str {
    "in stage : Str\n
     out m: Bool\n
     out a: Bool\n
     out p: Bool\n
     out l: Bool\n
     out e: Bool\n
     out maple : Bool\n
     m = (stage == \"m\") && default(e[-1], true)\n
     a = (stage == \"a\") && default(m[-1], false)\n
     p = (stage == \"p\") && default(a[-1], false)\n
     l = (stage == \"l\") && default(p[-1], false)\n
     e = (stage == \"e\") && default(l[-1], false)\n
     maple = m || a || p || l || e"
}

#[allow(dead_code)]
pub fn maple_valid_input_stream(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "stage".into(),
        Box::pin(stream::iter((0..size).map(|x| {
            if x % 5 == 0 {
                Value::Str("m".into())
            } else if x % 5 == 1 {
                Value::Str("a".into())
            } else if x % 5 == 2 {
                Value::Str("p".into())
            } else if x % 5 == 3 {
                Value::Str("l".into())
            } else {
                Value::Str("e".into())
            }
        }))) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_1(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "stage".into(),
        Box::pin(stream::iter((0..size).map(|x| {
            if x % 5 == 0 {
                Value::Str("m".into())
            } else if x % 5 == 1 {
                Value::Str("a".into())
            } else if x % 5 == 2 {
                Value::Str("m".into())
            } else if x % 5 == 3 {
                Value::Str("l".into())
            } else {
                Value::Str("e".into())
            }
        }))) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_2(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "stage".into(),
        Box::pin(stream::iter((0..size).map(|x| {
            if x % 5 == 0 {
                Value::Str("m".into())
            } else if x % 5 == 1 {
                Value::Str("a".into())
            } else if x % 5 == 2 {
                Value::Str("l".into())
            } else if x % 5 == 3 {
                Value::Str("p".into())
            } else {
                Value::Str("e".into())
            }
        }))) as OutputStream<Value>,
    );
    input_streams
}

#[allow(dead_code)]
pub fn spec_simple_add_decomposed_1() -> &'static str {
    "in x
     in y
     out w
     w = x + y"
}

#[allow(dead_code)]
pub fn spec_simple_add_decomposed_2() -> &'static str {
    "in z
     in w
     out v
     v = z + w"
}

#[allow(dead_code)]
pub fn spec_simple_add_decomposable() -> &'static str {
    "in x
     in y
     in z
     out w
     out v
     w = x + y
     v = z + w"
}

#[allow(dead_code)]
pub fn input_streams_defer_1() -> impl InputProvider<Val = Value> {
    let mut input_streams = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter((0..15).map(Value::Int))) as OutputStream<Value>,
    );

    // Create e stream with the defer expression
    input_streams.insert(
        "e".into(),
        Box::pin(futures::stream::iter((0..15).map(|i| {
            if i == 1 {
                Value::Str("x + 1".into())
            } else {
                Value::Unknown
            }
        }))) as OutputStream<Value>,
    );

    input_streams
}

#[allow(dead_code)]
pub fn input_streams_defer_2() -> impl InputProvider<Val = Value> {
    let mut input_streams = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter((0..15).map(Value::Int))) as OutputStream<Value>,
    );

    // Create e stream with the defer expression
    input_streams.insert(
        "e".into(),
        Box::pin(futures::stream::iter((0..15).map(|i| {
            if i == 3 {
                Value::Str("x + 1".into())
            } else {
                Value::Unknown
            }
        }))) as OutputStream<Value>,
    );

    input_streams
}

#[allow(dead_code)]
pub fn input_streams_defer_3() -> impl InputProvider<Val = Value> {
    let mut input_streams = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter((0..15).map(Value::Int))) as OutputStream<Value>,
    );

    // Create e stream with the defer expression
    input_streams.insert(
        "e".into(),
        Box::pin(futures::stream::iter((0..15).map(|i| {
            if i == 12 {
                Value::Str("x + 1".into())
            } else {
                Value::Unknown
            }
        }))) as OutputStream<Value>,
    );

    input_streams
}

// Example where defer needs to use the history
#[allow(dead_code)]
pub fn input_streams_defer_4() -> impl InputProvider<Val = Value> {
    let mut input_streams = BTreeMap::new();

    // Create x stream with values 1 through 5
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter((0..5).map(Value::Int))) as OutputStream<Value>,
    );

    // Create e stream with the defer expression
    input_streams.insert(
        "e".into(),
        Box::pin(futures::stream::iter((0..5).map(|i| {
            if i == 2 {
                Value::Str("x[-1]".into())
            } else {
                Value::Unknown
            }
        }))) as OutputStream<Value>,
    );

    input_streams
}

#[allow(dead_code)]
pub fn spec_defer() -> &'static str {
    "in x
     in e
     out z
     z = defer(e)"
}

#[allow(dead_code)]
pub fn spec_future_indexing() -> &'static str {
    "in x
     in y
     out z
     out a
     z = x[1]
     a = y"
}

#[allow(dead_code)]
pub fn spec_past_indexing() -> &'static str {
    "in x
     in y
     out z
     z = x[-1]"
}

#[allow(dead_code)]
pub fn input_streams_indexing() -> impl InputProvider<Val = Value> {
    let mut input_streams = BTreeMap::new();

    // Create x stream with values 1 through 6
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter((0..6).map(Value::Int))) as OutputStream<Value>,
    );

    // Create x stream with values 1 through 6
    input_streams.insert(
        "y".into(),
        Box::pin(futures::stream::iter((0..6).map(Value::Int))) as OutputStream<Value>,
    );

    input_streams
}

#[allow(dead_code)]
pub fn spec_add_defer() -> &'static str {
    "in x
     in y
     in e
     out z
     z = defer(e)"
}

#[allow(dead_code)]
pub fn spec_deferred_and() -> &'static str {
    "in x: Bool
     in y: Bool
     in e: Str
     out z: Bool
     z = default(defer(e), true)"
}

#[allow(dead_code)]
pub fn spec_direct_and() -> &'static str {
    "in x: Bool
     in y: Bool
     out z: Bool
     z = x && y"
}
#[allow(dead_code)]
pub fn spec_deferred_globally() -> &'static str {
    "in x: Bool
     in e: Str
     out y: Bool
     y = default(defer(e), x && default(x[-1], true))"
}

#[allow(dead_code)]
pub fn spec_direct_globally() -> &'static str {
    "in x: Bool
     out y: Bool
     y = x && default(x[-1], true)"
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark(
    percent: usize,
    size: usize,
) -> BTreeMap<VarName, OutputStream<Value>> {
    let x: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(true)));
    let y: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(false)));
    let e1 = stream::repeat(Value::Unknown).take(size * percent / 100 - 1);
    let e2 = stream::repeat(Value::Str("x && y".into()));
    let e: OutputStream<Value> = if percent == 100 {
        Box::pin(e1)
    } else if percent == 0 {
        Box::pin(e2)
    } else {
        Box::pin(e1.chain(e2))
    };
    vec![("x".into(), x), ("y".into(), y), ("e".into(), e)]
        .into_iter()
        .collect()
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark_direct(
    _size: usize,
) -> BTreeMap<VarName, OutputStream<Value>> {
    let x: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(true)));
    let y: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(false)));
    vec![("x".into(), x), ("y".into(), y)].into_iter().collect()
}
#[allow(dead_code)]
pub fn input_streams_paper_benchmark_globally(
    percent: usize,
    size: usize,
) -> BTreeMap<VarName, OutputStream<Value>> {
    let x: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(true)));
    let e1 = stream::repeat(Value::Unknown).take(size * percent / 100 - 1);
    let e2 = stream::repeat(Value::Str("x || default(y[-1], false)".into()));
    let e: OutputStream<Value> = if percent == 100 {
        Box::pin(e1)
    } else if percent == 0 {
        Box::pin(e2)
    } else {
        Box::pin(e1.chain(e2))
    };
    vec![("x".into(), x), ("e".into(), e)].into_iter().collect()
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark_direct_globally(
    _size: usize,
) -> BTreeMap<VarName, OutputStream<Value>> {
    let x: OutputStream<Value> = Box::pin(stream::repeat(Value::Bool(true)));
    vec![("x".into(), x)].into_iter().collect()
}

pub fn input_streams_add_defer(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter((0..size).map(|x| Value::Int(2 * x)))) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter((0..size).map(|y| Value::Int(2 * y + 1)))) as OutputStream<Value>,
    );
    let e_stream = stream::repeat(Value::Unknown)
        .take((size / 2) as usize)
        .chain(stream::iter(
            (0..size / 2).map(|_| Value::Str("x + y".into())),
        ));
    input_streams.insert("e".into(), Box::pin(e_stream) as OutputStream<Value>);

    input_streams
}

pub fn input_streams_simple_add(size: usize) -> BTreeMap<VarName, OutputStream<Value>> {
    let size = size as i64;
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(stream::iter((0..size).map(|x| Value::Int(2 * x)))) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(stream::iter((0..size).map(|y| Value::Int(2 * y + 1)))) as OutputStream<Value>,
    );
    input_streams
}
