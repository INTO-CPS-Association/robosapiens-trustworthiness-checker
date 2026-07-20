use crate::{
    InputStream, Value,
    io::map,
    lang::dsrv::lalr_parser::LALRParser,
    runtime::{
        asynchronous::AsyncRuntime,
        builder::{DistValueConfig, ValueConfig},
    },
    semantics::untimed_dsrv::semantics::UntimedDsrvSemantics,
};
use std::{collections::BTreeMap, iter};

// Dead code is allowed in this file since cargo does not correctly
// track when functions are used in tests or with specific features.

pub type TestConfig = ValueConfig;
pub type TestDistConfig = DistValueConfig;

// Default semantics to use in tests
pub type TestSemantics = UntimedDsrvSemantics<LALRParser>;

// Default monitor runner to use in tests
pub type TestRuntime = AsyncRuntime<TestConfig, TestSemantics>;

pub fn empty_input_stream() -> InputStream<Value> {
    map::input_stream(BTreeMap::new())
}

pub fn integer_pair_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Int(1), Value::Int(3)]);
    input_values.insert("y".into(), vec![Value::Int(2), Value::Int(4)]);
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn dynamic_expression_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Int(1), Value::Int(3)]);
    input_values.insert("y".into(), vec![Value::Int(2), Value::Int(4)]);
    input_values.insert(
        "s".into(),
        vec![Value::Str("x+y".into()), Value::Str("x+y".into())],
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn string_pair_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "x".into(),
        vec![Value::Str("a".into()), Value::Str("c".into())],
    );
    input_values.insert(
        "y".into(),
        vec![Value::Str("b".into()), Value::Str("d".into())],
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn boolean_pair_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "x".into(),
        vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
    );
    input_values.insert(
        "y".into(),
        vec![Value::Bool(true), Value::Bool(true), Value::Bool(false)],
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn float_pair_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Float(1.3), Value::Float(3.4)]);
    input_values.insert("y".into(), vec![Value::Float(2.4), Value::Float(4.3)]);
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"
}

#[allow(dead_code)]
pub fn spec_simple_add_monitor_plus_one() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y + 1"
}

#[allow(dead_code)]
pub fn spec_acc_monitor() -> &'static str {
    "in x\n\
     out z\n\
     z = default(z[1], 0) + x"
}

#[allow(dead_code)]
pub fn spec_assignment_monitor() -> &'static str {
    "in x\n\
     out v\n\
     v = x"
}

#[allow(dead_code)]
pub fn spec_assignment2_monitor() -> &'static str {
    "in x\n\
     out v\n\
     out w\n\
     v = x\n\
     w = v + 1"
}

#[allow(dead_code)]
pub fn spec_simple_add_aux_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     aux u\n\
     var w\n\
     u = x\n\
     w = y\n\
     z = u + w"
}

#[allow(dead_code)]
pub fn spec_simple_add_aux_typed_monitor() -> &'static str {
    "in x: Int\n\
     in y: Int\n\
     out z: Int\n\
     aux u: Int\n\
     var w: Int\n\
     u = x\n\
     w = y\n\
     z = u + w"
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
pub fn spec_sindex() -> &'static str {
    "in x\n\
     out z\n\
     z = x[1]"
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
     m = (stage == \"m\") && default(e[1], true)\n
     a = (stage == \"a\") && default(m[1], false)\n
     p = (stage == \"p\") && default(a[1], false)\n
     l = (stage == \"l\") && default(p[1], false)\n
     e = (stage == \"e\") && default(l[1], false)\n
     maple = m || a || p || l || e"
}

#[allow(dead_code)]
pub fn maple_valid_input_stream(size: usize) -> InputStream<Value> {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "stage".into(),
        (0..size)
            .map(|x| {
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
            })
            .collect(),
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_1(size: usize) -> InputStream<Value> {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "stage".into(),
        (0..size)
            .map(|x| {
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
            })
            .collect(),
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_2(size: usize) -> InputStream<Value> {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "stage".into(),
        (0..size)
            .map(|x| {
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
            })
            .collect(),
    );
    map::input_stream(input_values)
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
pub fn defer_input_stream_1() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_values.insert("x".into(), (0..15).map(Value::Int).collect());

    // Create e stream with the defer expression
    input_values.insert(
        "e".into(),
        (0..15)
            .map(|i| {
                if i == 1 {
                    Value::Str("x + 1".into())
                } else {
                    Value::Deferred
                }
            })
            .collect(),
    );

    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn defer_input_stream_2() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_values.insert("x".into(), (0..15).map(Value::Int).collect());

    // Create e stream with the defer expression
    input_values.insert(
        "e".into(),
        (0..15)
            .map(|i| {
                if i == 3 {
                    Value::Str("x + 1".into())
                } else {
                    Value::Deferred
                }
            })
            .collect(),
    );

    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn defer_input_stream_3() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 15
    input_values.insert("x".into(), (0..15).map(Value::Int).collect());

    // Create e stream with the defer expression
    input_values.insert(
        "e".into(),
        (0..15)
            .map(|i| {
                if i == 12 {
                    Value::Str("x + 1".into())
                } else {
                    Value::Deferred
                }
            })
            .collect(),
    );

    map::input_stream(input_values)
}

// Example where defer needs to use the history
#[allow(dead_code)]
pub fn defer_input_stream_4() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 5
    input_values.insert("x".into(), (0..5).map(Value::Int).collect());

    // Create e stream with the defer expression
    input_values.insert(
        "e".into(),
        (0..5)
            .map(|i| {
                if i == 2 {
                    Value::Str("x[1]".into())
                } else {
                    Value::Deferred
                }
            })
            .collect(),
    );

    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn spec_defer() -> &'static str {
    "in x
     in e
     out z
     z = defer(e)"
}

#[allow(dead_code)]
pub fn indexing_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 6
    input_values.insert("x".into(), (0..6).map(Value::Int).collect());

    // Create x stream with values 1 through 6
    input_values.insert("y".into(), (0..6).map(Value::Int).collect());

    map::input_stream(input_values)
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
pub fn paper_benchmark_input_stream(percent: usize, size: usize) -> InputStream<Value> {
    let x = iter::repeat(true.into()).take(size).collect();
    let y = iter::repeat(true.into()).take(size).collect();

    let e1 = iter::repeat(Value::Deferred).take((size * percent / 100).saturating_sub(1));
    let e2 = iter::repeat(Value::Str("x && y".into())).take(size);
    let e: Vec<Value> = if percent == 100 {
        e1.collect()
    } else if percent == 0 {
        e2.collect()
    } else {
        e1.chain(e2).collect()
    };
    let inputs =
        BTreeMap::from_iter(vec![("x".into(), x), ("y".into(), y), ("e".into(), e)].into_iter());
    map::input_stream(BTreeMap::from(inputs))
}

#[allow(dead_code)]
pub fn direct_paper_benchmark_input_stream(size: usize) -> InputStream<Value> {
    let x = iter::repeat(Value::Bool(true)).take(size).collect();
    let y = iter::repeat(Value::Bool(false)).take(size).collect();
    let map = BTreeMap::from_iter(vec![("x".into(), x), ("y".into(), y)].into_iter());
    map::input_stream(map)
}

#[allow(dead_code)]
pub fn global_paper_benchmark_input_stream(percent: usize, size: usize) -> InputStream<Value> {
    let x = iter::repeat(Value::Bool(true)).take(size).collect();
    let e1 = iter::repeat(Value::Deferred).take(size * percent / 100 - 1);
    let e2 = iter::repeat(Value::Str("x || default(y[1], false)".into())).take(size);
    let e: Vec<Value> = if percent == 100 {
        e1.collect()
    } else if percent == 0 {
        e2.collect()
    } else {
        e1.chain(e2).collect()
    };
    let inputs = BTreeMap::from_iter(vec![("x".into(), x), ("e".into(), e)].into_iter());
    map::input_stream(inputs)
}

#[allow(dead_code)]
pub fn direct_global_paper_benchmark_input_stream(size: usize) -> InputStream<Value> {
    let x = iter::repeat(Value::Bool(true)).take(size).collect();
    let inputs = BTreeMap::from_iter(vec![("x".into(), x)].into_iter());
    map::input_stream(inputs)
}

pub fn add_defer_input_stream(size: usize) -> InputStream<Value> {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), (0..size).map(|x| Value::Int(2 * x)).collect());
    input_values.insert(
        "y".into(),
        (0..size).map(|y| Value::Int(2 * y + 1)).collect(),
    );
    let add = if size % 2 == 0 { 0 } else { 1 };
    let e_stream = iter::repeat(Value::Deferred)
        .take((size / 2) as usize)
        .chain((0..(size / 2) + add).map(|_| Value::Str("x + y".into())))
        .collect();
    input_values.insert("e".into(), e_stream);

    map::input_stream(input_values)
}

pub fn simple_add_input_stream(size: usize) -> InputStream<Value> {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), (0..size).map(|x| Value::Int(2 * x)).collect());
    input_values.insert(
        "y".into(),
        (0..size).map(|y| Value::Int(2 * y + 1)).collect(),
    );
    map::input_stream(input_values)
}

#[allow(dead_code)]
pub fn dynamic_defer_composition_input_stream() -> InputStream<Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 100
    input_values.insert("x".into(), (0..5).map(Value::Int).collect());

    // Create e stream with the defer expression
    input_values.insert(
        "e".into(),
        (0..5)
            .map(|i| {
                if i == 1 {
                    Value::Str("x[2]".into())
                } else {
                    Value::Deferred
                }
            })
            .collect(),
    );

    map::input_stream(input_values)
}
