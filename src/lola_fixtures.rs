use crate::{
    InputProvider, LOLASpecification, SExpr, Value,
    io::map::MapInputProvider,
    lang::dynamic_lola::{lalr_parser::LALRParser, type_checker::SExprTE},
    runtime::asynchronous::{AsyncMonitorRunner, Context},
    semantics::{
        AsyncConfig, distributed::contexts::DistributedContext,
        untimed_untyped_lola::semantics::UntimedLolaSemantics,
    },
};
use std::{collections::BTreeMap, iter};

// Dead code is allowed in this file since cargo does not correctly
// track when functions are used in tests or with specific features.

#[derive(Clone)]
pub struct TestConfig {}
impl AsyncConfig for TestConfig {
    type Val = Value;
    type Expr = SExpr;
    type Ctx = Context<Self>;
}
#[derive(Clone)]
pub struct TestTypedConfig {}
impl AsyncConfig for TestTypedConfig {
    type Val = Value;
    type Expr = SExprTE;
    type Ctx = Context<Self>;
}
#[derive(Clone)]
pub struct TestDistConfig {}
impl AsyncConfig for TestDistConfig {
    type Val = Value;
    type Expr = SExpr;
    type Ctx = DistributedContext<Self>;
}

// Default semantics to use in tests
pub type TestSemantics = UntimedLolaSemantics<LALRParser>;

// Default monitor runner to use in tests
pub type TestMonitorRunner = AsyncMonitorRunner<TestConfig, TestSemantics, LOLASpecification>;

pub fn input_empty() -> MapInputProvider {
    MapInputProvider::new(BTreeMap::new())
}

pub fn input_streams1() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Int(1), Value::Int(3)]);
    input_values.insert("y".into(), vec![Value::Int(2), Value::Int(4)]);
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams2() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Int(1), Value::Int(3)]);
    input_values.insert("y".into(), vec![Value::Int(2), Value::Int(4)]);
    input_values.insert(
        "s".into(),
        vec![Value::Str("x+y".into()), Value::Str("x+y".into())],
    );
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams3() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Int(1), Value::Int(3)]);
    input_values.insert("y".into(), vec![Value::Int(2), Value::Int(4)]);
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams4() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "x".into(),
        vec![Value::Str("a".into()), Value::Str("c".into())],
    );
    input_values.insert(
        "y".into(),
        vec![Value::Str("b".into()), Value::Str("d".into())],
    );
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams5() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert(
        "x".into(),
        vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
    );
    input_values.insert(
        "y".into(),
        vec![Value::Bool(true), Value::Bool(true), Value::Bool(false)],
    );
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams_float() -> MapInputProvider {
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), vec![Value::Float(1.3), Value::Float(3.4)]);
    input_values.insert("y".into(), vec![Value::Float(2.4), Value::Float(4.3)]);
    MapInputProvider::new(input_values)
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
     x = 1 + default(x[1], 0)"
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
     x = 1 + default(x[1], 0)"
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
     m = (stage == \"m\") && default(e[1], true)\n
     a = (stage == \"a\") && default(m[1], false)\n
     p = (stage == \"p\") && default(a[1], false)\n
     l = (stage == \"l\") && default(p[1], false)\n
     e = (stage == \"e\") && default(l[1], false)\n
     maple = m || a || p || l || e"
}

#[allow(dead_code)]
pub fn maple_valid_input_stream(size: usize) -> MapInputProvider {
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
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_1(size: usize) -> MapInputProvider {
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
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn maple_invalid_input_stream_2(size: usize) -> MapInputProvider {
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
    MapInputProvider::new(input_values)
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

    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams_defer_2() -> impl InputProvider<Val = Value> {
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

    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams_defer_3() -> impl InputProvider<Val = Value> {
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

    MapInputProvider::new(input_values)
}

// Example where defer needs to use the history
#[allow(dead_code)]
pub fn input_streams_defer_4() -> impl InputProvider<Val = Value> {
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

    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn spec_defer() -> &'static str {
    "in x
     in e
     out z
     z = defer(e)"
}

#[allow(dead_code)]
pub fn spec_dynamic() -> &'static str {
    "in x
     in e
     out z
     z = dynamic(e)"
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
     z = x[1]"
}

#[allow(dead_code)]
pub fn input_streams_indexing() -> impl InputProvider<Val = Value> {
    let mut input_values = BTreeMap::new();

    // Create x stream with values 1 through 6
    input_values.insert("x".into(), (0..6).map(Value::Int).collect());

    // Create x stream with values 1 through 6
    input_values.insert("y".into(), (0..6).map(Value::Int).collect());

    MapInputProvider::new(input_values)
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
     y = default(defer(e), x && default(x[1], true))"
}

#[allow(dead_code)]
pub fn spec_direct_globally() -> &'static str {
    "in x: Bool
     out y: Bool
     y = x && default(x[1], true)"
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark(percent: usize, size: usize) -> MapInputProvider {
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
    MapInputProvider::new(BTreeMap::from(inputs))
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark_direct(size: usize) -> MapInputProvider {
    let x = iter::repeat(Value::Bool(true)).take(size).collect();
    let y = iter::repeat(Value::Bool(false)).take(size).collect();
    let map = BTreeMap::from_iter(vec![("x".into(), x), ("y".into(), y)].into_iter());
    MapInputProvider::new(map)
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark_globally(percent: usize, size: usize) -> MapInputProvider {
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
    MapInputProvider::new(inputs)
}

#[allow(dead_code)]
pub fn input_streams_paper_benchmark_direct_globally(size: usize) -> MapInputProvider {
    let x = iter::repeat(Value::Bool(true)).take(size).collect();
    let inputs = BTreeMap::from_iter(vec![("x".into(), x)].into_iter());
    MapInputProvider::new(inputs)
}

pub fn input_streams_add_defer(size: usize) -> MapInputProvider {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), (0..size).map(|x| Value::Int(2 * x)).collect());
    input_values.insert(
        "y".into(),
        (0..size).map(|y| Value::Int(2 * y + 1)).collect(),
    );
    let e_stream = iter::repeat(Value::Deferred)
        .take((size / 2) as usize)
        .chain((0..size / 2).map(|_| Value::Str("x + y".into())))
        .collect();
    input_values.insert("e".into(), e_stream);

    MapInputProvider::new(input_values)
}

pub fn input_streams_simple_add(size: usize) -> MapInputProvider {
    let size = size as i64;
    let mut input_values = BTreeMap::new();
    input_values.insert("x".into(), (0..size).map(|x| Value::Int(2 * x)).collect());
    input_values.insert(
        "y".into(),
        (0..size).map(|y| Value::Int(2 * y + 1)).collect(),
    );
    MapInputProvider::new(input_values)
}

#[allow(dead_code)]
pub fn input_streams_defer_comp_dynamic() -> impl InputProvider<Val = Value> {
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

    MapInputProvider::new(input_values)
}
