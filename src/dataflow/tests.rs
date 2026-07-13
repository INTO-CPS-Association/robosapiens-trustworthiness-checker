use super::*;
use crate::dsrv_fixtures::TestConfig;
use crate::io::map;
use crate::io::testing::ManualOutputHandler;
use crate::lang::dsrv::ast::generation::{
    arb_boolean_dsrv_spec, arb_boolean_sexpr, arb_dsrv_spec, arb_float_sexpr, arb_int_sexpr,
    arb_string_sexpr,
};
use crate::lang::dsrv::ast::{NumericalBinOp, SBinOp, SExpr};
use crate::lang::dsrv::lalr_parser::LALRParser;
use crate::lang::dsrv::type_checker::{TypeCheckable, TypeInfo, type_check};
use crate::runtime::asynchronous::AsyncRuntimeBuilder;
use crate::runtime::asynchronous::Context;
use crate::runtime::builder::{RuntimeBuilder, SemiSyncValueConfig};
use crate::runtime::dataflow::DataflowRuntimeBuilder;
use crate::runtime::semi_sync::SemiSyncRuntimeBuilder;
use crate::semantics::UntimedDsrvSemantics;
use crate::{UntypedDsrvSpecification, async_test, dsrv_specification};
use crate::{
    core::{InputEvent, OutputStream, Runtime},
    lang::core::parser::{ExprParser, SpecParser},
    semantics::{MonitoringSemantics, StreamContext},
};
use futures::{StreamExt, stream};
use macro_rules_attribute::apply;
use proptest::prelude::*;
use smol::LocalExecutor;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

fn arb_typed_dsrv_spec() -> impl Strategy<Value = TypedDsrvSpecification> {
    let int_vars = vec![VarName::new("a"), VarName::new("b")];
    let float_vars = vec![VarName::new("c"), VarName::new("d")];
    let bool_vars = vec![VarName::new("e"), VarName::new("f")];
    let string_vars = vec![VarName::new("g"), VarName::new("h")];
    let expression = prop_oneof![
        arb_int_sexpr(int_vars).boxed(),
        arb_float_sexpr(float_vars).boxed(),
        arb_boolean_sexpr(bool_vars).boxed(),
        arb_string_sexpr(string_vars).boxed(),
    ];

    prop::collection::btree_map("[i-z]".prop_map(VarName::from), expression, 0..8).prop_filter_map(
        "generated expression must have a typed representation",
        |exprs| {
            let mut types = TypeInfo::new();
            for name in ["a", "b"] {
                types.insert(VarName::new(name), StreamType::Int);
            }
            for name in ["c", "d"] {
                types.insert(VarName::new(name), StreamType::Float);
            }
            for name in ["e", "f"] {
                types.insert(VarName::new(name), StreamType::Bool);
            }
            for name in ["g", "h"] {
                types.insert(VarName::new(name), StreamType::Str);
            }
            let exprs = exprs
                .into_iter()
                .map(|(name, expr)| Some((name, expr.type_check(&mut types).ok()?)))
                .collect::<Option<BTreeMap<_, _>>>()?;
            let input_vars = ["a", "b", "c", "d", "e", "f", "g", "h"]
                .into_iter()
                .map(VarName::new)
                .collect();
            let stream_vars = exprs.keys().cloned().collect::<BTreeSet<_>>();

            Some(TypedDsrvSpecification {
                input_vars,
                output_vars: stream_vars.clone(),
                aux_vars: Default::default(),
                stream_vars,
                exprs,
                // Deliberately omit annotations: compilation must handle malformed typed specs.
                type_annotations: BTreeMap::new(),
            })
        },
    )
}

fn sparse_int() -> impl Strategy<Value = Value> {
    prop_oneof![
        6 => any::<i16>().prop_map(|value| Value::Int(i64::from(value))),
        2 => Just(Value::NoVal),
        2 => Just(Value::Deferred),
    ]
}

fn sparse_bool() -> impl Strategy<Value = Value> {
    prop_oneof![
        6 => any::<bool>().prop_map(Value::Bool),
        2 => Just(Value::NoVal),
        2 => Just(Value::Deferred),
    ]
}

fn arb_valid_dataflow_program_and_inputs()
-> impl Strategy<Value = (UntypedDsrvSpecification, Vec<(Value, Value)>)> {
    (
        prop::collection::vec(any::<[u8; 3]>(), 1..16),
        prop::collection::vec((sparse_int(), sparse_bool()), 1..40),
    )
        .prop_map(|(recipes, rows)| {
            let mut exprs = BTreeMap::<VarName, SpannedExpr>::new();
            let mut annotations = BTreeMap::from([
                (VarName::new("x"), StreamType::Int),
                (VarName::new("flag"), StreamType::Bool),
            ]);

            for (index, [operator, first, second]) in recipes.into_iter().enumerate() {
                let name = VarName::from(format!("s{index}"));
                let dependency = |selector: u8| {
                    let selected = usize::from(selector) % (index + 1);
                    if selected == 0 {
                        VarName::new("x")
                    } else {
                        VarName::from(format!("s{}", selected - 1))
                    }
                };
                let lhs = dependency(first);
                let rhs = dependency(second);
                let offset = u64::from(second % 5);
                let var = |name: VarName| SExpr::Var(name).into();
                let expression = match operator % 12 {
                    0 => SExpr::BinOp(
                        Box::new(var(lhs)),
                        Box::new(var(rhs)),
                        SBinOp::NOp(NumericalBinOp::Add),
                    ),
                    1 => SExpr::If(
                        Box::new(var(VarName::new("flag"))),
                        Box::new(var(lhs)),
                        Box::new(var(rhs)),
                    ),
                    2 => SExpr::SIndex(Box::new(var(lhs)), offset),
                    3 => SExpr::Default(
                        Box::new(SExpr::SIndex(Box::new(var(lhs)), offset).into()),
                        Box::new(var(VarName::new("x"))),
                    ),
                    4 => SExpr::Update(Box::new(var(lhs)), Box::new(var(rhs))),
                    5 => SExpr::Init(Box::new(var(lhs)), Box::new(var(rhs))),
                    6 => SExpr::BinOp(
                        Box::new(
                            SExpr::Default(
                                Box::new(
                                    SExpr::SIndex(
                                        Box::new(var(name.clone())),
                                        u64::from(second % 4) + 1,
                                    )
                                    .into(),
                                ),
                                Box::new(SExpr::Val(Value::Int(0)).into()),
                            )
                            .into(),
                        ),
                        Box::new(var(VarName::new("x"))),
                        SBinOp::NOp(NumericalBinOp::Add),
                    ),
                    7 => SExpr::Abs(Box::new(var(lhs))),
                    8 => SExpr::Latch(Box::new(var(lhs)), Box::new(var(rhs))),
                    9 => SExpr::BinOp(
                        Box::new(var(lhs)),
                        Box::new(var(rhs)),
                        SBinOp::NOp(NumericalBinOp::Sub),
                    ),
                    10 => SExpr::Default(Box::new(var(lhs)), Box::new(var(rhs))),
                    _ => SExpr::Val(Value::Int(i64::from(first))),
                };
                exprs.insert(name.clone(), expression.into());
                annotations.insert(name, StreamType::Int);
            }

            let stream_vars = exprs.keys().cloned().collect::<BTreeSet<_>>();
            let spec = UntypedDsrvSpecification::new(
                BTreeSet::from([VarName::new("x"), VarName::new("flag")]),
                stream_vars,
                exprs,
                annotations,
                Vec::new(),
            );
            (spec, rows)
        })
}

type DynamicInputRow = (Value, Value, Value);

fn arb_runtime_compiled_program(
    combinator: &'static str,
) -> impl Strategy<Value = (UntypedDsrvSpecification, Vec<DynamicInputRow>)> {
    prop::collection::vec((sparse_int(), sparse_int(), any::<[u8; 2]>()), 1..40).prop_map(
        move |rows| {
            let property = if combinator == "dynamic" {
                "dynamic(source: Int, {x, y, source, sum})"
            } else {
                "defer(source: Int)"
            };
            let spec_source = format!(
                "in x: Int\nin y: Int\nin source: Str\n\
             out z: Int\naux sum: Int\n\
             z = {property}\n\
             sum = x + y"
            );
            let mut spec_input = spec_source.as_str();
            let spec = dsrv_specification(&mut spec_input)
                .expect("generated dynamic/defer specification must parse");
            let rows = rows
                .into_iter()
                .map(|(x, y, [action, expression])| {
                    let source = match action % 5 {
                        0 => Value::NoVal,
                        1 => Value::Deferred,
                        _ => {
                            let offset = expression % 5;
                            let source = match (combinator, expression % 7) {
                                ("defer", 2) => "x + y".to_owned(),
                                ("defer", 5) => format!("default(y[{offset}], x)"),
                                (_, 0) => "x".to_owned(),
                                (_, 1) => "y".to_owned(),
                                (_, 2) => "sum".to_owned(),
                                (_, 3) => "x + y".to_owned(),
                                (_, 4) => format!("default(x[{offset}], 0) + y"),
                                (_, 5) => format!("default(sum[{offset}], x)"),
                                _ => "if x > y then x + y else x".to_owned(),
                            };
                            Value::Str(source.into())
                        }
                    };
                    (x, y, source)
                })
                .collect();
            (spec, rows)
        },
    )
}

fn evaluate_runtime_compiled_property(spec: UntypedDsrvSpecification, rows: &[DynamicInputRow]) {
    let typed_spec = type_check(spec.clone()).expect("generated specification must type check");
    let mut monitors = [
        DataflowMonitor::try_compile_untyped(spec)
            .expect("generated untyped specification must compile"),
        DataflowMonitor::try_compile_typed(typed_spec)
            .expect("generated typed specification must compile"),
    ];

    let mut traces = Vec::new();
    for monitor in &mut monitors {
        let mut trace = Vec::new();
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        for (x, y, source) in rows {
            let input = monitor
                .input_vars()
                .iter()
                .map(|name| {
                    if name == &VarName::new("x") {
                        x.clone()
                    } else if name == &VarName::new("y") {
                        y.clone()
                    } else if name == &VarName::new("source") {
                        source.clone()
                    } else {
                        unreachable!("generator declares only x, y, and source")
                    }
                })
                .collect::<Vec<_>>();
            let result = monitor.evaluate(&input, &mut output);
            trace.push((result.map_err(|error| error.to_string()), output.clone()));
            if trace.last().is_some_and(|(result, _)| result.is_err()) {
                break;
            }
        }
        traces.push(trace);
    }

    assert_eq!(traces[0], traces[1]);
}

const DATAFLOW_PROPTEST_CASES: u32 = if cfg!(feature = "extended-proptests") {
    10_000
} else {
    256
};

const RUNTIME_COMPILED_PROPTEST_CASES: u32 = if cfg!(feature = "extended-proptests") {
    1_000
} else {
    64
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(DATAFLOW_PROPTEST_CASES))]

    /// Compilation must terminate without panicking for every generated DSRV AST. Generated
    /// specifications may be invalid; a structured `DataflowCompileError` is a valid result.
    #[test]
    fn dataflow_plan_compilation_is_total(spec in arb_boolean_dsrv_spec()) {
        let _ = DataflowMonitor::try_compile_untyped(spec);
    }

    #[test]
    fn untyped_dataflow_plan_compilation_is_total(spec in arb_dsrv_spec()) {
        let _ = DataflowMonitor::try_compile_untyped(spec);
    }

    #[test]
    fn typed_dataflow_plan_compilation_is_total(spec in arb_typed_dsrv_spec()) {
        let _ = DataflowMonitor::try_compile_typed(spec);
    }

    #[test]
    fn valid_dataflow_program_evaluation_is_total(
        (spec, rows) in arb_valid_dataflow_program_and_inputs()
    ) {
        let typed_spec = type_check(spec.clone()).expect("generator must produce a typed program");
        let mut augmented_spec = spec.clone();
        let unused = VarName::new("unused");
        augmented_spec.aux_vars.insert(unused.clone());
        augmented_spec.stream_vars.insert(unused.clone());
        augmented_spec
            .exprs
            .insert(unused.clone(), SExpr::Val(Value::Int(0)).into());
        augmented_spec
            .type_annotations
            .insert(unused, StreamType::Int);
        let mut monitors = [
            DataflowMonitor::try_compile_untyped(spec.clone())
                .expect("generator must produce an untyped dataflow plan"),
            DataflowMonitor::try_compile_typed(typed_spec)
                .expect("generator must produce a typed dataflow plan"),
            DataflowMonitor::try_compile_untyped(spec)
                .expect("recompilation must produce an equivalent dataflow plan"),
            DataflowMonitor::try_compile_untyped(augmented_spec)
                .expect("adding an unused stream must preserve a valid dataflow plan"),
        ];

        let mut traces = Vec::new();
        for monitor in &mut monitors {
            let mut correctly_sized_output = vec![Value::NoVal; monitor.output_vars().len()];
            let input_count_result =
                monitor.evaluate(&[Value::NoVal], &mut correctly_sized_output);
            prop_assert!(
                matches!(input_count_result, Err(DataflowEvalError::InputCount { .. })),
                "undersized input did not return InputCount"
            );
            let mut undersized_output = vec![Value::NoVal; monitor.output_vars().len() - 1];
            let output_count_result =
                monitor.evaluate(&[Value::NoVal, Value::NoVal], &mut undersized_output);
            prop_assert!(
                matches!(output_count_result, Err(DataflowEvalError::OutputCount { .. })),
                "undersized output did not return OutputCount"
            );

            let mut output = vec![Value::NoVal; monitor.output_vars().len()];
            let mut trace = Vec::new();
            for (integer, boolean) in &rows {
                let row = monitor
                    .input_vars()
                    .iter()
                    .map(|name| {
                        if name == &VarName::new("x") {
                            integer.clone()
                        } else if name == &VarName::new("flag") {
                            boolean.clone()
                        } else {
                            unreachable!("generator declares only x and flag")
                        }
                    })
                    .collect::<Vec<_>>();
                let result = monitor.evaluate(&row, &mut output);
                trace.push((result.is_ok(), output.clone()));
                if result.is_err() {
                    break;
                }
            }
            traces.push(trace);
        }
        prop_assert_eq!(&traces[0], &traces[1]);
        prop_assert_eq!(&traces[0], &traces[2]);
        prop_assert_eq!(&traces[0], &traces[3]);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(RUNTIME_COMPILED_PROPTEST_CASES))]

    #[test]
    fn dynamic_typed_and_untyped_traces_agree(
        (spec, rows) in arb_runtime_compiled_program("dynamic")
    ) {
        evaluate_runtime_compiled_property(spec, &rows);
    }

    #[test]
    fn defer_typed_and_untyped_traces_agree(
        (spec, rows) in arb_runtime_compiled_program("defer")
    ) {
        evaluate_runtime_compiled_property(spec, &rows);
    }
}

fn evaluate(monitor: &mut DataflowMonitor, columns: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let len = columns.first().map_or(0, Vec::len);
    assert!(columns.iter().all(|column| column.len() == len));
    let mut output = vec![Vec::new(); monitor.output_vars().len()];
    let mut input_row = vec![Value::NoVal; columns.len()];
    let mut output_row = vec![Value::NoVal; output.len()];
    for tick in 0..len {
        for (value, column) in input_row.iter_mut().zip(columns) {
            *value = column[tick].clone();
        }
        monitor.evaluate(&input_row, &mut output_row).unwrap();
        for (column, value) in output.iter_mut().zip(&output_row) {
            column.push(value.clone());
        }
    }
    output
}

fn evaluate_events(
    monitor: &mut DataflowMonitor,
    events: &[crate::core::InputEvent<Value>],
) -> Vec<Vec<Value>> {
    let input_ids = monitor
        .input_vars()
        .iter()
        .enumerate()
        .map(|(index, var)| (var, index))
        .collect::<BTreeMap<_, _>>();
    let mut columns = (0..input_ids.len())
        .map(|_| vec![Value::NoVal; events.len()])
        .collect::<Vec<_>>();
    for (tick, event) in events.iter().enumerate() {
        columns[input_ids[&event.var]][tick] = event.value.clone();
    }
    evaluate(monitor, &columns)
}

fn parse_expr(src: &str) -> SpannedExpr {
    let mut src = src;
    <LALRParser as ExprParser<SpannedExpr>>::parse(&mut src).expect("expression should parse")
}

async fn eval_with<S>(
    executor: Rc<LocalExecutor<'static>>,
    src: &str,
    vars: Vec<(&str, Vec<Value>)>,
) -> Vec<Value>
where
    S: MonitoringSemantics<TestConfig>,
{
    let var_names = vars
        .iter()
        .map(|(name, _)| VarName::new(*name))
        .collect::<Vec<_>>();
    let streams = vars
        .into_iter()
        .map(|(_, values)| Box::pin(stream::iter(values)) as OutputStream<Value>)
        .collect::<Vec<_>>();
    let mut ctx = Context::<TestConfig>::new(executor, var_names, streams, 8);
    let output = S::to_async_stream(parse_expr(src), &ctx);
    ctx.run().await;
    output.collect().await
}

fn eval_dataflow_spec(
    spec: UntypedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> Vec<BTreeMap<VarName, Value>> {
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let input_len = inputs.values().next().map_or(0, Vec::len);
    assert!(inputs.values().all(|values| values.len() == input_len));
    let input_columns = monitor
        .input_vars()
        .iter()
        .map(|var| &inputs[var])
        .collect::<Vec<_>>();
    let output_vars = monitor.output_vars().to_vec();
    let mut rows = Vec::with_capacity(input_len);

    let columns = input_columns.into_iter().cloned().collect::<Vec<_>>();
    let output = evaluate(&mut monitor, &columns);
    for tick in 0..input_len {
        rows.push(
            output_vars
                .iter()
                .cloned()
                .zip(output.iter().map(|column| column[tick].clone()))
                .collect(),
        );
    }
    rows
}

fn eval_dataflow(src: &str, vars: Vec<(&str, Vec<Value>)>) -> Vec<Value> {
    let spec_src = vars
        .iter()
        .map(|(name, _)| format!("in {name}"))
        .chain(["out result".to_owned(), format!("result = {src}")])
        .collect::<Vec<_>>()
        .join("\n");
    let mut spec_src = spec_src.as_str();
    let spec = dsrv_specification(&mut spec_src).expect("test specification should parse");
    let inputs = vars
        .into_iter()
        .map(|(name, values)| (VarName::new(name), values))
        .collect();
    eval_dataflow_spec(spec, inputs)
        .into_iter()
        .map(|mut row| row.remove(&VarName::new("result")).unwrap())
        .collect()
}

async fn assert_matches_current(
    executor: Rc<LocalExecutor<'static>>,
    src: &str,
    vars: Vec<(&str, Vec<Value>)>,
) {
    let expected =
        eval_with::<UntimedDsrvSemantics<LALRParser>>(executor.clone(), src, vars.clone()).await;
    let actual = eval_dataflow(src, vars);
    assert_eq!(actual, expected, "dataflow differed for `{src}`");
}

async fn eval_runtime_with<S>(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
    limit: usize,
) -> Vec<BTreeMap<VarName, Value>>
where
    S: MonitoringSemantics<TestConfig>,
{
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let outputs = output_handler.get_output();
    let monitor = AsyncRuntimeBuilder::<TestConfig, S>::new()
        .executor(executor.clone())
        .model(spec)
        .input(map::input_stream(inputs))
        .output(output_handler)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();
    outputs.take(limit).collect().await
}

async fn eval_dataflow_runtime(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
    limit: usize,
) -> Vec<BTreeMap<VarName, Value>> {
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let outputs = output_handler.get_output();
    let runtime = DataflowRuntimeBuilder::<UntypedDsrvSpecification>::new()
        .executor(executor.clone())
        .model(spec)
        .input(map::input_stream(inputs))
        .output(output_handler)
        .build()
        .await;

    executor.spawn(runtime.run()).detach();
    outputs.take(limit).collect().await
}

async fn eval_semisync_runtime(
    executor: Rc<LocalExecutor<'static>>,
    spec: UntypedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
    limit: usize,
) -> Vec<BTreeMap<VarName, Value>> {
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let outputs = output_handler.get_output();
    let runtime =
        SemiSyncRuntimeBuilder::<SemiSyncValueConfig, UntimedDsrvSemantics<LALRParser>>::new()
            .executor(executor.clone())
            .model(spec)
            .input(map::input_stream(inputs))
            .output(output_handler)
            .build()
            .await;

    executor.spawn(runtime.run()).detach();
    outputs.take(limit).collect().await
}

async fn assert_dataflow_semisync_runtime_parity(
    executor: Rc<LocalExecutor<'static>>,
    spec_src: &str,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> Vec<BTreeMap<VarName, Value>> {
    let ticks = inputs.values().next().map_or(0, Vec::len);
    assert!(
        inputs.values().all(|values| values.len() == ticks),
        "parity test inputs must describe complete logical rows"
    );
    let mut source = spec_src;
    let spec = dsrv_specification(&mut source).expect("parity specification should parse");
    let input_trace = format!("{inputs:#?}");
    let dataflow =
        eval_dataflow_runtime(executor.clone(), spec.clone(), inputs.clone(), ticks).await;
    let semisync =
        eval_semisync_runtime(executor.clone(), spec.clone(), inputs.clone(), ticks).await;
    let asynchronous =
        eval_runtime_with::<UntimedDsrvSemantics<LALRParser>>(executor, spec, inputs, ticks).await;
    assert_eq!(
        dataflow, semisync,
        "dataflow and semi-sync differed for:\n{spec_src}\ninputs:\n{input_trace}"
    );
    assert_eq!(
        dataflow, asynchronous,
        "dataflow and asynchronous differed for:\n{spec_src}\ninputs:\n{input_trace}"
    );
    dataflow
}

#[test]
fn dataflow_delay_state_persists_across_evaluations() {
    let mut spec_src = "in x\nout z\nz = default(z[3], 0) + x";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    assert_eq!(
        evaluate(&mut monitor, &[vec![1.into(), 2.into()]]),
        vec![vec![1.into(), 2.into()]]
    );
    assert_eq!(
        evaluate(&mut monitor, &[vec![3.into(), 4.into()]]),
        vec![vec![3.into(), 5.into()]]
    );
}

#[test]
fn dataflow_state_is_shared_between_event_and_row_evaluation() {
    let mut spec_src = "in x\nout z\nz = default(z[1], 0) + x";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    assert_eq!(
        evaluate_events(
            &mut monitor,
            &[InputEvent::new(VarName::new("x"), Value::Int(1),)]
        ),
        vec![vec![1.into()]]
    );
    assert_eq!(
        evaluate(&mut monitor, &[vec![2.into(), 3.into()]]),
        vec![vec![3.into(), 6.into()]]
    );
}

#[test]
fn dataflow_input_and_output_counts_are_validated() {
    let mut spec_src = "in x\nin y\nout z\nz = y";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    let missing = [Value::Int(1)];
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    assert!(matches!(
        monitor.evaluate(&missing, &mut output),
        Err(DataflowEvalError::InputCount { .. })
    ));

    let valid = [Value::Int(1), Value::Int(10)];
    assert!(matches!(
        monitor.evaluate(&valid, &mut []),
        Err(DataflowEvalError::OutputCount { .. })
    ));
}

#[test]
fn dataflow_compilers_accept_zero_stream_indices() {
    let mut spec_src = "in x: Int\nout z: Int\nz = x[0]";
    let spec = dsrv_specification(&mut spec_src).unwrap();

    DataflowMonitor::try_compile_untyped(spec.clone())
        .expect("untyped compilation should accept a zero stream index");
    let typed = type_check(spec).expect("zero stream index should type check");
    DataflowMonitor::try_compile_typed(typed)
        .expect("typed compilation should accept a zero stream index");
}

#[test]
fn dataflow_compilation_reports_computed_dependency_cycles() {
    let mut spec_src = "in x\nout z\naux a\naux b\na = b + x\nb = a + x\nz = a";
    let spec = dsrv_specification(&mut spec_src).unwrap();

    let error = match DataflowMonitor::try_compile_untyped(spec) {
        Ok(_) => panic!("computed dependency cycle should be rejected"),
        Err(error) => error,
    };

    assert!(
        matches!(error, DataflowCompileError::DependencyCycle(_)),
        "unexpected compile error: {error:?}"
    );
}

#[test]
fn dataflow_compilation_reports_unavailable_inputs() {
    let mut spec_src = "out z\nz = missing + 1";
    let spec = dsrv_specification(&mut spec_src).unwrap();

    let error = match DataflowMonitor::try_compile_untyped(spec) {
        Ok(_) => panic!("unavailable input should be rejected"),
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            DataflowCompileError::UnavailableInputs { ref inputs, .. }
                if inputs == &[VarName::new("missing")]
        ),
        "unexpected compile error: {error:?}"
    );
}

#[test]
fn dataflow_rejects_unguarded_recursive_output() {
    let mut spec_src = "in x\nout z\nz = z + x";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let error = DataflowMonitor::try_compile_untyped(spec)
        .err()
        .expect("unguarded recursion should be rejected");

    assert!(
        error.to_string().contains("positive stream delay"),
        "unexpected compile error: {error}"
    );
}

#[test]
fn dataflow_rejects_direct_recursive_output() {
    let mut spec_src = "out z\nz = z";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let error = DataflowMonitor::try_compile_untyped(spec)
        .err()
        .expect("direct recursion should be rejected");

    assert!(
        error.to_string().contains("positive stream delay"),
        "unexpected compile error: {error}"
    );
}

#[test]
fn dataflow_rejects_recursive_branch_output() {
    let mut spec_src = "in choose\nout z\nz = if choose then z else 0";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let error = DataflowMonitor::try_compile_untyped(spec)
        .err()
        .expect("recursive branch output should be rejected");

    assert!(
        error.to_string().contains("positive stream delay"),
        "unexpected compile error: {error}"
    );
}

#[test]
fn dataflow_rejects_recursive_function_capture() {
    let mut spec_src = "out z\nz = (\\v: Int -> z)(1)";
    let spec = <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(&mut spec_src).unwrap();
    let error = DataflowMonitor::try_compile_untyped(spec)
        .err()
        .expect("recursive function capture should be rejected");

    assert!(
        error.to_string().contains("positive stream delay"),
        "unexpected compile error: {error}"
    );
}

#[test]
fn dataflow_rejects_temporal_function_bodies() {
    let mut spec_src = "in x\nout z\nz = (\\v: Int -> v[1])(x)";
    let spec = <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(&mut spec_src).unwrap();
    let error = DataflowMonitor::try_compile_untyped(spec)
        .err()
        .expect("temporal function body should be rejected");

    assert!(
        error.to_string().contains("inside a function body"),
        "unexpected compile error: {error}"
    );
}

#[test]
fn dataflow_evaluates_ticks_without_inputs() {
    let mut spec_src = "out z\nz = 42";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    for _ in 0..3 {
        monitor.evaluate(&[], &mut output).unwrap();
        assert_eq!(output, vec![42.into()]);
    }
}

#[test]
fn dataflow_dynamic_waits_for_computed_context_streams() {
    let mut spec_src = "in x: Int\nin source: Str\nout before: Int\naux z: Int\nbefore = dynamic(source: Int, {z})\nz = x + 1";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (VarName::new("x"), vec![1.into(), 2.into()]),
        (
            VarName::new("source"),
            vec![Value::Str("z".into()), Value::Str("z".into())],
        ),
    ]);
    let columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var[var].clone())
        .collect::<Vec<_>>();

    assert_eq!(
        evaluate(&mut monitor, &columns),
        vec![vec![2.into(), 3.into()]]
    );
}

#[test]
fn dataflow_explicit_full_dynamic_scope_keeps_computed_dependencies() {
    let mut spec_src = "in x: Int\nin source: Str\nout before: Int\naux z: Int\nbefore = dynamic(source: Int, {x, source, z})\nz = x + 1";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (VarName::new("x"), vec![1.into(), 2.into()]),
        (
            VarName::new("source"),
            vec![Value::Str("z".into()), Value::Str("z".into())],
        ),
    ]);
    let columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var[var].clone())
        .collect::<Vec<_>>();

    assert_eq!(
        evaluate(&mut monitor, &columns),
        vec![vec![2.into(), 3.into()]]
    );
}

fn runtime_input_row(monitor: &DataflowMonitor, values: &[(&str, Value)]) -> Vec<Value> {
    monitor
        .input_vars()
        .iter()
        .map(|var| {
            values
                .iter()
                .find_map(|(name, value)| (var == &VarName::new(name)).then(|| value.clone()))
                .unwrap_or_else(|| panic!("missing value for input `{var}`"))
        })
        .collect()
}

fn runtime_output(monitor: &DataflowMonitor, output: &[Value], name: &str) -> Value {
    let index = monitor
        .output_vars()
        .iter()
        .position(|var| var == &VarName::new(name))
        .unwrap_or_else(|| panic!("missing output `{name}`"));
    output[index].clone()
}

#[test]
fn dataflow_automatic_dynamic_scope_can_introduce_a_computed_dependency() {
    let mut spec_src = "in x: Int\nin source: Str\nout before: Int\naux intermediate: Int\n\
                        before = dynamic(source: Int)\nintermediate = x + 1";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let typed = type_check(spec.clone()).unwrap();
    for mut monitor in [
        DataflowMonitor::try_compile_untyped(spec).unwrap(),
        DataflowMonitor::try_compile_typed(typed).unwrap(),
    ] {
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        let input = runtime_input_row(
            &monitor,
            &[
                ("x", 4.into()),
                ("source", Value::Str("intermediate".into())),
            ],
        );

        monitor.evaluate(&input, &mut output).unwrap();

        assert_eq!(runtime_output(&monitor, &output, "before"), Value::Int(5));
    }
}

#[test]
fn dataflow_multiple_dynamics_reorder_atomically_when_dependencies_reverse() {
    let mut spec_src = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
                        a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let typed = type_check(spec.clone()).unwrap();
    for mut monitor in [
        DataflowMonitor::try_compile_untyped(spec).unwrap(),
        DataflowMonitor::try_compile_typed(typed).unwrap(),
    ] {
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];

        let first = runtime_input_row(
            &monitor,
            &[
                ("x", 10.into()),
                ("a_source", Value::Str("b + 1".into())),
                ("b_source", Value::Str("x".into())),
            ],
        );
        monitor.evaluate(&first, &mut output).unwrap();
        assert_eq!(runtime_output(&monitor, &output, "a"), Value::Int(11));
        assert_eq!(runtime_output(&monitor, &output, "b"), Value::Int(10));

        let second = runtime_input_row(
            &monitor,
            &[
                ("x", 20.into()),
                ("a_source", Value::Str("x".into())),
                ("b_source", Value::Str("a + 1".into())),
            ],
        );
        monitor.evaluate(&second, &mut output).unwrap();
        assert_eq!(runtime_output(&monitor, &output, "a"), Value::Int(20));
        assert_eq!(runtime_output(&monitor, &output, "b"), Value::Int(21));
    }
}

#[test]
fn dataflow_runtime_reordering_rolls_back_speculative_temporal_state() {
    let mut spec_src = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
                        a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];

    for (x, expected) in [(10, 0), (20, 10)] {
        let input = runtime_input_row(
            &monitor,
            &[
                ("x", x.into()),
                ("a_source", Value::Str("default(b[1], 0)".into())),
                ("b_source", Value::Str("x".into())),
            ],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(runtime_output(&monitor, &output, "a"), Value::Int(expected));
    }
}

#[test]
fn dataflow_runtime_dependency_cycles_are_terminal_errors() {
    let mut spec_src = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
                        a = dynamic(a_source: Int)\nb = dynamic(b_source: Int)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];
    let valid = runtime_input_row(
        &monitor,
        &[
            ("x", 1.into()),
            ("a_source", Value::Str("x".into())),
            ("b_source", Value::Str("a".into())),
        ],
    );
    monitor.evaluate(&valid, &mut output).unwrap();

    let cycle = runtime_input_row(
        &monitor,
        &[
            ("x", 2.into()),
            ("a_source", Value::Str("b".into())),
            ("b_source", Value::Str("a".into())),
        ],
    );
    assert!(matches!(
        monitor.evaluate(&cycle, &mut output),
        Err(DataflowEvalError::DynamicDependencyCycle(_))
    ));
    assert!(matches!(
        monitor.evaluate(&cycle, &mut output),
        Err(DataflowEvalError::MonitorFailed)
    ));
}

#[test]
fn dataflow_defer_reorders_once_and_ignores_later_definitions() {
    let mut spec_src = "in x: Int\nin a_source: Str\nin b_source: Str\nout a: Int\nout b: Int\n\
                        a = defer(a_source: Int)\nb = defer(b_source: Int)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];

    for (x, a_source, b_source, expected_a, expected_b) in
        [(10, "b + 1", "x", 11, 10), (20, "x", "a + 1", 21, 20)]
    {
        let input = runtime_input_row(
            &monitor,
            &[
                ("x", x.into()),
                ("a_source", Value::Str(a_source.into())),
                ("b_source", Value::Str(b_source.into())),
            ],
        );
        monitor.evaluate(&input, &mut output).unwrap();
        assert_eq!(
            runtime_output(&monitor, &output, "a"),
            Value::Int(expected_a)
        );
        assert_eq!(
            runtime_output(&monitor, &output, "b"),
            Value::Int(expected_b)
        );
    }
}

#[test]
fn dataflow_nested_dynamic_cannot_escape_parent_scope() {
    let mut spec_src =
        "in source: Str\nin secret: Int\nout z: Int\nz = dynamic(source: Int, {source})";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (
            VarName::new("source"),
            vec![Value::Str("dynamic(\"secret\": Int)".into())],
        ),
        (VarName::new("secret"), vec![42.into()]),
    ]);
    let columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var[var].clone())
        .collect::<Vec<_>>();
    let input = columns
        .iter()
        .map(|column| column[0].clone())
        .collect::<Vec<_>>();
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];

    assert!(matches!(
        monitor.evaluate(&input, &mut output),
        Err(DataflowEvalError::DynamicRestrictedContext(ref vars))
            if vars == &[VarName::new("secret")]
    ));
}

#[test]
fn dataflow_automatic_scope_does_not_add_unused_computed_dependencies() {
    let mut spec_src = "in x: Int\nin source: Str\nout dynamic_out: Int\naux downstream: Int\ndynamic_out = dynamic(source: Int)\ndownstream = dynamic_out + 1";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (VarName::new("x"), vec![1.into(), 2.into()]),
        (
            VarName::new("source"),
            vec![Value::Str("x".into()), Value::Str("x".into())],
        ),
    ]);
    let columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var[var].clone())
        .collect::<Vec<_>>();

    assert_eq!(
        evaluate(&mut monitor, &columns),
        vec![vec![1.into(), 2.into()]]
    );
}

#[test]
fn dataflow_evaluation_failures_are_returned() {
    let mut spec_src = "in source: Str\nout z: Int\nz = dynamic(source: Int)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let input = [Value::Str("not valid dsrv syntax (".into())];
    let mut output = vec![Value::NoVal; monitor.output_vars().len()];

    assert!(matches!(
        monitor.evaluate(&input, &mut output),
        Err(DataflowEvalError::DynamicParse { .. })
    ));
    assert!(matches!(
        monitor.evaluate(&input, &mut output),
        Err(DataflowEvalError::MonitorFailed)
    ));
}

#[apply(async_test)]
async fn dataflow_matches_arithmetic(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "(x + y) * 2 - 1",
        vec![
            ("x", vec![1.into(), 2.into(), 3.into(), 4.into()]),
            ("y", vec![10.into(), 20.into(), 30.into(), 40.into()]),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_lifts_sparse_binary_inputs_across_rows(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "x + y",
        vec![
            (
                "x",
                vec![Value::Int(1), Value::NoVal, Value::NoVal, Value::Int(4)],
            ),
            (
                "y",
                vec![Value::Int(10), Value::Int(20), Value::NoVal, Value::Int(40)],
            ),
        ],
    )
    .await;
}

#[test]
fn dataflow_event_batch_matches_sparse_rows_for_recursive_sum() {
    let mut spec_src =
        "in x: Int\nin y: Int\nout z: Int\nz = default(z[1], 0) + default(x, 0) + default(y, 0)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let spec = type_check(spec).unwrap();
    let mut row_monitor = DataflowMonitor::try_compile_typed(spec.clone()).unwrap();
    let mut event_monitor = DataflowMonitor::try_compile_typed(spec).unwrap();

    let rows = vec![
        vec![Value::Int(1), Value::NoVal],
        vec![Value::NoVal, Value::Int(10)],
        vec![Value::Int(2), Value::NoVal],
        vec![Value::NoVal, Value::Int(20)],
    ];
    let row_outputs = evaluate(
        &mut row_monitor,
        &[
            rows.iter().map(|row| row[0].clone()).collect(),
            rows.iter().map(|row| row[1].clone()).collect(),
        ],
    );
    let event_outputs = evaluate_events(
        &mut event_monitor,
        &[
            InputEvent::new(VarName::new("x"), Value::Int(1)),
            InputEvent::new(VarName::new("y"), Value::Int(10)),
            InputEvent::new(VarName::new("x"), Value::Int(2)),
            InputEvent::new(VarName::new("y"), Value::Int(20)),
        ],
    );

    assert_eq!(event_outputs, row_outputs);
}

#[test]
fn dataflow_lazy_if_propagates_initial_no_val_from_either_branch() {
    let mut spec_src = "in flag\nin good\nin bad\nout z\nz = if flag then good else bad";
    let spec = <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (
            VarName::new("flag"),
            vec![Value::Bool(true), Value::Bool(false)],
        ),
        (VarName::new("good"), vec![Value::Int(1), Value::NoVal]),
        (VarName::new("bad"), vec![Value::NoVal, Value::Int(2)]),
    ]);
    let input_columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var.get(var).cloned().unwrap())
        .collect::<Vec<_>>();

    let output = evaluate(&mut monitor, &input_columns);

    assert_eq!(output, vec![vec![Value::NoVal, Value::Int(2)]]);
}

#[test]
fn dataflow_lazy_if_reads_inputs_at_the_outer_tick() {
    let mut spec_src = "in flag\nin x\nout z\nz = if flag then x else 0";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    assert_eq!(
        evaluate(
            &mut monitor,
            &[
                vec![false.into(), true.into(), false.into(), true.into()],
                vec![10.into(), 20.into(), 30.into(), 40.into()],
            ]
        ),
        vec![vec![0.into(), 20.into(), 0.into(), 40.into()]]
    );
}

#[test]
fn dataflow_nested_dynamic_reads_the_shared_input_row() {
    let mut spec_src = "in flag: Bool\n\
                        in x: Int\n\
                        in s: Str\n\
                        out z: Int\n\
                        z = if flag then dynamic(s: Int) else x";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (
            VarName::new("flag"),
            vec![Value::Bool(false), Value::Bool(true)],
        ),
        (VarName::new("x"), vec![Value::Int(1), Value::Int(2)]),
        (
            VarName::new("s"),
            vec![Value::Str("x + 10".into()), Value::Str("x + 10".into())],
        ),
    ]);
    let input_columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var.get(var).cloned().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        evaluate(&mut monitor, &input_columns),
        vec![vec![Value::Int(1), Value::Int(12)]]
    );
}

#[test]
fn dataflow_lazy_if_allows_recursive_function_base_case() {
    let mut spec_src = "in n\n\
                        in bias\n\
                        out z\n\
                        z = fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)";
    let spec = <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let inputs_by_var = BTreeMap::from([
        (
            VarName::new("n"),
            vec![Value::Int(0), Value::Int(3), Value::Int(5)],
        ),
        (
            VarName::new("bias"),
            vec![Value::Int(10), Value::Int(10), Value::Int(2)],
        ),
    ]);
    let input_columns = monitor
        .input_vars()
        .iter()
        .map(|var| inputs_by_var.get(var).cloned().unwrap())
        .collect::<Vec<_>>();

    let output = evaluate(&mut monitor, &input_columns);

    assert_eq!(
        output,
        vec![vec![Value::Int(10), Value::Int(13), Value::Int(7)]]
    );
}

#[apply(async_test)]
async fn dataflow_matches_comparison_and_if(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "if x > 2 then x + 10 else y",
        vec![
            ("x", vec![1.into(), 2.into(), 3.into(), 4.into()]),
            ("y", vec![10.into(), 20.into(), 30.into(), 40.into()]),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_matches_list_operations(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "List.len(xs) + List.head(xs) + List.get(xs, 1)",
        vec![(
            "xs",
            vec![
                Value::List(vec![Value::Int(2), Value::Int(3)].into()),
                Value::List(vec![Value::Int(4), Value::Int(6), Value::Int(8)].into()),
            ],
        )],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_matches_map_operations(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        r#"Map.get(Map.insert(m, "z", x), "z") + Map.get(m, "a")"#,
        vec![
            ("x", vec![Value::Int(7), Value::Int(9)]),
            (
                "m",
                vec![
                    Value::Map(BTreeMap::from([("a".into(), Value::Int(1))])),
                    Value::Map(BTreeMap::from([("a".into(), Value::Int(2))])),
                ],
            ),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_matches_update(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "update(x, y)",
        vec![
            (
                "x",
                vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)],
            ),
            (
                "y",
                vec![Value::Deferred, Value::NoVal, Value::Int(30), Value::NoVal],
            ),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_matches_latch(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "latch(x, y)",
        vec![
            (
                "x",
                vec![Value::Int(1), Value::Int(2), Value::NoVal, Value::Int(4)],
            ),
            (
                "y",
                vec![
                    Value::NoVal,
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::NoVal,
                ],
            ),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_matches_dynamic(executor: Rc<LocalExecutor<'static>>) {
    let _ = executor;
    let mut spec_src = "in x: Int\nin y: Int\nin s: Str\nout z: Int\nz = dynamic(s: Int)";
    let spec = dsrv_specification(&mut spec_src).expect("spec should parse");
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let outputs = evaluate(
        &mut monitor,
        &[
            vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            vec![Value::Int(10), Value::Int(20), Value::Int(30)],
            vec![
                Value::Str("x + y".into()),
                Value::Str("x + y".into()),
                Value::Str("x * y".into()),
            ],
        ],
    );

    assert_eq!(
        outputs,
        vec![vec![Value::Int(11), Value::Int(22), Value::Int(90)]]
    );
}

#[apply(async_test)]
async fn dataflow_matches_defer(executor: Rc<LocalExecutor<'static>>) {
    let _ = executor;
    let mut spec_src = "in x: Int\nin s: Str\nout z: Int\nz = defer(s: Int)";
    let spec = dsrv_specification(&mut spec_src).expect("spec should parse");
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let outputs = evaluate(
        &mut monitor,
        &[
            vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            vec![Value::Deferred, Value::Str("x + 1".into()), Value::Deferred],
        ],
    );

    assert_eq!(
        outputs,
        vec![vec![Value::Deferred, Value::Int(3), Value::Int(4)]]
    );
}

#[test]
fn typed_and_untyped_dataflow_preserve_explicit_defer_scopes() {
    let mut source = "in x: Int\nin y: Int\nin s: Str\nout z: Int\nz = defer(s: Int, {x})";
    let spec = dsrv_specification(&mut source).expect("explicit defer scope should parse");
    let untyped = DataflowMonitor::try_compile_untyped(spec.clone())
        .expect("untyped explicit defer should compile");
    let typed = DataflowMonitor::try_compile_typed(
        type_check(spec).expect("typed explicit defer should type check"),
    )
    .expect("typed explicit defer should compile");

    for mut monitor in [untyped, typed] {
        let values = BTreeMap::from([
            (VarName::new("x"), Value::Int(1)),
            (VarName::new("y"), Value::Int(2)),
            (VarName::new("s"), Value::Str("y".into())),
        ]);
        let input = monitor
            .input_vars()
            .iter()
            .map(|var| values[var].clone())
            .collect::<Vec<_>>();
        let mut output = vec![Value::NoVal; monitor.output_vars().len()];
        assert!(
            monitor.evaluate(&input, &mut output).is_err(),
            "a deferred property must not access a variable outside its explicit scope"
        );
    }
}

#[apply(async_test)]
async fn dataflow_dynamic_temporal_dependency_starts_at_introduction(
    executor: Rc<LocalExecutor<'static>>,
) {
    let _ = executor;
    let mut spec_src = "in x: Int\nin s: Str\nout z: Int\nz = dynamic(s: Int)";
    let spec = dsrv_specification(&mut spec_src).expect("spec should parse");
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();
    let outputs = evaluate(
        &mut monitor,
        &[
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)],
            vec![
                Value::Deferred,
                Value::Deferred,
                Value::Str("x[1]".into()),
                Value::Str("x[1]".into()),
            ],
        ],
    );

    assert_eq!(
        outputs,
        vec![vec![
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
            Value::Int(3),
        ]]
    );
}

#[apply(async_test)]
async fn dataflow_semisync_defer_activation_and_redefinition_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let rows = assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)",
        BTreeMap::from([
            (
                VarName::new("x"),
                vec![
                    Value::Int(10),
                    Value::Int(11),
                    Value::Int(12),
                    Value::Int(13),
                    Value::Int(14),
                    Value::NoVal,
                    Value::Int(16),
                    Value::Int(17),
                ],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Deferred,
                    Value::NoVal,
                    Value::Str("x".into()),
                    Value::Str("x + 1".into()),
                    Value::Deferred,
                    Value::NoVal,
                    Value::Str("x * 2".into()),
                    Value::Str("x - 1".into()),
                ],
            ),
        ]),
    )
    .await;

    let actual = rows
        .iter()
        .map(|row| row[&VarName::new("z")].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            Value::Deferred,
            Value::Deferred,
            Value::Int(12),
            Value::Int(13),
            Value::Int(14),
            Value::Int(14),
            Value::Int(16),
            Value::Int(17),
        ]
    );
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_replacement_and_special_source_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let rows = assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (
                VarName::new("x"),
                vec![
                    Value::Int(10),
                    Value::Int(11),
                    Value::Int(12),
                    Value::Int(13),
                    Value::Int(14),
                    Value::Int(7),
                    Value::Int(8),
                ],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Str("x".into()),
                    Value::Str("x".into()),
                    Value::NoVal,
                    Value::Str("x + 1".into()),
                    Value::Str("x".into()),
                    Value::Str("x * 2".into()),
                    Value::Str("x * 2".into()),
                ],
            ),
        ]),
    )
    .await;

    let actual = rows
        .iter()
        .map(|row| row[&VarName::new("z")].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            Value::Int(10),
            Value::Int(11),
            Value::Int(12),
            Value::Int(14),
            Value::Int(14),
            Value::Int(14),
            Value::Int(16),
        ]
    );
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_temporal_state_across_no_val_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (
                VarName::new("x"),
                vec![
                    Value::Int(10),
                    Value::NoVal,
                    Value::Int(20),
                    Value::NoVal,
                    Value::Int(30),
                    Value::Int(40),
                    Value::NoVal,
                ],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Str("x[1]".into()),
                    Value::Str("x[1]".into()),
                    Value::NoVal,
                    Value::Str("x[2]".into()),
                    Value::Str("x[2]".into()),
                    Value::Deferred,
                    Value::Str("x[1]".into()),
                ],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_sparse_interdependent_outputs_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let x = vec![
        Value::Int(1),
        Value::NoVal,
        Value::Int(3),
        Value::NoVal,
        Value::Int(5),
        Value::NoVal,
    ];
    let y = vec![
        Value::NoVal,
        Value::Int(10),
        Value::NoVal,
        Value::Int(20),
        Value::NoVal,
        Value::Int(30),
    ];

    assert_dataflow_semisync_runtime_parity(
        executor.clone(),
        "in x: Int\nin y: Int\nin source: Str\n\
         out sum: Int\nout dynamic_value: Int\n\
         sum = x + y\n\
         dynamic_value = dynamic(source: Int)",
        BTreeMap::from([
            (VarName::new("x"), x.clone()),
            (VarName::new("y"), y.clone()),
            (
                VarName::new("source"),
                vec![
                    Value::Str("x".into()),
                    Value::NoVal,
                    Value::Str("y".into()),
                    Value::Deferred,
                    Value::Str("x + y".into()),
                    Value::NoVal,
                ],
            ),
        ]),
    )
    .await;

    assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin y: Int\nin source: Str\n\
         out sum: Int\nout deferred_value: Int\n\
         sum = x + y\n\
         deferred_value = defer(source: Int)",
        BTreeMap::from([
            (VarName::new("x"), x),
            (VarName::new("y"), y),
            (
                VarName::new("source"),
                vec![
                    Value::NoVal,
                    Value::Deferred,
                    Value::Str("x[1]".into()),
                    Value::Str("x".into()),
                    Value::NoVal,
                    Value::Deferred,
                ],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_all_no_val_and_single_tick_boundaries(
    executor: Rc<LocalExecutor<'static>>,
) {
    let all_no_val = vec![Value::NoVal; 5];
    assert_dataflow_semisync_runtime_parity(
        executor.clone(),
        "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (VarName::new("x"), all_no_val.clone()),
            (VarName::new("source"), all_no_val.clone()),
        ]),
    )
    .await;

    assert_dataflow_semisync_runtime_parity(
        executor.clone(),
        "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)",
        BTreeMap::from([
            (VarName::new("x"), all_no_val.clone()),
            (VarName::new("source"), all_no_val),
        ]),
    )
    .await;

    let single_tick = BTreeMap::from([
        (VarName::new("x"), vec![Value::Int(7)]),
        (VarName::new("source"), vec![Value::Str("x[1]".into())]),
    ]);
    assert_dataflow_semisync_runtime_parity(
        executor.clone(),
        "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        single_tick.clone(),
    )
    .await;
    assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin source: Str\nout z: Int\nz = defer(source: Int)",
        single_tick,
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_definition_arrival_by_stream_index_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let expressions = ["x", "x[0]", "x[1]", "x[2]", "default(x[2], 99)"];

    for operator in ["dynamic", "defer"] {
        for activation_tick in 0..4 {
            for expression in expressions {
                let mut sources = vec![Value::Deferred; 7];
                sources[activation_tick] = Value::Str(expression.into());
                sources[(activation_tick + 1)..].fill(Value::NoVal);
                let spec =
                    format!("in x: Int\nin source: Str\nout z: Int\nz = {operator}(source: Int)");
                assert_dataflow_semisync_runtime_parity(
                    executor.clone(),
                    &spec,
                    BTreeMap::from([
                        (
                            VarName::new("x"),
                            vec![
                                1.into(),
                                2.into(),
                                3.into(),
                                4.into(),
                                5.into(),
                                6.into(),
                                7.into(),
                            ],
                        ),
                        (VarName::new("source"), sources),
                    ]),
                )
                .await;
            }
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_sparse_history_by_stream_index_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for operator in ["dynamic", "defer"] {
        for expression in ["x[0]", "x[1]", "x[2]", "default(x[2], 99)"] {
            for presence_mask in 0_u8..16 {
                let x = (0..4)
                    .map(|tick| {
                        if presence_mask & (1 << tick) == 0 {
                            Value::NoVal
                        } else {
                            Value::Int(i64::from(tick + 1))
                        }
                    })
                    .collect::<Vec<_>>();
                let spec =
                    format!("in x: Int\nin source: Str\nout z: Int\nz = {operator}(source: Int)");
                assert_dataflow_semisync_runtime_parity(
                    executor.clone(),
                    &spec,
                    BTreeMap::from([
                        (VarName::new("x"), x),
                        (
                            VarName::new("source"),
                            vec![
                                Value::Str(expression.into()),
                                Value::NoVal,
                                Value::NoVal,
                                Value::NoVal,
                            ],
                        ),
                    ]),
                )
                .await;
            }
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_replacement_tick_and_history_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for replacement_tick in 1..6 {
        for replacement in ["y", "y[1]", "default(y[2], 77)"] {
            let mut sources = vec![Value::NoVal; 7];
            sources[0] = Value::Str("x[1]".into());
            sources[replacement_tick] = Value::Str(replacement.into());
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                "in x: Int\nin y: Int\nin source: Str\nout z: Int\n\
                 z = dynamic(source: Int)",
                BTreeMap::from([
                    (
                        VarName::new("x"),
                        vec![
                            1.into(),
                            Value::NoVal,
                            3.into(),
                            4.into(),
                            Value::NoVal,
                            6.into(),
                            7.into(),
                        ],
                    ),
                    (
                        VarName::new("y"),
                        vec![
                            10.into(),
                            20.into(),
                            Value::NoVal,
                            40.into(),
                            50.into(),
                            Value::NoVal,
                            70.into(),
                        ],
                    ),
                    (VarName::new("source"), sources),
                ]),
            )
            .await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_special_source_runs_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let tails = [
        vec![Value::NoVal, Value::NoVal, Value::NoVal],
        vec![Value::Deferred, Value::NoVal, Value::NoVal],
        vec![Value::NoVal, Value::Deferred, Value::NoVal],
        vec![Value::Deferred, Value::Deferred, Value::NoVal],
        vec![Value::NoVal, Value::NoVal, Value::Deferred],
        vec![Value::Deferred, Value::NoVal, Value::Deferred],
    ];

    for initial in [Value::NoVal, Value::Deferred, Value::Str("x".into())] {
        for tail in &tails {
            let mut sources = vec![initial.clone()];
            sources.extend(tail.iter().cloned());
            sources.push(Value::Str("x[1]".into()));
            sources.push(Value::NoVal);
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
                BTreeMap::from([
                    (
                        VarName::new("x"),
                        vec![
                            1.into(),
                            2.into(),
                            Value::NoVal,
                            4.into(),
                            5.into(),
                            Value::NoVal,
                        ],
                    ),
                    (VarName::new("source"), sources),
                ]),
            )
            .await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_compiled_expression_shapes_parity(executor: Rc<LocalExecutor<'static>>) {
    for operator in ["dynamic", "defer"] {
        for expression in [
            "x + y",
            "if flag then x else y",
            "if flag then x[1] else y[1]",
            "default(x[1], y)",
            "(\\v: Int -> v + 1)(x)",
            "(\\a: Int, b: Int -> a + b)(x, y)",
        ] {
            let spec = format!(
                "in x: Int\nin y: Int\nin flag: Bool\nin source: Str\nout z: Int\n\
                 z = {operator}(source: Int)"
            );
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                &spec,
                BTreeMap::from([
                    (
                        VarName::new("x"),
                        vec![1.into(), Value::NoVal, 3.into(), 4.into(), Value::NoVal],
                    ),
                    (
                        VarName::new("y"),
                        vec![10.into(), 20.into(), Value::NoVal, 40.into(), 50.into()],
                    ),
                    (
                        VarName::new("flag"),
                        vec![
                            true.into(),
                            false.into(),
                            Value::NoVal,
                            true.into(),
                            false.into(),
                        ],
                    ),
                    (
                        VarName::new("source"),
                        vec![
                            Value::Str(expression.into()),
                            Value::NoVal,
                            Value::NoVal,
                            Value::NoVal,
                            Value::NoVal,
                        ],
                    ),
                ]),
            )
            .await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_new_input_dependencies_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for replacement in ["y", "y[1]", "default(y[2], 77)", "x + y"] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), 2.into(), Value::NoVal, 4.into(), 5.into()],
                ),
                (
                    VarName::new("y"),
                    vec![10.into(), Value::NoVal, 30.into(), 40.into(), 50.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str("x".into()),
                        Value::NoVal,
                        Value::Str(replacement.into()),
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_defer_late_new_input_dependency_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for expression in ["y", "y[1]", "default(y[2], 77)"] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = defer(source: Int)",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
                ),
                (
                    VarName::new("y"),
                    vec![10.into(), Value::NoVal, 30.into(), 40.into(), 50.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Deferred,
                        Value::NoVal,
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_new_computed_dependency_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for replacement in ["intermediate", "intermediate[1]"] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin source: Str\nout z: Int\naux intermediate: Int\n\
             intermediate = x + 1\n\
             z = dynamic(source: Int, {x, intermediate})",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), 2.into(), Value::NoVal, 4.into(), 5.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str("x".into()),
                        Value::NoVal,
                        Value::Str(replacement.into()),
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_stateful_compiled_operators_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for operator in ["dynamic", "defer"] {
        for expression in [
            "update(x, y)",
            "latch(x, flag)",
            "default(x[2], y)",
            "if when(x) then 1 else 0",
        ] {
            let spec = format!(
                "in x: Int\nin y: Int\nin flag: Bool\nin source: Str\nout z: Int\n\
                 z = {operator}(source: Int)"
            );
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                &spec,
                BTreeMap::from([
                    (
                        VarName::new("x"),
                        vec![
                            1.into(),
                            Value::NoVal,
                            3.into(),
                            4.into(),
                            Value::NoVal,
                            6.into(),
                        ],
                    ),
                    (
                        VarName::new("y"),
                        vec![
                            Value::Deferred,
                            Value::NoVal,
                            30.into(),
                            Value::NoVal,
                            50.into(),
                            60.into(),
                        ],
                    ),
                    (
                        VarName::new("flag"),
                        vec![
                            Value::NoVal,
                            true.into(),
                            Value::NoVal,
                            false.into(),
                            true.into(),
                            Value::NoVal,
                        ],
                    ),
                    (
                        VarName::new("source"),
                        vec![
                            Value::Deferred,
                            Value::Str(expression.into()),
                            Value::NoVal,
                            Value::NoVal,
                            Value::NoVal,
                            Value::NoVal,
                        ],
                    ),
                ]),
            )
            .await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_switch_away_and_back_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    let choices = [
        Value::NoVal,
        Value::Deferred,
        Value::Str("x[1]".into()),
        Value::Str("y[1]".into()),
    ];
    for sequence in 0_usize..64 {
        let mut encoded = sequence;
        let mut sources = vec![Value::Str("x[1]".into())];
        for _ in 0..3 {
            sources.push(choices[encoded % choices.len()].clone());
            encoded /= choices.len();
        }
        sources.push(Value::Str("x[1]".into()));
        sources.push(Value::NoVal);
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![
                        1.into(),
                        Value::NoVal,
                        3.into(),
                        4.into(),
                        5.into(),
                        Value::NoVal,
                    ],
                ),
                (
                    VarName::new("y"),
                    vec![
                        10.into(),
                        20.into(),
                        Value::NoVal,
                        40.into(),
                        Value::NoVal,
                        60.into(),
                    ],
                ),
                (VarName::new("source"), sources),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_nested_runtime_compilation_parity(executor: Rc<LocalExecutor<'static>>) {
    for outer in ["dynamic", "defer"] {
        let spec =
            format!("in x: Int\nin inner: Str\nin outer: Str\nout z: Int\nz = {outer}(outer: Int)");
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            &spec,
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![
                        1.into(),
                        2.into(),
                        Value::NoVal,
                        4.into(),
                        5.into(),
                        6.into(),
                    ],
                ),
                (
                    VarName::new("inner"),
                    vec![
                        Value::Str("x".into()),
                        Value::NoVal,
                        Value::Str("x[1]".into()),
                        Value::NoVal,
                        Value::Deferred,
                        Value::Str("x + 1".into()),
                    ],
                ),
                (
                    VarName::new("outer"),
                    vec![
                        Value::Str("dynamic(inner: Int)".into()),
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_paired_sparse_inputs_across_expression_shapes_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for x_mask in 0_u8..16 {
        let y_mask = x_mask.rotate_left(2) ^ 0b0101;
        let values = |mask: u8, scale: i64| {
            (0..4)
                .map(|tick| {
                    if mask & (1 << tick) == 0 {
                        Value::NoVal
                    } else {
                        Value::Int(scale * i64::from(tick + 1))
                    }
                })
                .collect::<Vec<_>>()
        };
        for expression in [
            "x + y",
            "if flag then x else y",
            "(\\a: Int, b: Int -> a + b)(x, y)",
        ] {
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                "in x: Int\nin y: Int\nin flag: Bool\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
                BTreeMap::from([
                    (VarName::new("x"), values(x_mask, 1)),
                    (VarName::new("y"), values(y_mask, 10)),
                    (VarName::new("flag"), vec![true.into(), false.into(), Value::NoVal, true.into()]),
                    (VarName::new("source"), vec![Value::Str(expression.into()), Value::NoVal, Value::NoVal, Value::NoVal]),
                ]),
            ).await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_semisync_dynamic_result_domains_parity(executor: Rc<LocalExecutor<'static>>) {
    let cases = [
        (
            "in x: Bool\nin y: Bool\nin source: Str\nout z: Bool\nz = dynamic(source: Bool)",
            "x && y",
            vec![true.into(), Value::NoVal, false.into(), true.into()],
            vec![false.into(), true.into(), Value::NoVal, true.into()],
        ),
        (
            "in x: Str\nin y: Str\nin source: Str\nout z: Str\nz = dynamic(source: Str)",
            "x ++ y",
            vec!["a".into(), Value::NoVal, "c".into(), "d".into()],
            vec!["1".into(), "2".into(), Value::NoVal, "4".into()],
        ),
    ];

    for (spec, expression, x, y) in cases {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            spec,
            BTreeMap::from([
                (VarName::new("x"), x),
                (VarName::new("y"), y),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::Deferred,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }

    assert_dataflow_semisync_runtime_parity(
        executor,
        "in xs: List<Int>\nin x: Int\nin source: Str\nout z: List<Int>\nz = dynamic(source: List<Int>)",
        BTreeMap::from([
            (
                VarName::new("xs"),
                vec![
                    Value::List(vec![1.into()].into()),
                    Value::NoVal,
                    Value::List(vec![3.into()].into()),
                    Value::List(vec![4.into()].into()),
                ],
            ),
            (VarName::new("x"), vec![10.into(), 20.into(), Value::NoVal, 40.into()]),
            (
                VarName::new("source"),
                vec![Value::Str("List.append(xs, x)".into()), Value::NoVal, Value::NoVal, Value::NoVal],
            ),
        ]),
    ).await;
}

#[apply(async_test)]
async fn dataflow_semisync_collection_operator_lifting_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for expression in [
        "List(x, y)",
        "List.concat(xs, ys)",
        "List.tail(xs)",
        "List.map(\\v: Int -> v + 1, xs)",
        "List.filter(\\v: Int -> v > 1, xs)",
    ] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin y: Int\nin xs: List<Int>\nin ys: List<Int>\nin source: Str\n\
             out z: List<Int>\nz = dynamic(source: List<Int>)",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), Value::NoVal, 3.into(), 4.into()],
                ),
                (
                    VarName::new("y"),
                    vec![10.into(), 20.into(), Value::NoVal, 40.into()],
                ),
                (
                    VarName::new("xs"),
                    vec![
                        Value::List(vec![1.into(), 2.into()].into()),
                        Value::NoVal,
                        Value::List(vec![3.into(), 4.into()].into()),
                        Value::List(vec![4.into(), 5.into()].into()),
                    ],
                ),
                (
                    VarName::new("ys"),
                    vec![
                        Value::List(vec![10.into()].into()),
                        Value::List(vec![20.into()].into()),
                        Value::NoVal,
                        Value::List(vec![40.into()].into()),
                    ],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }

    assert_dataflow_semisync_runtime_parity(
        executor,
        "in xs: List<Int>\nin initial: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (
                VarName::new("xs"),
                vec![
                    Value::List(vec![1.into(), 2.into()].into()),
                    Value::NoVal,
                    Value::List(vec![3.into()].into()),
                    Value::List(vec![4.into()].into()),
                ],
            ),
            (
                VarName::new("initial"),
                vec![10.into(), 20.into(), Value::NoVal, 40.into()],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Str("List.fold(\\a: Int, v: Int -> a + v, initial, xs)".into()),
                    Value::NoVal,
                    Value::NoVal,
                    Value::NoVal,
                ],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_map_operator_lifting_parity(executor: Rc<LocalExecutor<'static>>) {
    for expression in ["Map.insert(m, \"z\", x)", "Map.remove(m, \"a\")"] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in m: Map<Int>\nin x: Int\nin source: Str\nout z: Map<Int>\n\
             z = dynamic(source: Map<Int>)",
            BTreeMap::from([
                (
                    VarName::new("m"),
                    vec![
                        Value::Map(BTreeMap::from([("a".into(), 1.into())])),
                        Value::NoVal,
                        Value::Map(BTreeMap::from([("a".into(), 3.into())])),
                        Value::Map(BTreeMap::from([("a".into(), 4.into())])),
                    ],
                ),
                (
                    VarName::new("x"),
                    vec![10.into(), 20.into(), Value::NoVal, 40.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }

    assert_dataflow_semisync_runtime_parity(
        executor,
        "in m: Map<Int>\nin x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (
                VarName::new("m"),
                vec![
                    Value::Map(BTreeMap::from([("a".into(), 1.into())])),
                    Value::NoVal,
                    Value::Map(BTreeMap::from([("a".into(), 3.into())])),
                    Value::Map(BTreeMap::from([("a".into(), 4.into())])),
                ],
            ),
            (
                VarName::new("x"),
                vec![10.into(), 20.into(), Value::NoVal, 40.into()],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Str("Map.get(m, \"a\") + x".into()),
                    Value::NoVal,
                    Value::NoVal,
                    Value::NoVal,
                ],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_partial_application_parity(executor: Rc<LocalExecutor<'static>>) {
    for expression in ["partial(\\a: Int, b: Int -> a + b, x)(y)"] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in x: Int\nin y: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), Value::NoVal, 3.into(), 4.into()],
                ),
                (
                    VarName::new("y"),
                    vec![10.into(), 20.into(), Value::NoVal, 40.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_collection_access_lifting_parity(executor: Rc<LocalExecutor<'static>>) {
    for expression in [
        "List.get(xs, index) + x",
        "List.head(xs) + x",
        "List.len(xs) + x",
    ] {
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            "in xs: List<Int>\nin index: Int\nin x: Int\nin source: Str\nout z: Int\n\
             z = dynamic(source: Int)",
            BTreeMap::from([
                (
                    VarName::new("xs"),
                    vec![
                        Value::List(vec![1.into(), 2.into()].into()),
                        Value::NoVal,
                        Value::List(vec![3.into(), 4.into()].into()),
                        Value::List(vec![4.into(), 5.into()].into()),
                    ],
                ),
                (
                    VarName::new("index"),
                    vec![0.into(), 1.into(), Value::NoVal, 0.into()],
                ),
                (
                    VarName::new("x"),
                    vec![10.into(), 20.into(), Value::NoVal, 40.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Str(expression.into()),
                        Value::NoVal,
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_map_predicate_lifting_parity(executor: Rc<LocalExecutor<'static>>) {
    assert_dataflow_semisync_runtime_parity(
        executor,
        "in m: Map<Int>\nin source: Str\nout z: Bool\nz = dynamic(source: Bool)",
        BTreeMap::from([
            (
                VarName::new("m"),
                vec![
                    Value::Map(BTreeMap::from([("a".into(), 1.into())])),
                    Value::NoVal,
                    Value::Map(BTreeMap::new()),
                    Value::NoVal,
                ],
            ),
            (
                VarName::new("source"),
                vec![
                    Value::Str("Map.has_key(m, \"a\")".into()),
                    Value::NoVal,
                    Value::NoVal,
                    Value::NoVal,
                ],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_semisync_stateful_helper_presence_matrix_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for presence_mask in 0_u8..16 {
        let sparse_ints = |scale: i64| {
            (0..4)
                .map(|tick| {
                    if presence_mask & (1 << tick) == 0 {
                        Value::NoVal
                    } else {
                        Value::Int(scale * i64::from(tick + 1))
                    }
                })
                .collect::<Vec<_>>()
        };
        for expression in [
            "update(x, replacement)",
            "default(x, replacement)",
            "init(x, replacement)",
        ] {
            assert_dataflow_semisync_runtime_parity(
                executor.clone(),
                "in x: Int\nin replacement: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
                BTreeMap::from([
                    (VarName::new("x"), sparse_ints(1)),
                    (VarName::new("replacement"), vec![Value::Deferred, 20.into(), Value::NoVal, 40.into()]),
                    (VarName::new("source"), vec![Value::Str(expression.into()), Value::NoVal, Value::NoVal, Value::NoVal]),
                ]),
            ).await;
        }
    }
}

#[apply(async_test)]
async fn dataflow_dynamic_recursive_function_uses_lazy_base_case(
    executor: Rc<LocalExecutor<'static>>,
) {
    let mut source =
        "in n: Int\nin bias: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let spec = dsrv_specification(&mut source).expect("recursive dynamic spec should parse");
    let rows = eval_dataflow_runtime(
        executor,
        spec,
        BTreeMap::from([
            (VarName::new("n"), vec![0.into(), 2.into(), Value::NoVal, 1.into()]),
            (VarName::new("bias"), vec![10.into(), 20.into(), 30.into(), Value::NoVal]),
            (
                VarName::new("source"),
                vec![
                    Value::Str("fix(\\self: (Int -> Int), k: Int -> if k == 0 then bias else self(k - 1) + 1)(n)".into()),
                    Value::NoVal,
                    Value::NoVal,
                    Value::NoVal,
                ],
            ),
        ]),
        4,
    )
    .await;

    assert_eq!(
        rows,
        vec![
            BTreeMap::from([(VarName::new("z"), Value::Int(10))]),
            BTreeMap::from([(VarName::new("z"), Value::Int(22))]),
            BTreeMap::from([(VarName::new("z"), Value::Int(32))]),
            BTreeMap::from([(VarName::new("z"), Value::Int(31))]),
        ]
    );
}

#[apply(async_test)]
async fn dataflow_multiple_runtime_compiled_outputs_complete_each_tick(
    executor: Rc<LocalExecutor<'static>>,
) {
    let mut source = "in x: Int\nin y: Int\nin left_source: Str\nin right_source: Str\n\
         out left: Int\nout right: Int\n\
         left = dynamic(left_source: Int)\n\
         right = defer(right_source: Int)";
    let spec = dsrv_specification(&mut source).expect("multiple DUP spec should parse");
    let rows = eval_dataflow_runtime(
        executor,
        spec,
        BTreeMap::from([
            (
                VarName::new("x"),
                vec![1.into(), Value::NoVal, 3.into(), 4.into()],
            ),
            (
                VarName::new("y"),
                vec![10.into(), 20.into(), Value::NoVal, 40.into()],
            ),
            (
                VarName::new("left_source"),
                vec![
                    Value::Str("x".into()),
                    Value::NoVal,
                    Value::Str("y".into()),
                    Value::NoVal,
                ],
            ),
            (
                VarName::new("right_source"),
                vec![
                    Value::Deferred,
                    Value::Str("y".into()),
                    Value::NoVal,
                    Value::Deferred,
                ],
            ),
        ]),
        4,
    )
    .await;

    assert_eq!(
        rows,
        vec![
            BTreeMap::from([
                (VarName::new("left"), Value::Int(1)),
                (VarName::new("right"), Value::Deferred),
            ]),
            BTreeMap::from([
                (VarName::new("left"), Value::Int(1)),
                (VarName::new("right"), Value::Int(20)),
            ]),
            BTreeMap::from([
                (VarName::new("left"), Value::Int(20)),
                (VarName::new("right"), Value::Int(20)),
            ]),
            BTreeMap::from([
                (VarName::new("left"), Value::Int(40)),
                (VarName::new("right"), Value::Int(40)),
            ]),
        ]
    );
}

#[apply(async_test)]
async fn dataflow_semisync_computed_property_source_parity(executor: Rc<LocalExecutor<'static>>) {
    for operator in ["dynamic", "defer"] {
        let application = if operator == "dynamic" {
            "dynamic(property: Int, {x, y, property})"
        } else {
            "defer(property: Int)"
        };
        let spec = format!(
            "in x: Int\nin y: Int\nin choose: Bool\nin left: Str\nin right: Str\n\
             out z: Int\naux property: Str\n\
             property = if choose then left else right\n\
             z = {application}"
        );
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            &spec,
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), Value::NoVal, 3.into(), 4.into(), 5.into()],
                ),
                (
                    VarName::new("y"),
                    vec![10.into(), 20.into(), Value::NoVal, 40.into(), 50.into()],
                ),
                (
                    VarName::new("choose"),
                    vec![
                        true.into(),
                        Value::NoVal,
                        false.into(),
                        true.into(),
                        false.into(),
                    ],
                ),
                (
                    VarName::new("left"),
                    vec![
                        "x".into(),
                        Value::NoVal,
                        "x[1]".into(),
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
                (
                    VarName::new("right"),
                    vec![
                        "y".into(),
                        Value::NoVal,
                        "y[1]".into(),
                        Value::NoVal,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_downstream_of_runtime_compiled_output_parity(
    executor: Rc<LocalExecutor<'static>>,
) {
    for operator in ["dynamic", "defer"] {
        let spec = format!(
            "in x: Int\nin source: Str\nout dynamic_value: Int\nout downstream: Int\n\
             dynamic_value = {operator}(source: Int)\n\
             downstream = default(dynamic_value[1], 0) + 1"
        );
        assert_dataflow_semisync_runtime_parity(
            executor.clone(),
            &spec,
            BTreeMap::from([
                (
                    VarName::new("x"),
                    vec![1.into(), Value::NoVal, 3.into(), 4.into(), 5.into()],
                ),
                (
                    VarName::new("source"),
                    vec![
                        Value::Deferred,
                        Value::Str("x".into()),
                        Value::NoVal,
                        Value::Deferred,
                        Value::NoVal,
                    ],
                ),
            ]),
        )
        .await;
    }
}

#[apply(async_test)]
async fn dataflow_semisync_new_property_uses_current_global_tick(
    executor: Rc<LocalExecutor<'static>>,
) {
    assert_dataflow_semisync_runtime_parity(
        executor,
        "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)",
        BTreeMap::from([
            (VarName::new("x"), vec![1.into(), Value::NoVal, 3.into()]),
            (
                VarName::new("source"),
                vec![Value::Deferred, Value::Str("x".into()), Value::NoVal],
            ),
        ]),
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_async_new_property_uses_current_global_tick(
    executor: Rc<LocalExecutor<'static>>,
) {
    let mut source = "in x: Int\nin source: Str\nout z: Int\nz = dynamic(source: Int)";
    let spec = dsrv_specification(&mut source).expect("async parity specification should parse");
    let inputs = BTreeMap::from([
        (VarName::new("x"), vec![1.into(), Value::NoVal, 3.into()]),
        (
            VarName::new("source"),
            vec![Value::Deferred, Value::Str("x".into()), Value::NoVal],
        ),
    ]);
    let dataflow = eval_dataflow_runtime(executor.clone(), spec.clone(), inputs.clone(), 3).await;
    let asynchronous =
        eval_runtime_with::<UntimedDsrvSemantics<LALRParser>>(executor, spec, inputs, 3).await;
    assert_eq!(dataflow, asynchronous);
}

#[apply(async_test)]
async fn dataflow_matches_sindex_default(executor: Rc<LocalExecutor<'static>>) {
    assert_matches_current(
        executor,
        "default(x[1], 0) + y",
        vec![
            ("x", vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()]),
            (
                "y",
                vec![10.into(), 20.into(), 30.into(), 40.into(), 50.into()],
            ),
        ],
    )
    .await;
}

#[apply(async_test)]
async fn dataflow_runtime_matches_recursive_accumulator(executor: Rc<LocalExecutor<'static>>) {
    let mut spec_src = "in x\nout z\nz = default(z[1], 0) + x";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let inputs = BTreeMap::from([(
        VarName::new("x"),
        vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
    )]);

    let expected = eval_runtime_with::<UntimedDsrvSemantics<LALRParser>>(
        executor.clone(),
        spec.clone(),
        inputs.clone(),
        5,
    )
    .await;
    let actual = eval_dataflow_spec(spec, inputs);

    assert_eq!(actual, expected);
    assert_eq!(
        actual
            .iter()
            .map(|values| values[&VarName::new("z")].clone())
            .collect::<Vec<_>>(),
        vec![1.into(), 3.into(), 6.into(), 10.into(), 15.into()]
    );
}

#[apply(async_test)]
async fn dataflow_stateful_lifting_matches_current(executor: Rc<LocalExecutor<'static>>) {
    let vars = vec![(
        "x",
        vec![
            Value::Int(1),
            Value::NoVal,
            Value::Deferred,
            Value::NoVal,
            Value::Int(4),
        ],
    )];

    for expression in ["default(x, 0)", "is_defined(x)", "when(x)"] {
        let expected = eval_with::<UntimedDsrvSemantics<LALRParser>>(
            executor.clone(),
            expression,
            vars.clone(),
        )
        .await;
        let actual = eval_dataflow(expression, vars.clone());
        assert_eq!(actual, expected, "dataflow differed for `{expression}`");
    }
}

#[test]
fn dataflow_lifting_does_not_depend_on_recursive_plan_shape() {
    let mut spec_src = "in x\nout z\nz = if false then z[1] else default(x, 42)";
    let spec = dsrv_specification(&mut spec_src).unwrap();
    let mut monitor = DataflowMonitor::try_compile_untyped(spec).unwrap();

    let output = evaluate(
        &mut monitor,
        &[vec![
            Value::Int(1),
            Value::NoVal,
            Value::Deferred,
            Value::NoVal,
            Value::Int(4),
        ]],
    );

    assert_eq!(
        output,
        vec![vec![
            Value::Int(1),
            Value::Int(1),
            Value::Int(42),
            Value::Int(42),
            Value::Int(4),
        ]]
    );
}
