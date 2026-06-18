use approx::assert_abs_diff_eq;
use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::core::{
    Runtime, RuntimeSpec, Semantics, Specification, StreamType, StreamTypeAscription,
};
use trustworthiness_checker::io::file::FileInputProvider;
use trustworthiness_checker::io::map::MapInputProvider;
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::lang::dsrv::type_checker::{type_check, type_check_gradual};
use trustworthiness_checker::lang::untimed_input::untimed_input_file;
use trustworthiness_checker::runtime::builder::GeneralRuntimeBuilder;
use trustworthiness_checker::{
    SExpr, Value, dsrv_specification, parse_file, runtime::RuntimeBuilder,
};
use trustworthiness_checker::{UntypedDsrvSpecification, dsrv_fixtures::*};
use trustworthiness_checker::{VarName, async_test};

#[derive(Debug, Clone, Copy, PartialEq)]
enum TestConfiguration {
    AsyncUntimed,
    AsyncTypedUntimed,
    AsyncGradualTypedUntimed,
    SemiSyncUntimed,
    SemiSyncTypedUntimed,
    SemiSyncGradualTypedUntimed,
}

impl TestConfiguration {
    fn all() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
            TestConfiguration::AsyncGradualTypedUntimed,
            TestConfiguration::SemiSyncUntimed,
            TestConfiguration::SemiSyncTypedUntimed,
            TestConfiguration::SemiSyncGradualTypedUntimed,
        ]
    }

    fn async_configurations() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
            TestConfiguration::AsyncGradualTypedUntimed,
            TestConfiguration::SemiSyncUntimed,
            TestConfiguration::SemiSyncTypedUntimed,
            TestConfiguration::SemiSyncGradualTypedUntimed,
        ]
    }

    fn untyped_configurations() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::SemiSyncUntimed,
        ]
    }
}

fn create_builder_from_config(
    builder: GeneralRuntimeBuilder<UntypedDsrvSpecification, Value>,
    config: TestConfiguration,
) -> GeneralRuntimeBuilder<UntypedDsrvSpecification, Value> {
    match config {
        TestConfiguration::AsyncUntimed => {
            let builder = builder.runtime(RuntimeSpec::Async);
            builder.semantics(Semantics::Untimed)
        }
        TestConfiguration::AsyncTypedUntimed => {
            let builder = builder.runtime(RuntimeSpec::Async);
            builder.semantics(Semantics::TypedUntimed)
        }
        TestConfiguration::AsyncGradualTypedUntimed => {
            let builder = builder.runtime(RuntimeSpec::Async);
            builder.semantics(Semantics::GradualTypedUntimed)
        }
        TestConfiguration::SemiSyncUntimed => builder
            .runtime(RuntimeSpec::SemiSync)
            .semantics(Semantics::Untimed),
        TestConfiguration::SemiSyncTypedUntimed => builder
            .runtime(RuntimeSpec::SemiSync)
            .semantics(Semantics::TypedUntimed),
        TestConfiguration::SemiSyncGradualTypedUntimed => builder
            .runtime(RuntimeSpec::SemiSync)
            .semantics(Semantics::GradualTypedUntimed),
    }
}

async fn run_typed_runtime_with_spec(
    executor: Rc<LocalExecutor<'static>>,
    runtime: RuntimeSpec,
    spec_str: &str,
    input_streams: BTreeMap<VarName, Vec<Value>>,
    timeout_label: &'static str,
) -> anyhow::Result<Vec<(usize, BTreeMap<VarName, Value>)>> {
    let mut spec_input = spec_str;
    let spec = dsrv_specification(&mut spec_input).unwrap();
    let input_streams = MapInputProvider::new(input_streams);
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars(),
    ));
    let outputs = output_handler.get_output();

    let monitor = GeneralRuntimeBuilder::new()
        .executor(executor.clone())
        .model(spec)
        .input(Box::new(input_streams))
        .output(output_handler)
        .runtime(runtime)
        .semantics(Semantics::TypedUntimed)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();
    with_timeout(outputs.enumerate().collect(), 5, timeout_label).await
}

async fn run_typed_runtime(
    executor: Rc<LocalExecutor<'static>>,
    spec_str: &str,
    input_streams: BTreeMap<VarName, Vec<Value>>,
) -> anyhow::Result<Vec<(usize, BTreeMap<VarName, Value>)>> {
    run_typed_runtime_with_spec(
        executor,
        RuntimeSpec::Async,
        spec_str,
        input_streams,
        "typed runtime outputs.collect()",
    )
    .await
}

async fn run_typed_semisync_runtime(
    executor: Rc<LocalExecutor<'static>>,
    spec_str: &str,
    input_streams: BTreeMap<VarName, Vec<Value>>,
) -> anyhow::Result<Vec<(usize, BTreeMap<VarName, Value>)>> {
    run_typed_runtime_with_spec(
        executor,
        RuntimeSpec::SemiSync,
        spec_str,
        input_streams,
        "typed semisync runtime outputs.collect()",
    )
    .await
}

async fn run_gradual_typed_runtime_with_spec(
    executor: Rc<LocalExecutor<'static>>,
    runtime: RuntimeSpec,
    spec_str: &str,
    input_streams: BTreeMap<VarName, Vec<Value>>,
    timeout_label: &'static str,
) -> anyhow::Result<Vec<(usize, BTreeMap<VarName, Value>)>> {
    let mut spec_input = spec_str;
    let spec = dsrv_specification(&mut spec_input).unwrap();
    let input_streams = MapInputProvider::new(input_streams);
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars(),
    ));
    let outputs = output_handler.get_output();

    let monitor = GeneralRuntimeBuilder::new()
        .executor(executor.clone())
        .model(spec)
        .input(Box::new(input_streams))
        .output(output_handler)
        .runtime(runtime)
        .semantics(Semantics::GradualTypedUntimed)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();
    with_timeout(outputs.enumerate().collect(), 5, timeout_label).await
}

fn expected_dsrv_future_window_outputs(size: usize) -> Vec<(usize, BTreeMap<VarName, Value>)> {
    (0..size)
        .map(|idx| {
            let value = if idx < 2 {
                Value::Deferred
            } else {
                let current_window_is_above_threshold =
                    (idx - 2..=idx).all(|window_idx| window_idx % 8 != 3);
                Value::Bool(current_window_is_above_threshold)
            };
            (idx, BTreeMap::from([(VarName::new("always_x"), value)]))
        })
        .collect()
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_output_can_reference_aux_variable(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in x: Float
out always_x: Bool
aux above: Bool
above = x > 3.0
always_x = above && default(above[1], true) && default(above[2], true)
"#;
    let size = 16usize;
    let input = || {
        BTreeMap::from([(
            VarName::new("x"),
            (0..size)
                .map(|idx| Value::Float(if idx % 8 == 3 { 2.0 } else { 5.0 }))
                .collect::<Vec<_>>(),
        )])
    };
    let expected = (0..size)
        .map(|idx| {
            let value = (idx.saturating_sub(2)..=idx).all(|window_idx| window_idx % 8 != 3);
            (
                idx,
                BTreeMap::from([(VarName::new("always_x"), Value::Bool(value))]),
            )
        })
        .collect::<Vec<_>>();

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            input(),
            "gradual typed aux output runtime outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs, expected,
            "unexpected aux-referencing output values for runtime {runtime:?}",
        );
    }

    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_future_window_matches_benchmark(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in x: Float
out always_x: Bool
always_x = (x > 3.0) && (x[1] > 3.0) && (x[2] > 3.0)
"#;
    let size = 16;
    let input = || {
        BTreeMap::from([(
            VarName::new("x"),
            (0..size)
                .map(|idx| Value::Float(if idx % 8 == 3 { 2.0 } else { 5.0 }))
                .collect::<Vec<_>>(),
        )])
    };
    let expected = expected_dsrv_future_window_outputs(size);

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            input(),
            "gradual typed future window benchmark outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs, expected,
            "unexpected future-window benchmark outputs for runtime {runtime:?}",
        );
    }

    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_infers_unannotated_output(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // The output `z` has no type annotation, so this spec is rejected by the
    // strict typed-untimed semantics but accepted under gradual typing, where
    // `z` is inferred as Int from its definition.
    let spec = "in x: Int\nout z\nz = x + 1";

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            BTreeMap::from([("x".into(), vec![0.into(), 1.into(), 2.into()])]),
            "gradual typed runtime outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(1))])),
                (1, BTreeMap::from([("z".into(), Value::Int(2))])),
                (2, BTreeMap::from([("z".into(), Value::Int(3))])),
            ],
            "unexpected gradual typed outputs for runtime {runtime:?}",
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_casts_untyped_input(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // The input `x` has no annotation, so it is treated as Any and cast to Int
    // at runtime where it is used in an integer addition.
    let spec = "in x\nout z\nz = x + 1";

    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("x".into(), vec![41.into(), 1.into()])]),
        "gradual typed any input outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("z".into(), Value::Int(42))])),
            (1, BTreeMap::from([("z".into(), Value::Int(2))])),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_accepts_explicit_any_annotations(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Explicit Any annotations should parse and execute through the same
    // runtime-checked path as inferred Any expressions.
    let spec = "in x: Any\nin y: Any\nout z: Any\nz = x + y";

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            BTreeMap::from([
                ("x".into(), vec![Value::Int(1), Value::Int(2)]),
                ("y".into(), vec![Value::Int(3), Value::Float(0.5)]),
            ]),
            "gradual typed explicit Any outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(4))])),
                (1, BTreeMap::from([("z".into(), Value::Float(2.5))])),
            ],
            "unexpected gradual explicit Any outputs for runtime {runtime:?}",
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_accepts_any_inside_list_annotation(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Any should also be accepted inside container annotations. This verifies
    // parsing and runtime validation of a heterogeneous List<Any>.
    let spec = "in xs: List<Any>\nout ys: List<Any>\nys = xs";

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            BTreeMap::from([(
                "xs".into(),
                vec![Value::List(
                    vec![Value::Int(1), Value::Str("robot".into())].into(),
                )],
            )]),
            "gradual typed List<Any> outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![(
                0,
                BTreeMap::from([(
                    "ys".into(),
                    Value::List(vec![Value::Int(1), Value::Str("robot".into())].into())
                )]),
            )],
            "unexpected gradual List<Any> outputs for runtime {runtime:?}",
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_evaluates_any_plus_any(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Both inputs and the output are unannotated, so the expression remains
    // Any + Any after type checking. The typed runtime must therefore evaluate
    // the operation through the dynamic runtime path and check/dispatch against
    // the actual Value types at each tick.
    let spec = "in x\nin y\nout z\nz = x + y";

    for runtime in [RuntimeSpec::Async, RuntimeSpec::SemiSync] {
        let outputs = run_gradual_typed_runtime_with_spec(
            executor.clone(),
            runtime,
            spec,
            BTreeMap::from([
                ("x".into(), vec![Value::Int(1), Value::Int(2)]),
                ("y".into(), vec![Value::Int(3), Value::Float(0.5)]),
            ]),
            "gradual typed any+any outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(4))])),
                (1, BTreeMap::from([("z".into(), Value::Float(2.5))])),
            ],
            "unexpected gradual Any + Any outputs for runtime {runtime:?}",
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_respects_explicit_annotations(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Fully annotated specs should behave identically under gradual semantics.
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec_simple_add_monitor_typed(),
        BTreeMap::from([
            ("x".into(), vec![0.into(), 1.into(), 2.into()]),
            ("y".into(), vec![3.into(), 4.into(), 5.into()]),
        ]),
        "gradual typed annotated outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("z".into(), Value::Int(3))])),
            (1, BTreeMap::from([("z".into(), Value::Int(5))])),
            (2, BTreeMap::from([("z".into(), Value::Int(7))])),
        ]
    );
    Ok(())
}

#[test]
fn test_gradual_type_check_accepts_spec_rejected_by_strict() {
    // Missing annotations are a hard error for the strict checker, but the
    // gradual checker infers them.
    let mut spec_str = "in gsx\nout gsz\ngsz = gsx + 1";
    let spec = dsrv_specification(&mut spec_str).unwrap();
    assert!(
        type_check(spec.clone()).is_err(),
        "strict checker should reject missing annotations"
    );
    type_check_gradual(spec).expect("gradual checker should accept the spec");
}

#[test]
fn test_gradual_type_check_rejects_annotated_mismatch() {
    // A concrete static mismatch must remain an error under gradual typing.
    let mut spec_str = "out gbz: Bool\ngbz = 1 + 1";
    let spec = dsrv_specification(&mut spec_str).unwrap();
    assert!(type_check_gradual(spec).is_err());
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_inference_chain_out_of_order(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // `gzb` is declared (and therefore visited) before `gza`, but depends on
    // it; gradual inference must resolve the chain via its fixed-point pass.
    let spec = "in gx: Int\nout gzb\nout gza\ngzb = gza + 1\ngza = gx + 1";
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("gx".into(), vec![0.into(), 1.into(), 2.into()])]),
        "gradual chain outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([("gza".into(), Value::Int(1)), ("gzb".into(), Value::Int(2)),])
            ),
            (
                1,
                BTreeMap::from([("gza".into(), Value::Int(2)), ("gzb".into(), Value::Int(3)),])
            ),
            (
                2,
                BTreeMap::from([("gza".into(), Value::Int(3)), ("gzb".into(), Value::Int(4)),])
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_dyn_comparisons(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // `gd` is untyped (Any); comparisons against an Int literal must cast it
    // to Int at runtime and produce Bool outputs.
    let spec = "in gd\nout geq\nout gle\ngeq = gd == 10\ngle = gd <= 10";
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("gd".into(), vec![10.into(), 3.into(), 42.into()])]),
        "gradual any comparison outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    ("geq".into(), Value::Bool(true)),
                    ("gle".into(), Value::Bool(true)),
                ])
            ),
            (
                1,
                BTreeMap::from([
                    ("geq".into(), Value::Bool(false)),
                    ("gle".into(), Value::Bool(true)),
                ])
            ),
            (
                2,
                BTreeMap::from([
                    ("geq".into(), Value::Bool(false)),
                    ("gle".into(), Value::Bool(false)),
                ])
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_dyn_float_arithmetic(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // An untyped input used in float arithmetic is cast to Float at runtime
    // and the output is inferred as Float.
    let spec = "in gf\nout gg\ngg = gf + 1.5";
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("gf".into(), vec![1.0.into(), 2.5.into()])]),
        "gradual any float outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("gg".into(), Value::Float(2.5))])),
            (1, BTreeMap::from([("gg".into(), Value::Float(4.0))])),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_dyn_string_concat(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // An untyped input used in string concatenation is cast to Str at runtime
    // and the output is inferred as Str.
    let spec = "in gs\nout gt\ngt = gs ++ \"!\"";
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("gs".into(), vec!["a".into(), "b".into()])]),
        "gradual any string outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("gt".into(), Value::Str("a!".into()))])),
            (1, BTreeMap::from([("gt".into(), Value::Str("b!".into()))])),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_gradual_typed_runtime_if_expression_inference(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // The unannotated output is inferred as Int from both branches of the
    // conditional expression.
    let spec = "in gc: Bool\nout gz\ngz = if gc then 1 else 2";
    let outputs = run_gradual_typed_runtime_with_spec(
        executor.clone(),
        RuntimeSpec::Async,
        spec,
        BTreeMap::from([("gc".into(), vec![true.into(), false.into()])]),
        "gradual if inference outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("gz".into(), Value::Int(1))])),
            (1, BTreeMap::from([("gz".into(), Value::Int(2))])),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_typed_semisync_runtime_simple_add(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let outputs = run_typed_semisync_runtime(
        executor,
        spec_simple_add_monitor_typed(),
        BTreeMap::from([
            ("x".into(), vec![0.into(), 1.into(), 2.into()]),
            ("y".into(), vec![3.into(), 4.into(), 5.into()]),
        ]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (0, BTreeMap::from([("z".into(), Value::Int(3))])),
            (1, BTreeMap::from([("z".into(), Value::Int(5))])),
            (2, BTreeMap::from([("z".into(), Value::Int(7))])),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_typed_semisync_runtime_struct_and_list_access(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out robot: Struct<id: Int, flags: List<Bool>>
out id: Int
out active: Bool
robot = Struct("id": tick + 10, "flags": List(tick > 0, tick == 2))
id = robot.id
active = List.get(robot.flags, 0)
"#;

    let outputs = run_typed_semisync_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![1.into(), 2.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    ("active".into(), Value::Bool(true)),
                    ("id".into(), Value::Int(11)),
                    (
                        "robot".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(false)].into()),
                            ),
                            ("id".into(), Value::Int(11)),
                        ])),
                    ),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    ("active".into(), Value::Bool(true)),
                    ("id".into(), Value::Int(12)),
                    (
                        "robot".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(true)].into()),
                            ),
                            ("id".into(), Value::Int(12)),
                        ])),
                    ),
                ]),
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_typed_runtime_object_literals_assign_to_maps_and_structs(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out objmap: Map<Int>
out objstruct: Struct<id: Int, label: Str, ...>
out mapvalue: Int
out structid: Int
objmap = {"x": tick, "y": tick + 1}
objstruct = {"id": tick + 10, "label": "robot", "model": "extra"}
mapvalue = Map.get(objmap, "y")
structid = objstruct.id
"#;

    let outputs = run_typed_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![1.into(), 2.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    ("mapvalue".into(), Value::Int(2)),
                    (
                        "objmap".into(),
                        Value::Map(BTreeMap::from([
                            ("x".into(), Value::Int(1)),
                            ("y".into(), Value::Int(2)),
                        ])),
                    ),
                    (
                        "objstruct".into(),
                        Value::Map(BTreeMap::from([
                            ("id".into(), Value::Int(11)),
                            ("label".into(), Value::Str("robot".into())),
                        ])),
                    ),
                    ("structid".into(), Value::Int(11)),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    ("mapvalue".into(), Value::Int(3)),
                    (
                        "objmap".into(),
                        Value::Map(BTreeMap::from([
                            ("x".into(), Value::Int(2)),
                            ("y".into(), Value::Int(3)),
                        ])),
                    ),
                    (
                        "objstruct".into(),
                        Value::Map(BTreeMap::from([
                            ("id".into(), Value::Int(12)),
                            ("label".into(), Value::Str("robot".into())),
                        ])),
                    ),
                    ("structid".into(), Value::Int(12)),
                ]),
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_file_input_json_object_with_extra_fields_assigns_to_permissive_typed_struct(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in payload: Struct<x: Int, y: Int, ...>
out selected: Int
out echoed: Struct<x: Int, y: Int, ...>
selected = Map.get(payload, "x")
echoed = payload
"#;
    let mut spec_input = spec;
    let spec = dsrv_specification(&mut spec_input).unwrap();
    let input_data = parse_file(untimed_input_file, "fixtures/object_literal_extra.input").await?;
    let input_provider = FileInputProvider::new(input_data);
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars(),
    ));
    let outputs = output_handler.get_output();

    let monitor = GeneralRuntimeBuilder::new()
        .executor(executor.clone())
        .model(spec)
        .input(Box::new(input_provider))
        .output(output_handler)
        .runtime(RuntimeSpec::Async)
        .semantics(Semantics::TypedUntimed)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();
    let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
        outputs.enumerate().collect(),
        5,
        "typed file input object literal outputs.collect()",
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    (
                        "echoed".into(),
                        Value::Map(BTreeMap::from([
                            ("extra".into(), Value::Int(99)),
                            ("x".into(), Value::Int(10)),
                            ("y".into(), Value::Int(20)),
                        ])),
                    ),
                    ("selected".into(), Value::Int(10)),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    (
                        "echoed".into(),
                        Value::Map(BTreeMap::from([
                            ("extra".into(), Value::Int(100)),
                            ("x".into(), Value::Int(30)),
                            ("y".into(), Value::Int(40)),
                        ])),
                    ),
                    ("selected".into(), Value::Int(30)),
                ]),
            ),
        ]
    );
    Ok(())
}

#[test]
fn test_typed_object_literal_struct_fields_are_checked() {
    let mut ok_extra_fields = r#"
out robot: Struct<id: Int, ...>
robot = {"id": 1, "label": "robot"}
"#;
    let spec = dsrv_specification(&mut ok_extra_fields).unwrap();
    type_check(spec)
        .expect("object literal with extra fields should type-check as a permissive struct");

    let cases = [
        (
            r#"
out robot: Struct<id: Int>
robot = {"id": 1, "label": "robot"}
"#,
            "unknown fields",
        ),
        (
            r#"
out robot: Struct<id: Int, label: Str>
robot = {"id": 1}
"#,
            "missing required field",
        ),
        (
            r#"
out robot: Struct<id: Int, label: Str>
robot = {"id": "not an int", "label": "robot"}
"#,
            "has type Str, expected Int",
        ),
        (
            r#"
out robot: Struct<id: Int, ...>
robot = {"id": 1, "label": unknown}
"#,
            "undeclared variable",
        ),
        (
            r#"
out robot: Struct<id: Int>
robot = Struct("id": 1, "label": "robot")
"#,
            "unknown fields",
        ),
        (
            r#"
out robot: Struct<id: Int>
out label: Str
robot = Struct("id": 1)
label = robot.label
"#,
            "Struct has no field",
        ),
        (
            r#"
out values: Map<Int>
out x: Int
values = Map("x": 1)
x = values.x
"#,
            "Dot field access requires a Struct",
        ),
    ];

    for (spec_src, expected_error) in cases {
        let mut spec_src = spec_src;
        let spec = dsrv_specification(&mut spec_src).unwrap();
        let errors = type_check(spec).expect_err("object literal should not type-check");
        assert!(
            errors
                .iter()
                .any(|err| format!("{err:?}").contains(expected_error)),
            "Expected error containing {expected_error:?}, got {errors:?}",
        );
    }
}

#[test]
fn test_typed_container_and_struct_operations_type_check() {
    let cases = [
        r#"
out structs: List<Struct<id: Int>>
out first: Struct<id: Int>
out first_id: Int
structs = List(Struct("id": 1))
first = List.head(structs)
first_id = first.id
"#,
        r#"
out nested: Struct<pose: Struct<x: Int>>
out x: Int
nested = Struct("pose": Struct("x": 1))
x = nested.pose.x
"#,
        r#"
out records: Map<Struct<id: Int>>
out record: Struct<id: Int>
out id: Int
records = Map("a": Struct("id": 1))
record = Map.get(records, "a")
id = record.id
"#,
        r#"
out s: Struct<id: Int>
s = default(Struct("id": 1), Struct("id": 2))
"#,
        r#"
out s: Struct<id: Int>
s = if true then Struct("id": 1) else Struct("id": 2)
"#,
        r#"
out s: Struct<id: Int>
s = init(Struct("id": 1), Struct("id": 2))
"#,
        r#"
out s: Struct<id: Int>
s = update(Struct("id": 1), Struct("id": 2))
"#,
        r#"
out s: Struct<id: Int>
s = latch(Struct("id": 1), Struct("id": 2))
"#,
    ];

    for spec_src in cases {
        let mut spec_src = spec_src;
        let spec = dsrv_specification(&mut spec_src).unwrap();
        type_check(spec).unwrap_or_else(|errors| {
            panic!("expected spec to type-check, got {errors:?}\n{spec_src}")
        });
    }

    let mut ctx = BTreeMap::new();
    for (expr, expected) in [
        (
            SExpr::Dynamic(
                Box::new(SExpr::Val(Value::Str("Map(\"x\": 1)".into()))),
                StreamTypeAscription::Ascribed(StreamType::Map(Box::new(StreamType::Int))),
            ),
            StreamType::Map(Box::new(StreamType::Int)),
        ),
        (
            SExpr::Dynamic(
                Box::new(SExpr::Val(Value::Str("Struct(\"id\": 1)".into()))),
                StreamTypeAscription::Ascribed(StreamType::Struct(
                    vec![("id".into(), StreamType::Int)].into(),
                    false,
                )),
            ),
            StreamType::Struct(vec![("id".into(), StreamType::Int)].into(), false),
        ),
        (
            SExpr::Defer(
                Box::new(SExpr::Val(Value::Str("Map(\"x\": 1)".into()))),
                StreamTypeAscription::Ascribed(StreamType::Map(Box::new(StreamType::Int))),
                vec![].into(),
            ),
            StreamType::Map(Box::new(StreamType::Int)),
        ),
        (
            SExpr::Defer(
                Box::new(SExpr::Val(Value::Str("Struct(\"id\": 1)".into()))),
                StreamTypeAscription::Ascribed(StreamType::Struct(
                    vec![("id".into(), StreamType::Int)].into(),
                    false,
                )),
                vec![].into(),
            ),
            StreamType::Struct(vec![("id".into(), StreamType::Int)].into(), false),
        ),
    ] {
        use trustworthiness_checker::lang::dsrv::type_checker::TypeCheckableHelper;
        let mut errors = vec![];
        expr.type_check_raw(Some(&expected), &mut ctx, &mut errors)
            .unwrap_or_else(|_| {
                panic!("expected dynamic/defer expression to type-check, got {errors:?}")
            });
    }
}

#[apply(async_test)]
async fn test_typed_runtime_struct_literal_get_and_update(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out robot: Struct<id: Int, sensor_value: Int, name: Str, flags: List<Bool>>
out id: Int
out sensor_value: Int
out active: Bool
out updated: Struct<id: Int, sensor_value: Int, name: Str, flags: List<Bool>>
robot = Struct("id": tick, "sensor_value": tick + 100, "name": "r2d2", "flags": List(tick > 0, tick == 2))
id = robot.id
sensor_value = robot.sensor_value
active = List.get(robot.flags, 0)
updated = Map.insert(robot, "id", tick + 10)
"#;

    let outputs = run_typed_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![1.into(), 2.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    ("active".into(), Value::Bool(true)),
                    ("id".into(), Value::Int(1)),
                    (
                        "robot".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(false)].into()),
                            ),
                            ("id".into(), Value::Int(1)),
                            ("name".into(), Value::Str("r2d2".into())),
                            ("sensor_value".into(), Value::Int(101)),
                        ])),
                    ),
                    ("sensor_value".into(), Value::Int(101)),
                    (
                        "updated".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(false)].into()),
                            ),
                            ("id".into(), Value::Int(11)),
                            ("name".into(), Value::Str("r2d2".into())),
                            ("sensor_value".into(), Value::Int(101)),
                        ])),
                    ),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    ("active".into(), Value::Bool(true)),
                    ("id".into(), Value::Int(2)),
                    (
                        "robot".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(true)].into()),
                            ),
                            ("id".into(), Value::Int(2)),
                            ("name".into(), Value::Str("r2d2".into())),
                            ("sensor_value".into(), Value::Int(102)),
                        ])),
                    ),
                    ("sensor_value".into(), Value::Int(102)),
                    (
                        "updated".into(),
                        Value::Map(BTreeMap::from([
                            (
                                "flags".into(),
                                Value::List(vec![Value::Bool(true), Value::Bool(true)].into()),
                            ),
                            ("id".into(), Value::Int(12)),
                            ("name".into(), Value::Str("r2d2".into())),
                            ("sensor_value".into(), Value::Int(102)),
                        ])),
                    ),
                ]),
            ),
        ]
    );
    Ok(())
}

#[test]
fn test_typed_semantics_reject_struct_constructor_assigned_to_map() {
    let mut spec_str = r#"
out robot: Map<Int>
robot = Struct("id": 7)
"#;
    let spec = dsrv_specification(&mut spec_str).unwrap();

    let errors = type_check(spec).expect_err("Struct constructor should not type-check as a Map");
    assert!(
        errors.iter().any(|err| format!("{err:?}")
            .contains("Struct constructor requires an expected Struct type")),
        "Expected a struct-constructor/map-assignment type error, got {errors:?}",
    );
}

#[apply(async_test)]
async fn test_untyped_runtime_struct_constructor_can_be_assigned_to_map(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        let mut spec_str = r#"
in tick
out robot
out id
robot = Struct("id": tick + 7, "active": true)
id = Map.get(robot, "id")
"#;
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("tick".into(), vec![0.into()])]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);
        let monitor = create_builder_from_config(builder, config).build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.enumerate().collect(),
            5,
            "untyped struct constructor runtime outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![(
                0,
                BTreeMap::from([
                    ("id".into(), Value::Int(7)),
                    (
                        "robot".into(),
                        Value::Map(BTreeMap::from([
                            ("active".into(), Value::Bool(true)),
                            ("id".into(), Value::Int(7)),
                        ])),
                    ),
                ]),
            )],
            "Untyped struct constructor output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_untyped_runtime_struct_like_map_input_and_update(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        let mut spec_str = r#"
in robot
out id
out active
out renamed
id = Map.get(robot, "id")
active = Map.get(robot, "active")
renamed = Map.insert(robot, "name", "bb8")
"#;
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();
        let input_streams = MapInputProvider::new(BTreeMap::from([(
            "robot".into(),
            vec![
                Value::Map(BTreeMap::from([
                    ("active".into(), Value::Bool(true)),
                    ("id".into(), Value::Int(7)),
                    ("name".into(), Value::Str("r2d2".into())),
                ])),
                Value::Map(BTreeMap::from([
                    ("active".into(), Value::Bool(false)),
                    ("id".into(), Value::Int(8)),
                    ("name".into(), Value::Str("c3po".into())),
                ])),
            ],
        )]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);
        let monitor = create_builder_from_config(builder, config).build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.enumerate().collect(),
            5,
            "untyped struct-like runtime outputs.collect()",
        )
        .await?;

        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    BTreeMap::from([
                        ("active".into(), Value::Bool(true)),
                        ("id".into(), Value::Int(7)),
                        (
                            "renamed".into(),
                            Value::Map(BTreeMap::from([
                                ("active".into(), Value::Bool(true)),
                                ("id".into(), Value::Int(7)),
                                ("name".into(), Value::Str("bb8".into())),
                            ])),
                        ),
                    ]),
                ),
                (
                    1,
                    BTreeMap::from([
                        ("active".into(), Value::Bool(false)),
                        ("id".into(), Value::Int(8)),
                        (
                            "renamed".into(),
                            Value::Map(BTreeMap::from([
                                ("active".into(), Value::Bool(false)),
                                ("id".into(), Value::Int(8)),
                                ("name".into(), Value::Str("bb8".into())),
                            ])),
                        ),
                    ]),
                ),
            ],
            "Untyped struct-like output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_typed_runtime_nested_list_operations(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out head: List<Int>
out elt: Int
head = List.head(List(List(tick, tick + 1), List(tick + 2, tick + 3)))
elt = List.get(List.head(List(List(tick, tick + 1), List(tick + 2, tick + 3))), 1)
"#;

    let outputs = run_typed_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![10.into(), 20.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    (
                        "head".into(),
                        Value::List(vec![Value::Int(10), Value::Int(11)].into()),
                    ),
                    ("elt".into(), Value::Int(11)),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    (
                        "head".into(),
                        Value::List(vec![Value::Int(20), Value::Int(21)].into()),
                    ),
                    ("elt".into(), Value::Int(21)),
                ]),
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_typed_runtime_map_get_nested_list_value(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out selected: List<Bool>
out hasflags: Bool
selected = Map.get(Map("flags": List(tick == 1, tick == 2)), "flags")
hasflags = Map.has_key(Map("flags": List(true)), "flags")
"#;

    let outputs = run_typed_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![1.into(), 2.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([
                    (
                        "selected".into(),
                        Value::List(vec![Value::Bool(true), Value::Bool(false)].into()),
                    ),
                    ("hasflags".into(), Value::Bool(true)),
                ]),
            ),
            (
                1,
                BTreeMap::from([
                    (
                        "selected".into(),
                        Value::List(vec![Value::Bool(false), Value::Bool(true)].into()),
                    ),
                    ("hasflags".into(), Value::Bool(true)),
                ]),
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_typed_runtime_map_of_nested_list_output(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let spec = r#"
in tick: Int
out nested: Map<List<Int>>
nested = Map.insert(Map(), "xs", List(tick, tick + 1))
"#;

    let outputs = run_typed_runtime(
        executor,
        spec,
        BTreeMap::from([("tick".into(), vec![3.into(), 4.into()])]),
    )
    .await?;

    assert_eq!(
        outputs,
        vec![
            (
                0,
                BTreeMap::from([(
                    "nested".into(),
                    Value::Map(BTreeMap::from([(
                        "xs".into(),
                        Value::List(vec![Value::Int(3), Value::Int(4)].into()),
                    )])),
                )]),
            ),
            (
                1,
                BTreeMap::from([(
                    "nested".into(),
                    Value::Map(BTreeMap::from([(
                        "xs".into(),
                        Value::List(vec![Value::Int(4), Value::Int(5)].into()),
                    )])),
                )]),
            ),
        ]
    );
    Ok(())
}

#[apply(async_test)]
async fn test_defer(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Defer test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 2.into())])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
            "Defer output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![1.into(), 2.into(), 3.into()];
        let e = vec!["x * x".into(), "x * x + 1".into(), "x * x + 2".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Defer x squared test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 4.into())])),
                (2, BTreeMap::from([("z".into(), 9.into())])),
            ],
            "Defer x squared output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_deferred(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![1.into(), 2.into(), 3.into()];
        let e = vec![Value::Deferred, "x + 1".into(), "x + 2".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Defer deferred test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), 3.into())])),
                (2, BTreeMap::from([("z".into(), 4.into())])),
            ],
            "Defer deferred output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_deferred2(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec![Value::Deferred, "x + 1".into(), Value::Deferred];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Defer deferred2 test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), 2.into())])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
            "Defer deferred2 output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_dependency(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin y: Int\nin e: Str\nout z1: Int\nout z2: Int\nz1 = defer(e : Int)\nz2 = x + y";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![1.into(), 2.into(), 3.into(), 4.into()];
        let y = vec![10.into(), 20.into(), 30.into(), 40.into()];
        let e = vec![
            Value::Deferred,
            "x + y".into(),
            "x + y".into(),
            "x + y".into(),
        ];
        let input_streams = MapInputProvider::new(BTreeMap::from([
            ("x".into(), x),
            ("y".into(), y),
            ("e".into(), e),
        ]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            4,
            "Defer dependency test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    BTreeMap::from([("z1".into(), Value::Deferred), ("z2".into(), 11.into())])
                ),
                (
                    1,
                    BTreeMap::from([("z1".into(), 22.into()), ("z2".into(), 22.into())])
                ),
                (
                    2,
                    BTreeMap::from([("z1".into(), 33.into()), ("z2".into(), 33.into())])
                ),
                (
                    3,
                    BTreeMap::from([("z1".into(), 44.into()), ("z2".into(), 44.into())])
                ),
            ],
            "Defer dependency output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_update_both_init(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    // TODO: This test only runs on untyped configurations due to update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = dsrv_specification(&mut "in x\nin y\nout z\nz = update(x, y)").unwrap();

        let x = vec!["x0".into(), "x1".into(), "x2".into()];
        let y = vec!["y0".into(), "y1".into(), "y2".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Update both init test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), "y0".into())])),
                (1, BTreeMap::from([("z".into(), "y1".into())])),
                (2, BTreeMap::from([("z".into(), "y2".into())])),
            ],
            "Update both init output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_update_first_x_then_y(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    // TODO: This test only runs on untyped configurations due to update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = dsrv_specification(&mut "in x\nin y\nout z\nz = update(x, y)").unwrap();

        let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
        let y = vec![Value::Deferred, "y1".into(), Value::Deferred, "y3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            4,
            "Update first x then y test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), "x0".into())])),
                (1, BTreeMap::from([("z".into(), "y1".into())])),
                (2, BTreeMap::from([("z".into(), Value::Deferred)])),
                (3, BTreeMap::from([("z".into(), "y3".into())])),
            ],
            "Update first x then y output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_update_defer(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    // TODO: This test only works on untyped_configurations
    for config in TestConfiguration::untyped_configurations() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let spec_untyped =
            dsrv_specification(&mut "in x\nin e\nout z\nz = update(\"def\", defer(e))").unwrap();

        let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
        let e = vec![Value::Deferred, "x".into(), "x".into(), "x".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            4,
            "Update defer test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), "def".into())])),
                (1, BTreeMap::from([("z".into(), "x1".into())])),
                (2, BTreeMap::from([("z".into(), "x2".into())])),
                (3, BTreeMap::from([("z".into(), "x3".into())])),
            ],
            "Update defer output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_update(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    // TODO: This test only runs on constraints due to defer/update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let spec_untyped =
            dsrv_specification(&mut "in x\nin y\nout z\nz = defer(update(x, y))").unwrap();

        let x = vec![Value::Deferred, "x".into(), "x_lost".into(), "x_sad".into()];
        let y = vec![
            Value::Deferred,
            "y".into(),
            "y_won!".into(),
            "y_happy".into(),
        ];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            4,
            "Defer update test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), "y".into())])),
                (2, BTreeMap::from([("z".into(), "y_won!".into())])),
                (3, BTreeMap::from([("z".into(), "y_happy".into())])),
            ],
            "Defer update output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

// #[apply(async_test)]
// async fn test_recursive_update(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
//     // TODO: This test only works on the constraint-based runtime
//     for config in vec![TestConfiguration::Constraints] {
//         let spec_untyped = dsrv_specification(&mut "in x\nout z\nz = update(x, z))").unwrap();
//
//             let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
//             let input_streams = BTreeMap::from([("x".into(), x)]);
//             let mut output_handler = Box::new(ManualOutputHandler::new(
//                 executor.clone(),
//                 spec_untyped.output_vars(),
//             ));
//             let outputs = output_handler.get_output();
//
//             let builder = RuntimeBuilder::new()
//                 .executor(executor.clone())
//                 .model(spec_untyped.clone())
//                 .input(Box::new(input_streams))
//                 .output(output_handler);
//
//             let builder = create_builder_from_config(builder, config);
//
//             let monitor = builder.build();
//
//             executor.spawn(monitor.run()).detach();
//             let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
//             assert_eq!(
//                 outputs.len(),
//                 4,
//                 "Recursive update test failed for config {:?}",
//                 config,
//             );
//             assert_eq!(
//                 outputs,
//                 vec![
//                     (0, vec!["x0".into()]),
//                     (1, vec!["x1".into()]),
//                     (2, vec!["x2".into()]),
//                     (3, vec!["x3".into()]),
//                 ],
//                 "Recursive update output mismatch for config {:?}",
//                 config,
//             );
//         }
// }

// NOTE: While this test is interesting, it cannot work due to how defer is handled.
// When defer receives a prop stream it changes state from being a defer expression into
// the received prop stream. Thus, it cannot be used recursively.
// This is the reason why we also need dynamic for the constraint based runtime.
// #[apply(async_test)]
// async fn test_recursive_update_defer(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
//     // TODO: This test only runs on the constraint-based runtime due to update/defer functionality
//     // limitations
//     for config in vec![TestConfiguration::Constraints] {
//         let spec_untyped = dsrv_specification(&mut "in x\nout z\nz = update(defer(x), z)").unwrap();
//
//             let x = vec!["0".into(), "1".into(), "2".into(), "3".into()];
//             let input_streams = BTreeMap::from([("x".into(), x)]);
//             let mut output_handler = Box::new(ManualOutputHandler::new(
//                 executor.clone(),
//                 spec_untyped.output_vars(),
//             ));
//             let outputs = output_handler.get_output();
//
//             let builder = RuntimeBuilder::new()
//                 .executor(executor.clone())
//                 .model(spec_untyped.clone())
//                 .input(Box::new(input_streams))
//                 .output(output_handler);
//
//             let builder = create_builder_from_config(builder, config);
//
//             let monitor = builder.build();
//
//             executor.spawn(monitor.run()).detach();
//             let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
//             assert_eq!(
//                 outputs.len(),
//                 4,
//                 "Recursive update defer test failed for config {:?}",
//                 config,
//             );
//             assert_eq!(
//                 outputs,
//                 vec![
//                     (0, vec![0.into()]),
//                     (1, vec![0.into()],),
//                     (2, vec![0.into()],),
//                     (3, vec![0.into()],),
//                 ],
//                 "Recursive update defer output mismatch for config {:?}",
//                 config,
//             );
//         }
// }

pub fn input_streams_constraint() -> MapInputProvider {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        vec![Value::Int(1), Value::Int(3), Value::Int(5)],
    );
    input_streams.insert(
        "y".into(),
        vec![Value::Int(2), Value::Int(4), Value::Int(6)],
    );
    MapInputProvider::new(input_streams)
}

#[ignore = "Cannot have empty spec or inputs"]
#[apply(async_test)]
async fn test_runtime_initialization(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let spec_untyped = dsrv_specification(&mut spec_empty()).unwrap();

        let input_streams = input_empty();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            0,
            "Runtime initialization failed for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_var(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = x";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Variable test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 3.into())])),
                (2, BTreeMap::from([("z".into(), 5.into())])),
            ],
            "Variable test output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_literal_expression(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "out z: Int\nz = 42";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.take(3).enumerate().collect(),
            5,
            "outputs.collect()",
        )
        .await?;
        assert_eq!(
            outputs.len(),
            3,
            "Literal expression test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 42.into())])),
                (1, BTreeMap::from([("z".into(), 42.into())])),
                (2, BTreeMap::from([("z".into(), 42.into())])),
            ],
            "Literal expression output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_addition(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = x + 1";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Addition test failed for config {:?}",
            config,
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 2.into())])),
                (1, BTreeMap::from([("z".into(), 4.into())])),
                (2, BTreeMap::from([("z".into(), 6.into())])),
            ],
            "Addition output mismatch for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_subtraction(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = x - 10";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Subtraction test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(-9))])),
                (1, BTreeMap::from([("z".into(), Value::Int(-7))])),
                (2, BTreeMap::from([("z".into(), Value::Int(-5))])),
            ],
            "Subtraction output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_index_past_mult_dependencies(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z1: Int\nout z2: Int\nz2 = x[2]\nz1 = x[1]";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            4,
            "Index past mult dependencies test failed for config {:?}",
            config
        );
        assert_eq!(
            &outputs[0..3],
            vec![
                (
                    0,
                    BTreeMap::from([
                        ("z1".into(), Value::Deferred),
                        ("z2".into(), Value::Deferred)
                    ])
                ),
                (
                    1,
                    BTreeMap::from([("z1".into(), 1.into()), ("z2".into(), Value::Deferred)])
                ),
                (
                    2,
                    BTreeMap::from([("z1".into(), 3.into()), ("z2".into(), 1.into())])
                ),
            ],
            "Index past mult dependencies output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_if_else_expression(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Bool\nin y: Bool\nout z: Bool\nz = if(x) then y else false";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams5();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "If-else expression test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), true.into())])),
                (1, BTreeMap::from([("z".into(), false.into())])),
                (2, BTreeMap::from([("z".into(), false.into())])),
            ],
            "If-else expression output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_string_append(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Str\nin y: Str\nout z: Str\nz = x ++ y";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams4();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            2,
            "String append test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), "ab".into())])),
                (1, BTreeMap::from([("z".into(), "cd".into())])),
            ],
            "String append output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_default_no_deferred(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = default(x, 42)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Default no deferred test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 3.into())])),
                (2, BTreeMap::from([("z".into(), 5.into())])),
            ],
            "Default no deferred output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_default_all_deferred(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = default(x, 42)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = MapInputProvider::new(BTreeMap::from([(
            "x".into(),
            vec![Value::Deferred, Value::Deferred, Value::Deferred],
        )]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Default all deferred test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 42.into())])),
                (1, BTreeMap::from([("z".into(), 42.into())])),
                (2, BTreeMap::from([("z".into(), 42.into())])),
            ],
            "Default all deferred output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_default_one_deferred(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nout z: Int\nz = default(x, 42)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = MapInputProvider::new(BTreeMap::from([(
            "x".into(),
            vec![1.into(), Value::Deferred, 5.into()],
        )]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        assert_eq!(
            outputs.len(),
            3,
            "Default one deferred test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 42.into())])),
                (2, BTreeMap::from([("z".into(), 5.into())])),
            ],
            "Default one deferred output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_counter(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "out x: Int\nx = 1 + default(x[1], 0)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_empty();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.take(4).enumerate().collect(),
            5,
            "outputs.collect()",
        )
        .await?;
        assert_eq!(
            outputs.len(),
            4,
            "Counter test failed for config {:?}",
            config
        );
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("x".into(), 1.into())])),
                (1, BTreeMap::from([("x".into(), 2.into())])),
                (2, BTreeMap::from([("x".into(), 3.into())])),
                (3, BTreeMap::from([("x".into(), 4.into())])),
            ],
            "Counter output mismatch for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_simple_add_monitor_does_not_go_away(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        // Common specification for all configurations
        let spec_untyped = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams1();

        // Test that monitor continues to work even after output handler goes out of scope
        let outputs = {
            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = GeneralRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler);

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);
            let monitor = builder.build().await;

            // Start monitor and return outputs stream
            executor.spawn(monitor.run()).detach();
            outputs
            // output_handler goes out of scope here, but monitor should continue
        };

        // Collect results after output handler has been dropped
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - monitor should persist and produce correct outputs
        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(3))])),
                (1, BTreeMap::from([("z".into(), Value::Int(7))])),
            ],
            "Monitor persistence failed for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_simple_add_monitor_large_input(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        // Common specification for all configurations
        let spec_untyped = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        // Create fresh input streams for each test iteration (100 elements)
        let input_streams = trustworthiness_checker::dsrv_fixtures::input_streams_simple_add(100);

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert that large input produces expected number of outputs
        assert_eq!(
            result.len(),
            100,
            "Expected 100 outputs for large input, got {} for config {:?}",
            result.len(),
            config
        );

        // Verify outputs are correct (z = x + y where x=2*i, y=2*i+1, so z=4*i+1)
        for (i, (time, values)) in result.iter().enumerate() {
            assert_eq!(*time, i, "Output time should match index");
            assert_eq!(values.len(), 1, "Should have exactly one output value");
            let expected = BTreeMap::from([("z".into(), Value::Int(4 * (i as i64) + 1))]);
            assert_eq!(
                *values, expected,
                "Output at time {} should be {:?}, got {:?} for config {:?}",
                i, expected, values, config
            );
        }
    }
    Ok(())
}

pub fn input_streams_simple_add() -> MapInputProvider {
    let mut input_streams = BTreeMap::new();
    input_streams.insert("x".into(), vec![Value::Int(1), 3.into()]);
    input_streams.insert("y".into(), vec![Value::Int(2), 4.into()]);
    MapInputProvider::new(input_streams)
}

pub fn input_streams_constraint_style() -> MapInputProvider {
    let mut input_streams = BTreeMap::new();
    input_streams.insert("x".into(), vec![Value::Int(1), 3.into(), 5.into()]);
    input_streams.insert("y".into(), vec![Value::Int(2), 4.into(), 6.into()]);
    MapInputProvider::new(input_streams)
}

#[apply(async_test)]
async fn test_simple_add_monitor(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let spec_untyped = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        let input_streams = input_streams3();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(3))])),
                (1, BTreeMap::from([("z".into(), Value::Int(7))])),
            ]
        );
    }
    Ok(())
}

/// Verify that the untyped semantics (AsyncUntimed, SemiSyncUntimed) works with
/// specifications that have no type annotations (since type annotations are used
/// for most tests)
#[apply(async_test)]
async fn test_simple_add_monitor_untyped_spec(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let input_streams = input_streams3();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Int(3))])),
                (1, BTreeMap::from([("z".into(), Value::Int(7))])),
            ],
            "Untyped spec failed for config {:?}",
            config,
        );
    }
    Ok(())
}

/// Verify that the untyped semantics (AsyncUntimed, SemiSyncUntimed) works with
/// specifications that have no type annotations and which involve defer
/// for most tests)
#[apply(async_test)]
async fn test_defer_untyped_spec(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        if config == TestConfiguration::SemiSyncUntimed {
            continue;
        }
        let mut spec_str = "in x\nin e\nout z\nz = defer(e)";
        let spec = dsrv_specification(&mut spec_str).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 2.into())])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
            "Untyped defer spec failed for config {:?}",
            config,
        );
    }
    Ok(())
}

/// Verify that the untyped semantics works with an annotation-free spec with dynamic
#[apply(async_test)]
async fn test_dynamic_untyped_spec(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        let mut spec_str = spec_dynamic_monitor();
        let spec = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams2();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (
                    0,
                    BTreeMap::from([("z".into(), Value::Int(3)), ("w".into(), Value::Int(3))])
                ),
                (
                    1,
                    BTreeMap::from([("z".into(), Value::Int(7)), ("w".into(), Value::Int(7))])
                ),
            ],
            "Untyped dynamic spec failed for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_simple_modulo_monitor(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let spec_untyped = dsrv_specification(&mut spec_simple_modulo_monitor_typed()).unwrap();

        let input_streams = input_streams3();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert based on configuration expectations
        match config {
            TestConfiguration::AsyncUntimed
            | TestConfiguration::AsyncTypedUntimed
            | TestConfiguration::AsyncGradualTypedUntimed
            | TestConfiguration::SemiSyncUntimed
            | TestConfiguration::SemiSyncTypedUntimed
            | TestConfiguration::SemiSyncGradualTypedUntimed => {
                assert_eq!(
                    result,
                    vec![
                        (0, BTreeMap::from([("z".into(), Value::Int(0))])),
                        (1, BTreeMap::from([("z".into(), Value::Int(1))])),
                    ]
                );
            }
        }
    }
    Ok(())
}

#[apply(async_test)]
async fn test_simple_add_monitor_float(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::async_configurations() {
        let spec_untyped = dsrv_specification(&mut spec_simple_add_monitor_typed_float()).unwrap();

        let input_streams = input_streams_float();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(result.len(), 2);
        match *result[0].1.get(&VarName::new("z")).unwrap() {
            Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
        match *result[1].1.get(&VarName::new("z")).unwrap() {
            Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
            _ => panic!("Expected float"),
        }
    }
    Ok(())
}

#[apply(async_test)]
async fn test_count_monitor_sequential_with_drop_guard(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Test running monitors sequentially using drop guard cancellation approach
    for semantics in [Semantics::Untimed, Semantics::TypedUntimed] {
        // First run
        {
            let input_streams = MapInputProvider::new(BTreeMap::new());
            let mut spec_str = "out x: Int\nx = 1 + default(x[1], 0)";
            let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars(),
            ));
            let outputs = output_handler.get_output();

            let monitor = GeneralRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .semantics(semantics)
                .build()
                .await;

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, BTreeMap<VarName, Value>)> =
                with_timeout(outputs.take(4).enumerate().collect(), 5, "outputs.collect").await?;

            assert_eq!(
                result,
                vec![
                    (0, BTreeMap::from([("x".into(), Value::Int(1))])),
                    (1, BTreeMap::from([("x".into(), Value::Int(2))])),
                    (2, BTreeMap::from([("x".into(), Value::Int(3))])),
                    (3, BTreeMap::from([("x".into(), Value::Int(4))])),
                ],
                "First run failed for semantics {:?}",
                semantics,
            );
        }

        // Second run - should work now with drop guard cancellation
        {
            let input_streams = MapInputProvider::new(BTreeMap::new());
            let mut spec_str = "out x: Int\nx = 1 + default(x[1], 0)";
            let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars(),
            ));
            let outputs = output_handler.get_output();

            let monitor = GeneralRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .semantics(semantics)
                .build()
                .await;

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, BTreeMap<VarName, Value>)> =
                with_timeout(outputs.take(4).enumerate().collect(), 5, "outputs.collect").await?;

            assert_eq!(
                result,
                vec![
                    (0, BTreeMap::from([("x".into(), Value::Int(1))])),
                    (1, BTreeMap::from([("x".into(), Value::Int(2))])),
                    (2, BTreeMap::from([("x".into(), Value::Int(3))])),
                    (3, BTreeMap::from([("x".into(), Value::Int(4))])),
                ],
                "Second run failed for semantics {:?}",
                semantics,
            );
        }
    }
    Ok(())
}

#[apply(async_test)]
async fn test_direct_varmanager_cancellation(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    use async_stream::stream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use trustworthiness_checker::utils::cancellation_token::CancellationToken;

    // Create a cancellation token and an infinite input stream
    let cancellation_token = CancellationToken::new();
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    let input_stream = Box::pin(stream! {
        let mut counter = 0;
        while running_clone.load(Ordering::Relaxed) {
            yield Value::Int(counter);
            counter += 1;
            smol::Timer::after(std::time::Duration::from_millis(10)).await;
        }
    });

    // Create VarManager directly with cancellation token
    let mut var_manager = trustworthiness_checker::runtime::asynchronous::VarManager::new(
        executor.clone(),
        "test_var".into(),
        input_stream,
        cancellation_token.clone(),
    );

    // Subscribe to get some output
    let output_stream = var_manager.subscribe();

    // Start the var manager
    let var_manager_task = executor.spawn(var_manager.run());

    // Collect a few values
    let mut values = Vec::new();
    let mut output_stream = output_stream;
    for _ in 0..3 {
        if let Some(value) = output_stream.next().await {
            values.push(value);
        }
    }

    // Verify we got some values
    assert_eq!(values.len(), 3);

    // Now cancel the token
    cancellation_token.cancel();
    running.store(false, Ordering::Relaxed);

    // The var manager task should complete soon due to cancellation
    // Use select to race the task completion against a timeout
    use futures::{FutureExt, select};
    let timeout_fut = smol::Timer::after(std::time::Duration::from_millis(500));

    let completed = select! {
        _ = FutureExt::fuse(var_manager_task) => true,
        _ = FutureExt::fuse(timeout_fut) => false,
    };

    // The task should have completed due to cancellation
    assert!(
        completed,
        "VarManager should have stopped due to cancellation"
    );
    Ok(())
}

#[apply(async_test)]
async fn test_drop_guard_cancellation_behavior(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Test to verify that drop guard properly stops VarManagers when output streams are dropped
    for semantics in [Semantics::Untimed, Semantics::TypedUntimed] {
        let input_streams = MapInputProvider::new(BTreeMap::new());
        let mut spec_str = "out x: Int\nx = 1 + default(x[1], 0)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let monitor = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler)
            .semantics(semantics)
            .build()
            .await;

        executor.spawn(monitor.run()).detach();

        // Take only 2 values - this should trigger drop guard when output stream is dropped
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.take(2).enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("x".into(), Value::Int(1))])),
                (1, BTreeMap::from([("x".into(), Value::Int(2))])),
            ],
            "Drop guard cancellation failed for semantics {:?}",
            semantics,
        );

        // Add a small delay to allow cancellation to propagate via drop guard
        smol::Timer::after(std::time::Duration::from_millis(100)).await;
    }
    Ok(())
}

#[apply(async_test)]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = "out x: Int\nx = 1 + default(x[1], 0)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration (empty for count monitor)
        let input_streams = MapInputProvider::new(BTreeMap::new());

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.take(4).enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - count functionality should work across configurations
        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("x".into(), Value::Int(1))])),
                (1, BTreeMap::from([("x".into(), Value::Int(2))])),
                (2, BTreeMap::from([("x".into(), Value::Int(3))])),
                (3, BTreeMap::from([("x".into(), Value::Int(4))])),
            ],
            "Count monitor failed for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_multiple_parameters(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::async_configurations() {
        let mut spec = "in x : Int\nin y : Int\nout r1 : Int\nout r2 : Int\nr1 =x+y\nr2 = x * y";
        let spec_untyped = dsrv_specification(&mut spec).unwrap();

        let input_streams = input_streams3();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(result.len(), 2);
        assert_eq!(
            result,
            vec![
                (
                    0,
                    BTreeMap::from([("r1".into(), Value::Int(3)), ("r2".into(), Value::Int(2))])
                ),
                (
                    1,
                    BTreeMap::from([("r1".into(), Value::Int(7)), ("r2".into(), Value::Int(12))])
                ),
            ]
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_dynamic_monitor_untimed(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nin y: Int\nin s: Str\nout z: Int\nout w: Int\nz = x + y\nw = dynamic(s : Int)";
        let input_streams = input_streams2();
        let spec = dsrv_specification(&mut spec_str).unwrap();
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;
        assert_eq!(
            result,
            vec![
                (
                    0,
                    BTreeMap::from([("z".into(), Value::Int(3)), ("w".into(), Value::Int(3))])
                ),
                (
                    1,
                    BTreeMap::from([("z".into(), Value::Int(7)), ("w".into(), Value::Int(7))])
                ),
            ],
            "Dynamic monitor untimed failed for config {:?}",
            config,
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_string_concatenation(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let spec_untyped = dsrv_specification(&mut spec_typed_string_concat()).unwrap();

        let input_streams = input_streams4();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        assert_eq!(
            result,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Str("ab".into()))])),
                (1, BTreeMap::from([("z".into(), Value::Str("cd".into()))])),
            ]
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_past_indexing(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nin y: Int\nout z: Int\nz = x[1]";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = input_streams_constraint_style();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        let expected_results = vec![
            (0, BTreeMap::from([("z".into(), Value::Deferred)])), // Default value for x[1] at time 0
            (1, BTreeMap::from([("z".into(), Value::Int(1))])),   // x[0] = 1 at time 1
            (2, BTreeMap::from([("z".into(), Value::Int(3))])),   // x[1] = 3 at time 2
            (3, BTreeMap::from([("z".into(), Value::Int(5))])),   // x[2] = 3 at time 3
        ];
        assert_eq!(
            result, expected_results,
            "Temporal access failed for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
async fn test_maple_sequence(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let spec_untyped = dsrv_specification(&mut spec_maple_sequence()).unwrap();

        let input_streams = maple_valid_input_stream(10);

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);
        let monitor = builder.build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // TODO: Different runtimes may handle temporal dependencies differently for complex patterns
        // Expected: maple sequence should detect complete "maple" patterns in the input stream
        // The exact number and content of outputs may vary between runtimes due to different
        // handling of temporal access patterns and default values
        assert!(
            result.len() >= 5 && result.len() <= 15,
            "Maple sequence produced {} outputs for config {:?}",
            result.len(),
            config
        );

        // Verify that we get boolean outputs (the specification outputs are all Bool type)
        for (time, values) in &result {
            assert!(
                values
                    .iter()
                    .map(|(_, v)| v)
                    .all(|v| matches!(v, Value::Bool(_))),
                "Expected all boolean outputs at time {}, got {:?} for config {:?}",
                time,
                values,
                config
            );
        }
    }
    Ok(())
}

#[apply(async_test)]
async fn test_restricted_dynamic_monitor(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if config == TestConfiguration::SemiSyncUntimed {
            // dynamic evaluation not yet verified on SemiSync
            continue;
        }
        let mut spec_str = "in x: Int\nin y: Int\nin s: Str\nout z: Int\nout w: Int\nz = x + y\nw = dynamic(s : Int, {x,y})";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams2();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - dynamic monitor should work across configurations
        assert!(
            result.len() >= 2,
            "Expected at least 2 outputs for restricted dynamic monitor, got {} for config {:?}",
            result.len(),
            config
        );

        // Verify that we get outputs with expected structure
        for (time, values) in &result {
            assert_eq!(
                values.len(),
                2,
                "Expected 2 output values (z and w) at time {}, got {} for config {:?}",
                time,
                values.len(),
                config
            );
        }
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_stream_1(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams_defer_1();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - defer functionality should work across configurations
        assert!(
            result.len() >= 2,
            "Expected at least 2 outputs for defer stream 1, got {} for config {:?}",
            result.len(),
            config
        );

        let expected_outputs = vec![
            (0, BTreeMap::from([("z".into(), Value::Deferred)])),
            (1, BTreeMap::from([("z".into(), Value::Int(2))])),
            (2, BTreeMap::from([("z".into(), Value::Int(3))])),
            (3, BTreeMap::from([("z".into(), Value::Int(4))])),
            (4, BTreeMap::from([("z".into(), Value::Int(5))])),
            (5, BTreeMap::from([("z".into(), Value::Int(6))])),
            (6, BTreeMap::from([("z".into(), Value::Int(7))])),
            (7, BTreeMap::from([("z".into(), Value::Int(8))])),
            (8, BTreeMap::from([("z".into(), Value::Int(9))])),
            (9, BTreeMap::from([("z".into(), Value::Int(10))])),
            (10, BTreeMap::from([("z".into(), Value::Int(11))])),
            (11, BTreeMap::from([("z".into(), Value::Int(12))])),
            (12, BTreeMap::from([("z".into(), Value::Int(13))])),
            (13, BTreeMap::from([("z".into(), Value::Int(14))])),
            (14, BTreeMap::from([("z".into(), Value::Int(15))])),
        ];
        assert_eq!(
            result, expected_outputs,
            "Did not get expected output for config {:?}",
            config
        )
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_stream_2(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams_defer_2();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - defer functionality should work across configurations
        assert!(
            result.len() >= 2,
            "Expected at least 2 outputs for defer stream 2, got {} for config {:?}",
            result.len(),
            config
        );

        let expected_outputs = vec![
            (0, BTreeMap::from([("z".into(), Value::Deferred)])),
            (1, BTreeMap::from([("z".into(), Value::Deferred)])),
            (2, BTreeMap::from([("z".into(), Value::Deferred)])),
            (3, BTreeMap::from([("z".into(), Value::Int(4))])),
            (4, BTreeMap::from([("z".into(), Value::Int(5))])),
            (5, BTreeMap::from([("z".into(), Value::Int(6))])),
            (6, BTreeMap::from([("z".into(), Value::Int(7))])),
            (7, BTreeMap::from([("z".into(), Value::Int(8))])),
            (8, BTreeMap::from([("z".into(), Value::Int(9))])),
            (9, BTreeMap::from([("z".into(), Value::Int(10))])),
            (10, BTreeMap::from([("z".into(), Value::Int(11))])),
            (11, BTreeMap::from([("z".into(), Value::Int(12))])),
            (12, BTreeMap::from([("z".into(), Value::Int(13))])),
            (13, BTreeMap::from([("z".into(), Value::Int(14))])),
            (14, BTreeMap::from([("z".into(), Value::Int(15))])),
        ];
        assert_eq!(
            result, expected_outputs,
            "Did not get expected output for config {:?}",
            config
        )
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_stream_3(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams_defer_3();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - defer functionality should work across configurations
        assert!(
            result.len() >= 2,
            "Expected at least 2 outputs for defer stream 3, got {} for config {:?}",
            result.len(),
            config
        );

        let expected_outputs = vec![
            (0, BTreeMap::from([("z".into(), Value::Deferred)])),
            (1, BTreeMap::from([("z".into(), Value::Deferred)])),
            (2, BTreeMap::from([("z".into(), Value::Deferred)])),
            (3, BTreeMap::from([("z".into(), Value::Deferred)])),
            (4, BTreeMap::from([("z".into(), Value::Deferred)])),
            (5, BTreeMap::from([("z".into(), Value::Deferred)])),
            (6, BTreeMap::from([("z".into(), Value::Deferred)])),
            (7, BTreeMap::from([("z".into(), Value::Deferred)])),
            (8, BTreeMap::from([("z".into(), Value::Deferred)])),
            (9, BTreeMap::from([("z".into(), Value::Deferred)])),
            (10, BTreeMap::from([("z".into(), Value::Deferred)])),
            (11, BTreeMap::from([("z".into(), Value::Deferred)])),
            (12, BTreeMap::from([("z".into(), Value::Int(13))])),
            (13, BTreeMap::from([("z".into(), Value::Int(14))])),
            (14, BTreeMap::from([("z".into(), Value::Int(15))])),
        ];
        assert_eq!(
            result, expected_outputs,
            "Did not get expected output for config {:?}",
            config
        )
    }
    Ok(())
}

#[apply(async_test)]
async fn test_defer_stream_4(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if matches!(
            config,
            TestConfiguration::SemiSyncUntimed
                | TestConfiguration::SemiSyncTypedUntimed
                | TestConfiguration::SemiSyncGradualTypedUntimed
        ) {
            // Bugs in defer that this runtime does not like
            continue;
        }
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams_defer_4();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert expected results - defer functionality should work across configurations
        assert!(
            result.len() >= 2,
            "Expected at least 2 outputs for defer stream 4, got {} for config {:?}",
            result.len(),
            config
        );

        // Notice one output "too many". This is expected behaviour (at least with a global default
        // history_length = 10 for defer) since once e = x[1] has arrived
        // the stream for z = defer(e) will continue as long as x[1] keeps
        // producing values (making use of its history) which can continue beyond
        // the lifetime of the stream for e (since it does not depend on e any more
        // once a value has been received). This differs from the behaviour of
        // defer(e) which stops if e stops.
        //
        // See also: Comment on sindex combinator.

        let expected_outputs = vec![
            (0, BTreeMap::from([("z".into(), Value::Deferred)])),
            (1, BTreeMap::from([("z".into(), Value::Deferred)])),
            (2, BTreeMap::from([("z".into(), Value::Deferred)])),
            (3, BTreeMap::from([("z".into(), Value::Int(2))])),
            (4, BTreeMap::from([("z".into(), Value::Int(3))])),
        ];
        assert_eq!(
            result, expected_outputs,
            "Did not get expected output for config {:?}",
            config
        );
    }
    Ok(())
}

#[apply(async_test)]
#[ignore = "Bug with DUPs here exposed by recent changes to defer impl. \
    Subcontexts have deadlock scenario with multiple defer/dynamic streams. \
    Before, this was not exposed because defer's usage of subcontexts was significantly different \
    from dynamic. So before the defer patch, the bug would only happen with multiple dynamic streams, \
    which we do not have a test for..."]
async fn test_defer_comp_dynamic(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nin e: Str\nout z1: Int\nout z2: Int\nz1 = defer(e : Int)\nz2 = dynamic(e : Int)";
        let spec_untyped = dsrv_specification(&mut spec_str).unwrap();

        // Create fresh input streams for each test iteration
        let input_streams = input_streams_defer_comp_dynamic();

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.enumerate().collect(),
            1,
            format!("outputs.collect with config: {:?}", config).as_str(),
        )
        .await?;

        for (time, values) in &result {
            assert_eq!(
                values.len(),
                2,
                "Expected 2 output values (z1 and z2) at time {}, got {} for config {:?}",
                time,
                values.len(),
                config
            );

            let (v_defer, v_dynamic) = (&values.get(&"z1".into()), &values.get(&"z2".into()));
            assert_eq!(
                v_defer, v_dynamic,
                "Expected defer and dynamic outputs to match at time {}, got {:?} and {:?} for config {:?}.\nFull values:\n{:?}",
                time, v_defer, v_dynamic, config, result
            );
        }
    }
    Ok(())
}

// TODO: TW there is a bug here with the typed async configuration - hence why I used the
// untyped only
#[apply(async_test)]
async fn test_benchmark_regression_long_add_defer(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    // Specifically we used to fail with size >= 2007
    const SIZE: usize = 3000;

    // Hack to force log into being INFO for this test.
    // Needed to make CI perform better
    use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, util::SubscriberInitExt};
    let subscriber = tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(fmt::layer().with_test_writer());
    let _guard = subscriber.set_default(); // active only in this scope

    for config in TestConfiguration::untyped_configurations() {
        let spec = dsrv_specification(&mut spec_add_defer()).unwrap();

        let input_streams = input_streams_add_defer(SIZE);

        // Create output handler based on configuration
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        // Apply configuration-specific settings
        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build().await;

        // Run monitor and collect results
        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> = with_timeout(
            outputs.take(SIZE).enumerate().collect(),
            10,
            format!("outputs.collect with config: {:?}", config).as_str(),
        )
        .await?;

        // This test is just about not hanging
        assert_eq!(result.len(), SIZE);
    }
    Ok(())
}

// Map operations with deferred values use untyped configurations only:
// StreamType has no Map variant, so map specs must remain annotation-free.

#[apply(async_test)]
async fn test_map_get_deferred_propagates(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    for config in TestConfiguration::untyped_configurations() {
        // Map.get should propagate Deferred rather than panicking when the map
        // input stream carries a Deferred tick.
        let mut spec_str = "in m\nout z\nz = Map.get(m, \"x\")";
        let spec = dsrv_specification(&mut spec_str).unwrap();

        let input_streams = MapInputProvider::new(BTreeMap::from([(
            "m".into(),
            vec![
                Value::Map(BTreeMap::from([("x".into(), 1.into())])),
                Value::Deferred,
                Value::Map(BTreeMap::from([("x".into(), 3.into())])),
            ],
        )]));

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec)
            .input(Box::new(input_streams))
            .output(output_handler);
        let builder = create_builder_from_config(builder, config);
        let monitor = builder.build().await;
        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;

        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), Value::Deferred)])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
            "Map.get deferred propagation failed for config {:?}",
            config,
        );
    }
    Ok(())
}

#[cfg(test)]
mod reconf_tests {
    use super::*;
    use async_unsync::bounded;
    use futures::future;
    use std::collections::BTreeSet;
    use tc_testutils::streams::with_timeout;
    use tracing::info;
    use trustworthiness_checker::io::builders::output_handler_builder::OutputHandlerSpec;
    use trustworthiness_checker::io::builders::{
        InputProviderBuilder, InputProviderSpec, OutputHandlerBuilder,
    };
    use trustworthiness_checker::lang::dsrv::lalr_parser::LALRParser;
    use trustworthiness_checker::runtime::builder::SemiSyncValueConfig;
    use trustworthiness_checker::runtime::reconfigurable_semi_sync::ReconfSemiSyncRuntimeBuilder;
    use trustworthiness_checker::semantics::UntimedDsrvSemantics;
    use trustworthiness_checker::stream_utils::{Fanout, FanoutSender};

    type TestRuntimeBuilder = ReconfSemiSyncRuntimeBuilder<
        SemiSyncValueConfig,
        UntimedDsrvSemantics<LALRParser>,
        LALRParser,
    >;

    const RECONF_TOPIC: &str = "RECONF_ME";

    async fn send_value(tx: &FanoutSender<Value>, val: Value, var: VarName) {
        let _ = with_timeout(
            tx.send(val),
            1,
            format!("tx.send for var {:?}", var).as_str(),
        )
        .await;
    }

    async fn send_value_noval_others(
        var_val: (&str, Value),
        tx_fans: &mut BTreeMap<VarName, FanoutSender<Value>>,
    ) {
        for (var, fan) in tx_fans.iter() {
            let val = if var.name() == var_val.0 {
                var_val.1.clone()
            } else {
                Value::NoVal
            };
            send_value(fan, val, var.clone()).await;
        }
    }

    #[apply(async_test)]
    async fn test_reconf_simple_add_no_reconf(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, without actually sending a
        // reconfiguration, to check that the basic input/output works as expected.
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let expected = vec![Value::NoVal, Value::Int(3), Value::Int(5), Value::Int(7)];
        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);

        // Manual providers:
        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let mut z_iter = expected.into_iter();

        for (x_exp, y_exp) in xs.into_iter().zip(ys.into_iter()) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 1, "out_rx.next()")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 1, "out_rx.next()")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }
    }

    #[apply(async_test)]
    async fn test_typed_reconf_no_change_of_streams(ex: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4), Value::Int(6), Value::Int(8)];
        let expected = vec![
            Value::NoVal,
            Value::Int(3),
            Value::Int(5),
            Value::Int(7),
            Value::NoVal,
            Value::Int(12),
            Value::Int(14),
            Value::Int(16),
        ];
        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);
        let in_len = xs.len();

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);

        let monitor = GeneralRuntimeBuilder::new()
            .executor(ex.clone())
            .model(spec.clone())
            .input_provider_builder(input_builder)
            .output_handler_builder(output_builder)
            .runtime(RuntimeSpec::ReconfSemiSync)
            .semantics(Semantics::TypedUntimed)
            .reconf_topic(RECONF_TOPIC.into())
            .build()
            .await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(in_len / 2);
        let x_iter2 = xs.into_iter().skip(in_len / 2);
        let y_iter1 = ys.clone().into_iter().take(in_len / 2);
        let y_iter2 = ys.into_iter().skip(in_len / 2);
        let mut z_iter = expected.into_iter();

        for (x_exp, y_exp) in x_iter1.zip(y_iter1) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;
            let mut out_res = with_timeout(out_rx.recv(), 3, "typed out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            assert_eq!(out_res.remove(&"z".into()).unwrap(), z_iter.next().unwrap());

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;
            let mut out_res = with_timeout(out_rx.recv(), 3, "typed out_rx.y")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            assert_eq!(out_res.remove(&"z".into()).unwrap(), z_iter.next().unwrap());
        }

        let typed_plus_one_spec = "in x: Int\nin y: Int\nout z: Int\nz = x + y + 1";
        let reconf_json = serde_json::json!({
            "spec": typed_plus_one_spec,
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("typed sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;
        future::join_all(sub_event_futs).await;

        for (x_exp, y_exp) in x_iter2.zip(y_iter2) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;
            let mut out_res = with_timeout(out_rx.recv(), 3, "typed out_rx.x_post")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            assert_eq!(out_res.remove(&"z".into()).unwrap(), z_iter.next().unwrap());

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;
            let mut out_res = with_timeout(out_rx.recv(), 3, "typed out_rx.y_post")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            assert_eq!(out_res.remove(&"z".into()).unwrap(), z_iter.next().unwrap());
        }
    }

    #[apply(async_test)]
    async fn test_typed_reconf_type_error_is_reported(ex: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();
        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);

        let input_builder = InputProviderBuilder::new(InputProviderSpec::Manual(inp_fans))
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, _out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_builder = OutputHandlerBuilder::new(OutputHandlerSpec::Manual(out_tx))
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);

        let monitor = GeneralRuntimeBuilder::new()
            .executor(ex.clone())
            .model(spec.clone())
            .input_provider_builder(input_builder)
            .output_handler_builder(output_builder)
            .runtime(RuntimeSpec::ReconfSemiSync)
            .semantics(Semantics::TypedUntimed)
            .reconf_topic(RECONF_TOPIC.into())
            .build()
            .await;

        let (run_tx, mut run_rx) = bounded::channel::<Result<(), String>>(1).into_split();
        ex.spawn(async move {
            let result = monitor.run().await.map_err(|err| err.to_string());
            let _ = run_tx.send(result).await;
        })
        .detach();

        let invalid_reconf_spec = "in x: Int\nin y: Int\nout z: Int\nz = x + \"not an int\"";
        let reconf_json = serde_json::json!({
            "spec": invalid_reconf_spec,
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        let run_result = with_timeout(run_rx.recv(), 3, "typed reconf runtime error")
            .await
            .expect("runtime did not report the type error")
            .expect("runtime result channel closed");
        let err = run_result.expect_err("invalid typed reconfiguration should fail the runtime");
        assert!(
            err.contains("Reconfigured spec failed type checking"),
            "expected type-checking error, got: {err}"
        );
    }

    #[apply(async_test)]
    async fn test_reconf_no_change_of_streams(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, where we reconfigure but do
        // not introduce/remove any streams
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4), Value::Int(6), Value::Int(8)];
        let expected = vec![
            Value::NoVal,
            Value::Int(3),
            Value::Int(5),
            Value::Int(7),
            // Here we reconf:
            Value::NoVal,
            Value::Int(12),
            Value::Int(14),
            Value::Int(16),
        ];
        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);
        let in_len = xs.len();

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(in_len / 2);
        let x_iter2 = xs.into_iter().skip(in_len / 2);
        let y_iter1 = ys.clone().into_iter().take(in_len / 2);
        let y_iter2 = ys.into_iter().skip(in_len / 2);
        let mut z_iter = expected.into_iter();

        // Pre-reconf: interleave x and y with NoVal for the others
        for (x_exp, y_exp) in x_iter1.zip(y_iter1) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.y")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }

        info!("Finished pre-reconf phase, now sending reconf");
        // Reconfigure: send the new spec via RECONF_TOPIC
        let reconf_json = serde_json::json!({
            "spec": spec_simple_add_monitor_plus_one(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        // Post-reconf: same interleave pattern
        for (x_exp, y_exp) in x_iter2.zip(y_iter2) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x_post")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.y_post")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }
    }

    #[apply(async_test)]
    async fn test_reconf_delete_input_stream(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the simple add monitor, where we reconfigure to a
        // spec that does not require a y stream

        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let y_len = ys.len();
        let expected = vec![
            Value::NoVal,
            Value::Int(3),
            Value::Int(5),
            Value::Int(7),
            // Here we reconf:
            Value::Int(5),
            Value::Int(12),
        ];

        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(y_len);
        let x_iter2 = xs.into_iter().skip(y_len);
        let y_iter = ys.into_iter();
        let mut z_iter = expected.into_iter();

        // Pre-reconf: interleave x and y with NoVal for the others
        for (x_exp, y_exp) in x_iter1.zip(y_iter) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.y")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }

        info!("Finished pre-reconf phase, now sending reconf");
        // Reconfigure: send the new spec via RECONF_TOPIC
        let reconf_json = serde_json::json!({
            "spec": spec_acc_monitor(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .filter(|(var, _)| var.name() != "y") // New spec does not have y
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        // Post-reconf: Only x
        for x_exp in x_iter2 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x_post")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }
    }

    #[apply(async_test)]
    async fn test_reconf_add_input_stream(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the acc spec, where we reconfigure to
        // run the simple_add spec, which includes an extra input stream

        let spec = dsrv_specification(&mut spec_acc_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(3), Value::Int(5), Value::Int(7)];
        let ys = vec![Value::Int(2), Value::Int(4)];
        let y_len = ys.len();
        let expected = vec![
            Value::Int(1),
            Value::Int(4),
            // Here we reconf:
            Value::NoVal,
            Value::Int(7),
            Value::Int(9),
            Value::Int(11),
        ];

        // Note: Defines Fanout for y initially but is not used until after reconf.
        // Needed because we cannot modify the InputProviderBuilder after giving it to Runtime
        let (tx_x, fx) = Fanout::new();
        let (tx_y, fy) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([
            ("x".into(), fx),
            ("y".into(), fy),
            (RECONF_TOPIC.into(), fr),
        ]);
        let mut tx_fans = BTreeMap::from([
            ("x".into(), tx_x),
            ("y".into(), tx_y),
            (RECONF_TOPIC.into(), tx_r),
        ]);

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();
        let x_iter1 = xs.clone().into_iter().take(y_len);
        let x_iter2 = xs.into_iter().skip(y_len);
        let y_iter = ys.into_iter();
        let mut z_iter = expected.into_iter();

        // Pre-reconf: Only x
        for x_exp in x_iter1 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }

        info!("Finished pre-reconf phase, now sending reconf");
        let reconf_json = serde_json::json!({
            "spec": spec_simple_add_monitor(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        // Post-reconf: interleave x and y with NoVal for the others
        for (x_exp, y_exp) in x_iter2.zip(y_iter) {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);

            send_value_noval_others(("y", y_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.y")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }
    }

    #[apply(async_test)]
    async fn test_reconf_delete_output_stream(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the where we initally have two output streams,
        // and reconfigure into having one

        let spec = dsrv_specification(&mut spec_assignment2_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let vs = xs.clone();
        let ws = vec![Value::Int(2), Value::Int(3)];
        let ws_len = ws.len();

        let (tx_x, fx) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([("x".into(), fx), (RECONF_TOPIC.into(), fr)]);
        let mut tx_fans = BTreeMap::from([("x".into(), tx_x), (RECONF_TOPIC.into(), tx_r)]);

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["v".into(), "w".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(ws_len);
        let x_iter2 = xs.into_iter().skip(ws_len);
        let mut v_iter = vs.into_iter();
        let mut w_iter = ws.into_iter();

        // Pre-reconf: Both v and w outputs
        for x_exp in x_iter1 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let v_res = out_res
                .remove(&"v".into())
                .expect("output did not contain v");
            let v_exp = v_iter.next().unwrap();
            assert_eq!(v_res, v_exp);
            let w_res = out_res
                .remove(&"w".into())
                .expect("output did not contain w");
            let w_exp = w_iter.next().unwrap();
            assert_eq!(w_res, w_exp);
        }

        info!("Finished pre-reconf phase, now sending reconf");
        let reconf_json = serde_json::json!({
            "spec": spec_assignment_monitor(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        // Post-reconf: Only v
        for x_exp in x_iter2 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let v_res = out_res
                .remove(&"v".into())
                .expect("output did not contain v");
            let v_exp = v_iter.next().unwrap();
            assert_eq!(v_res, v_exp);
            assert!(
                !out_res.contains_key(&"w".into()),
                "Output should not contain w after reconf, but got {:?}",
                out_res
            );
        }
    }

    #[apply(async_test)]
    async fn test_reconf_add_output_stream(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime with the where we initally have one output streams,
        // and reconfigure into having two

        let spec = dsrv_specification(&mut spec_assignment_monitor()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let vs = xs.clone();
        let ws = vec![Value::Int(4), Value::Int(5)];
        let ws_len = ws.len();

        let (tx_x, fx) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([("x".into(), fx), (RECONF_TOPIC.into(), fr)]);
        let mut tx_fans = BTreeMap::from([("x".into(), tx_x), (RECONF_TOPIC.into(), tx_r)]);

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["v".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(ws_len);
        let x_iter2 = xs.into_iter().skip(ws_len);
        let mut v_iter = vs.into_iter();
        let mut w_iter = ws.into_iter();

        // Pre-reconf: Only v
        for x_exp in x_iter1 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let v_res = out_res
                .remove(&"v".into())
                .expect("output did not contain v");
            let v_exp = v_iter.next().unwrap();
            assert_eq!(v_res, v_exp);
            assert!(
                !out_res.contains_key(&"w".into()),
                "Output should not contain w after reconf, but got {:?}",
                out_res
            );
        }

        info!("Finished pre-reconf phase, now sending reconf");
        let reconf_json = serde_json::json!({
            "spec": spec_assignment2_monitor(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        // Post-reconf: Both v and w outputs
        for x_exp in x_iter2 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let v_res = out_res
                .remove(&"v".into())
                .expect("output did not contain v");
            let v_exp = v_iter.next().unwrap();
            assert_eq!(v_res, v_exp);
            let w_res = out_res
                .remove(&"w".into())
                .expect("output did not contain w");
            let w_exp = w_iter.next().unwrap();
            assert_eq!(w_res, w_exp);
        }
    }

    #[apply(async_test)]
    async fn test_reconf_sindex_context_transfer_simple(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime correctly transfers the context from the old spec to the
        // new one.

        let spec = dsrv_specification(&mut spec_sindex()).unwrap();
        let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let in_len = xs.len();
        let expected = vec![
            Value::Deferred,
            Value::Int(1),
            // Notice no new deferred
            Value::Int(3),
            Value::Int(4),
        ];

        let (tx_x, fx) = Fanout::new();
        let (tx_r, fr) = Fanout::new();
        let inp_fans = BTreeMap::from([("x".into(), fx), (RECONF_TOPIC.into(), fr)]);
        let mut tx_fans = BTreeMap::from([("x".into(), tx_x), (RECONF_TOPIC.into(), tx_r)]);

        let input_spec = InputProviderSpec::Manual(inp_fans);
        let input_builder = InputProviderBuilder::new(input_spec)
            .model(spec.clone())
            .executor(ex.clone());

        let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
        let output_spec = OutputHandlerSpec::Manual(out_tx);
        let output_builder = OutputHandlerBuilder::new(output_spec)
            .executor(ex.clone())
            .output_var_names(BTreeSet::from(["z".into()]))
            .aux_info(vec![]);
        let monitor_builder = Box::new(
            TestRuntimeBuilder::new()
                .executor(ex.clone())
                .model(spec.clone())
                .input_builder(input_builder)
                .output_builder(output_builder)
                .reconf_topic(RECONF_TOPIC.into()),
        );
        let monitor = monitor_builder.build().await;
        ex.spawn(monitor.run()).detach();

        let x_iter1 = xs.clone().into_iter().take(in_len / 2);
        let x_iter2 = xs.into_iter().skip(in_len / 2);
        let mut z_iter = expected.into_iter();

        for x_exp in x_iter1 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }

        info!("Finished pre-reconf phase, now sending reconf");
        let reconf_json = serde_json::json!({
            "spec": spec_sindex_plus(),
            "type_info": {},
            "topic_mapping": {}
        })
        .to_string();

        // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
        let sub_event_futs: Vec<_> = tx_fans
            .iter()
            .map(|(var, fan_tx)| {
                let fan_rc = fan_tx.fanout();
                let label = format!("sub event on {}", var);
                let wait_fut = async move {
                    let fan = fan_rc.as_ref();
                    let seen = fan.sub_events();
                    with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                };
                Box::pin(wait_fut)
            })
            .collect();

        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        future::join_all(sub_event_futs).await;
        info!("Finished reconf, now sending post-reconf values");

        for x_exp in x_iter2 {
            send_value_noval_others(("x", x_exp), &mut tx_fans).await;

            let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                .await
                .expect("failed to get result")
                .expect("output channel closed");
            let z_res = out_res
                .remove(&"z".into())
                .expect("output did not contain z");
            let z_exp = z_iter.next().unwrap();
            assert_eq!(z_res, z_exp);
        }
    }

    #[apply(async_test)]
    async fn test_reconf_sindex_context_transfer_simple_output(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime correctly transfers the context from the old spec to the
        // new one, when the context to transfer from is an output stream.
        // Runs twice: once with context transfer, once without.

        for use_context_transfer in [true, false] {
            let mut first_spec = "in x\n\
            out y\n\
            out z\n\
            y = x\n\
            z = y[1]";
            let second_spec = "in x\n\
            out y\n\
            out z\n\
            y = x\n\
            z = y[1] + 1";
            let spec = dsrv_specification(&mut first_spec).unwrap();
            let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
            let in_len = xs.len();
            let expected = if use_context_transfer {
                vec![
                    Value::Deferred,
                    Value::Int(1),
                    // Notice no new deferred
                    Value::Int(3),
                    Value::Int(4),
                ]
            } else {
                vec![
                    Value::Deferred,
                    Value::Int(1),
                    // Notice the extra deferred
                    Value::Deferred,
                    Value::Int(4),
                ]
            };

            let (tx_x, fx) = Fanout::new();
            let (tx_r, fr) = Fanout::new();
            let inp_fans = BTreeMap::from([("x".into(), fx), (RECONF_TOPIC.into(), fr)]);
            let mut tx_fans = BTreeMap::from([("x".into(), tx_x), (RECONF_TOPIC.into(), tx_r)]);

            let input_spec = InputProviderSpec::Manual(inp_fans);
            let input_builder = InputProviderBuilder::new(input_spec)
                .model(spec.clone())
                .executor(ex.clone());

            let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
            let output_spec = OutputHandlerSpec::Manual(out_tx);
            let output_builder = OutputHandlerBuilder::new(output_spec)
                .executor(ex.clone())
                .output_var_names(BTreeSet::from(["y".into(), "z".into()]))
                .aux_info(vec![]);
            let monitor_builder = Box::new(
                TestRuntimeBuilder::new()
                    .executor(ex.clone())
                    .model(spec.clone())
                    .input_builder(input_builder)
                    .output_builder(output_builder)
                    .reconf_topic(RECONF_TOPIC.into())
                    .use_context_transfer(use_context_transfer),
            );
            let monitor = monitor_builder.build().await;
            ex.spawn(monitor.run()).detach();

            let x_iter1 = xs.clone().into_iter().take(in_len / 2);
            let x_iter2 = xs.into_iter().skip(in_len / 2);
            let mut z_iter = expected.into_iter();

            for x_exp in x_iter1 {
                send_value_noval_others(("x", x_exp.clone()), &mut tx_fans).await;

                let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                    .await
                    .expect("failed to get result")
                    .expect("output channel closed");
                let y_res = out_res
                    .remove(&"y".into())
                    .expect("output did not contain y");
                let y_exp = x_exp.clone();
                assert_eq!(y_res, y_exp);
                let z_res = out_res
                    .remove(&"z".into())
                    .expect("output did not contain z");
                let z_exp = z_iter.next().unwrap();
                assert_eq!(z_res, z_exp);
            }

            info!("Finished pre-reconf phase, now sending reconf");
            let reconf_json = serde_json::json!({
                "spec": second_spec,
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string();

            // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
            let sub_event_futs: Vec<_> = tx_fans
                .iter()
                .map(|(var, fan_tx)| {
                    let fan_rc = fan_tx.fanout();
                    let label = format!("sub event on {}", var);
                    let wait_fut = async move {
                        let fan = fan_rc.as_ref();
                        let seen = fan.sub_events();
                        with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                    };
                    Box::pin(wait_fut)
                })
                .collect();

            send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans)
                .await;

            future::join_all(sub_event_futs).await;
            info!("Finished reconf, now sending post-reconf values");

            for x_exp in x_iter2 {
                send_value_noval_others(("x", x_exp.clone()), &mut tx_fans).await;

                let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                    .await
                    .expect("failed to get result")
                    .expect("output channel closed");
                let y_res = out_res
                    .remove(&"y".into())
                    .expect("output did not contain y");
                let y_exp = x_exp.clone();
                assert_eq!(y_res, y_exp);
                let z_res = out_res
                    .remove(&"z".into())
                    .expect("output did not contain z");
                let z_exp = z_iter.next().unwrap();
                assert_eq!(z_res, z_exp);
            }
        }
    }

    #[apply(async_test)]
    async fn test_reconf_sindex_context_transfer_self_reference(ex: Rc<LocalExecutor<'static>>) {
        // Tests the ReconfSemiSyncRuntime correctly transfers the context from the old spec to the
        // new one, when an expression is self-referential and the context transfer is from that expression.
        // Runs twice: once with context transfer, once without.

        for use_context_transfer in [true, false] {
            let spec = dsrv_specification(&mut spec_acc_monitor()).unwrap();
            let xs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
            let in_len = xs.len();
            let expected = if use_context_transfer {
                vec![
                    Value::Int(1),
                    Value::Int(3),
                    // Here we reconf - since context transfer we continue counting
                    Value::Int(4),
                    Value::Int(5),
                ]
            } else {
                vec![
                    Value::Int(1),
                    Value::Int(3),
                    // Here we reconf - since no context transfer we count from 1:
                    Value::Int(1),
                    Value::Int(2),
                ]
            };

            let (tx_x, fx) = Fanout::new();
            let (tx_r, fr) = Fanout::new();
            let inp_fans = BTreeMap::from([("x".into(), fx), (RECONF_TOPIC.into(), fr)]);
            let mut tx_fans = BTreeMap::from([("x".into(), tx_x), (RECONF_TOPIC.into(), tx_r)]);

            let input_spec = InputProviderSpec::Manual(inp_fans);
            let input_builder = InputProviderBuilder::new(input_spec)
                .model(spec.clone())
                .executor(ex.clone());

            let (out_tx, mut out_rx) = bounded::channel::<BTreeMap<VarName, Value>>(4).into_split();
            let output_spec = OutputHandlerSpec::Manual(out_tx);
            let output_builder = OutputHandlerBuilder::new(output_spec)
                .executor(ex.clone())
                .output_var_names(BTreeSet::from(["z".into()]))
                .aux_info(vec![]);
            let monitor_builder = Box::new(
                TestRuntimeBuilder::new()
                    .executor(ex.clone())
                    .model(spec.clone())
                    .input_builder(input_builder)
                    .output_builder(output_builder)
                    .reconf_topic(RECONF_TOPIC.into())
                    .use_context_transfer(use_context_transfer),
            );
            let monitor = monitor_builder.build().await;
            ex.spawn(monitor.run()).detach();

            let x_iter1 = xs.clone().into_iter().take(in_len / 2);
            let x_iter2 = xs.into_iter().skip(in_len / 2);
            let mut z_iter = expected.into_iter();

            for x_exp in x_iter1 {
                send_value_noval_others(("x", x_exp), &mut tx_fans).await;

                let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                    .await
                    .expect("failed to get result")
                    .expect("output channel closed");
                let z_res = out_res
                    .remove(&"z".into())
                    .expect("output did not contain z");
                let z_exp = z_iter.next().unwrap();
                assert_eq!(z_res, z_exp);
            }

            info!("Finished pre-reconf phase, now sending reconf");
            let reconf_json = serde_json::json!({
                "spec": spec_count_bounded_monitor(),
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string();

            // Wait for sub events to be triggered, i.e., new InputProvider has subscribed
            let sub_event_futs: Vec<_> = tx_fans
                .iter()
                .map(|(var, fan_tx)| {
                    let fan_rc = fan_tx.fanout();
                    let label = format!("sub event on {}", var);
                    let wait_fut = async move {
                        let fan = fan_rc.as_ref();
                        let seen = fan.sub_events();
                        with_timeout(fan.wait_for_sub_event(seen), 3, label.as_str()).await
                    };
                    Box::pin(wait_fut)
                })
                .collect();

            send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans)
                .await;

            future::join_all(sub_event_futs).await;
            info!("Finished reconf, now sending post-reconf values");

            for x_exp in x_iter2 {
                send_value_noval_others(("x", x_exp), &mut tx_fans).await;

                let mut out_res = with_timeout(out_rx.recv(), 3, "out_rx.x")
                    .await
                    .expect("failed to get result")
                    .expect("output channel closed");
                let z_res = out_res
                    .remove(&"z".into())
                    .expect("output did not contain z");
                let z_exp = z_iter.next().unwrap();
                assert_eq!(z_res, z_exp);
            }
        }
    }
}
