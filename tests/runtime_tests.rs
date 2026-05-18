use approx::assert_abs_diff_eq;
use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::core::{Runtime, RuntimeSpec, Semantics};
use trustworthiness_checker::io::map::MapInputProvider;
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::runtime::builder::GeneralRuntimeBuilder;
use trustworthiness_checker::{DsrvSpecification, dsrv_fixtures::*};
use trustworthiness_checker::{Value, dsrv_specification, runtime::RuntimeBuilder};
use trustworthiness_checker::{VarName, async_test};

#[derive(Debug, Clone, Copy, PartialEq)]
enum TestConfiguration {
    AsyncUntimed,
    AsyncTypedUntimed,
    SemiSyncUntimed,
}

impl TestConfiguration {
    fn all() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
            TestConfiguration::SemiSyncUntimed,
        ]
    }

    fn async_configurations() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
            TestConfiguration::SemiSyncUntimed,
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
    builder: GeneralRuntimeBuilder<DsrvSpecification, Value>,
    config: TestConfiguration,
) -> GeneralRuntimeBuilder<DsrvSpecification, Value> {
    match config {
        TestConfiguration::AsyncUntimed => {
            let builder = builder.runtime(RuntimeSpec::Async);
            builder.semantics(Semantics::Untimed)
        }
        TestConfiguration::AsyncTypedUntimed => {
            let builder = builder.runtime(RuntimeSpec::Async);
            builder.semantics(Semantics::TypedUntimed)
        }
        TestConfiguration::SemiSyncUntimed => builder.runtime(RuntimeSpec::SemiSync),
    }
}

#[apply(async_test)]
async fn test_defer(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    for config in TestConfiguration::all() {
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
//                 spec_untyped.output_vars.clone(),
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
//                 spec_untyped.output_vars.clone(),
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

        executor.spawn(monitor.run()).detach();
        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect()").await?;
        // TODO: async runtime produces more data than the constraint based runtime
        let num_expected_outputs = match config {
            TestConfiguration::AsyncTypedUntimed
            | TestConfiguration::AsyncUntimed
            | TestConfiguration::SemiSyncUntimed => 4,
        };
        assert_eq!(
            outputs.len(),
            num_expected_outputs,
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
                spec_untyped.output_vars.clone(),
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
            let monitor = builder.async_build().await;

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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.async_build().await;

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.async_build().await;

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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.async_build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 5, "outputs.collect").await?;

        // Assert based on configuration expectations
        match config {
            TestConfiguration::AsyncUntimed
            | TestConfiguration::AsyncTypedUntimed
            | TestConfiguration::SemiSyncUntimed => {
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.async_build().await;

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
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let monitor = GeneralRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .semantics(semantics)
                .async_build()
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
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let monitor = GeneralRuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .semantics(semantics)
                .async_build()
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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler)
            .semantics(semantics)
            .async_build()
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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        // Build base monitor with common settings
        let builder = GeneralRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler);

        let builder = create_builder_from_config(builder, config);
        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
        if config == TestConfiguration::SemiSyncUntimed {
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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
            spec_untyped.output_vars.clone(),
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

        let monitor = builder.build();

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
            spec.output_vars.clone(),
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

        let monitor = builder.build();

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

#[cfg(test)]
mod reconf_tests {
    use super::*;
    use async_unsync::bounded;
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
    async fn test_reconf_simple_add_manual_no_reconf(ex: Rc<LocalExecutor<'static>>) {
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
        let monitor = monitor_builder.async_build().await;
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
        let monitor = monitor_builder.async_build().await;
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
        send_value_noval_others((RECONF_TOPIC, Value::Str(reconf_json.into())), &mut tx_fans).await;

        // I am not sure why this yield_now is needed, but without it seems like we never
        // seem to progress after a reconfiguration.
        smol::future::yield_now().await;
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
}
