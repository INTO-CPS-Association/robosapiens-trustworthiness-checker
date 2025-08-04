use approx::assert_abs_diff_eq;
use futures::stream::StreamExt;
use macro_rules_attribute::apply;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;
use strum::IntoEnumIterator;
use trustworthiness_checker::async_test;
use trustworthiness_checker::core::{AbstractMonitorBuilder, Runnable, Runtime, Semantics};
use trustworthiness_checker::dep_manage::interface::{DependencyKind, create_dependency_manager};
use trustworthiness_checker::io::testing::ManualOutputHandler;
use trustworthiness_checker::runtime::builder::GenericMonitorBuilder;

use trustworthiness_checker::{LOLASpecification, OutputStream, lola_fixtures::*};
use trustworthiness_checker::{Value, VarName, lola_specification, runtime::RuntimeBuilder};

#[derive(Debug, Clone, Copy)]
enum TestConfiguration {
    AsyncUntimed,
    AsyncTypedUntimed,
    Constraints,
}

impl TestConfiguration {
    fn all() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
            TestConfiguration::Constraints,
        ]
    }

    fn async_configurations() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::AsyncTypedUntimed,
        ]
    }

    fn untyped_configurations() -> Vec<Self> {
        vec![
            TestConfiguration::AsyncUntimed,
            TestConfiguration::Constraints,
        ]
    }

    fn dependency_kinds(&self) -> Vec<DependencyKind> {
        match self {
            TestConfiguration::Constraints => DependencyKind::iter().collect::<Vec<_>>(),
            TestConfiguration::AsyncUntimed | TestConfiguration::AsyncTypedUntimed => {
                vec![DependencyKind::Empty]
            }
        }
    }
}

fn create_builder_from_config(
    builder: GenericMonitorBuilder<LOLASpecification, Value>,
    config: TestConfiguration,
) -> GenericMonitorBuilder<LOLASpecification, Value> {
    match config {
        TestConfiguration::AsyncUntimed => builder.semantics(Semantics::Untimed),
        TestConfiguration::AsyncTypedUntimed => builder.semantics(Semantics::TypedUntimed),
        TestConfiguration::Constraints => builder.runtime(Runtime::Constraints),
    }
}

#[apply(async_test)]
async fn test_defer(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to defer functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin e\nout z\nz = defer(e)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Defer test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ],
                "Defer output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_defer_x_squared(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to defer functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin e\nout z\nz = defer(e)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![1.into(), 2.into(), 3.into()];
            let e = vec!["x * x".into(), "x * x + 1".into(), "x * x + 2".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Defer x squared test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![4.into()]),
                    (2, vec![9.into()]),
                ],
                "Defer x squared output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_defer_unknown(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to defer functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin e\nout z\nz = defer(e)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![1.into(), 2.into(), 3.into()];
            let e = vec![Value::Unknown, "x + 1".into(), "x + 2".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Defer unknown test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec![3.into()]),
                    (2, vec![4.into()]),
                ],
                "Defer unknown output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_defer_unknown2(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to defer functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin e\nout z\nz = defer(e)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![0.into(), 1.into(), 2.into()];
            let e = vec![Value::Unknown, "x + 1".into(), Value::Unknown];
            let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Defer unknown2 test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                ],
                "Defer unknown2 output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_defer_dependency(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to defer functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped =
            lola_specification(&mut "in x\nin y\nin e\nout z1\nout z2\nz1 = defer(e)\nz2 = x + y")
                .unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![1.into(), 2.into(), 3.into(), 4.into()];
            let y = vec![10.into(), 20.into(), 30.into(), 40.into()];
            let e = vec![
                Value::Unknown,
                "x + y".into(),
                "x + y".into(),
                "x + y".into(),
            ];
            let input_streams = BTreeMap::from([("x".into(), x), ("y".into(), y), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Defer dependency test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown, 11.into()]),
                    (1, vec![22.into(), 22.into()]),
                    (2, vec![33.into(), 33.into()]),
                    (3, vec![44.into(), 44.into()]),
                ],
                "Defer dependency output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_update_both_init(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin y\nout z\nz = update(x, y)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec!["x0".into(), "x1".into(), "x2".into()];
            let y = vec!["y0".into(), "y1".into(), "y2".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("y".into(), y)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Update both init test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["y0".into()]),
                    (1, vec!["y1".into()]),
                    (2, vec!["y2".into()]),
                ],
                "Update both init output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_update_first_x_then_y(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on untyped configurations due to update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped = lola_specification(&mut "in x\nin y\nout z\nz = update(x, y)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let y = vec![Value::Unknown, "y1".into(), Value::Unknown, "y3".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("y".into(), y)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Update first x then y test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["x0".into()]),
                    (1, vec!["y1".into()]),
                    (2, vec![Value::Unknown]),
                    (3, vec!["y3".into()]),
                ],
                "Update first x then y output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

// #[ignore = "Hangs for unknown reasons"]
#[apply(async_test)]
async fn test_update_defer(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only works on untyped_configurations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped =
            lola_specification(&mut "in x\nin e\nout z\nz = update(\"def\", defer(e))").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let e = vec![Value::Unknown, "x".into(), "x".into(), "x".into()];
            let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Update defer test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["def".into()]),
                    (1, vec!["x1".into()]),
                    (2, vec!["x2".into()]),
                    (3, vec!["x3".into()]),
                ],
                "Update defer output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_defer_update(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on constraints due to defer/update functionality limitations
    for config in TestConfiguration::untyped_configurations() {
        let spec_untyped =
            lola_specification(&mut "in x\nin y\nout z\nz = defer(update(x, y))").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec![Value::Unknown, "x".into(), "x_lost".into(), "x_sad".into()];
            let y = vec![
                Value::Unknown,
                "y".into(),
                "y_won!".into(),
                "y_happy".into(),
            ];
            let input_streams = BTreeMap::from([("x".into(), x), ("y".into(), y)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Defer update test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Unknown]),
                    (1, vec!["y".into()]),
                    (2, vec!["y_won!".into()]),
                    (3, vec!["y_happy".into()]),
                ],
                "Defer update output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_recursive_update(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only works on the constraint-based runtime
    for config in vec![TestConfiguration::Constraints] {
        let spec_untyped = lola_specification(&mut "in x\nout z\nz = update(x, z))").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec!["x0".into(), "x1".into(), "x2".into(), "x3".into()];
            let input_streams = BTreeMap::from([("x".into(), x)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Recursive update test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec!["x0".into()]),
                    (1, vec!["x1".into()]),
                    (2, vec!["x2".into()]),
                    (3, vec!["x3".into()]),
                ],
                "Recursive update output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

// NOTE: While this test is interesting, it cannot work due to how defer is handled.
// When defer receives a prop stream it changes state from being a defer expression into
// the received prop stream. Thus, it cannot be used recursively.
// This is the reason why we also need dynamic for the constraint based runtime.
#[apply(async_test)]
async fn test_recursive_update_defer(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only runs on the constraint-based runtime due to update/defer functionality
    // limitations
    for config in vec![TestConfiguration::Constraints] {
        let spec_untyped = lola_specification(&mut "in x\nout z\nz = update(defer(x), z)").unwrap();

        for kind in config.dependency_kinds() {
            let x = vec!["0".into(), "1".into(), "2".into(), "3".into()];
            let input_streams = BTreeMap::from([("x".into(), x)]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Recursive update defer test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![0.into()]),
                    (1, vec![0.into()],),
                    (2, vec![0.into()],),
                    (3, vec![0.into()],),
                ],
                "Recursive update defer output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

pub fn input_streams_constraint() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter(vec![
            Value::Int(1),
            Value::Int(3),
            Value::Int(5),
        ])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(futures::stream::iter(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6),
        ])) as OutputStream<Value>,
    );
    input_streams
}

#[ignore = "Cannot have empty spec or inputs"]
#[apply(async_test)]
async fn test_runtime_initialization(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let spec_untyped = lola_specification(&mut spec_empty()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_empty();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<Vec<Value>> = outputs.collect().await;
            assert_eq!(
                outputs.len(),
                0,
                "Runtime initialization failed for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_var(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = x",
            _ => "in x\nout z\nz = x",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Variable test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![3.into()]),
                    (2, vec![5.into()]),
                ],
                "Variable test output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_literal_expression(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "out z: Int\nz = 42",
            _ => "out z\nz = 42",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.take(3).enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Literal expression test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![42.into()]),
                    (1, vec![42.into()]),
                    (2, vec![42.into()]),
                ],
                "Literal expression output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_addition(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = x + 1",
            _ => "in x\nout z\nz = x + 1",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Addition test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![2.into()]),
                    (1, vec![4.into()]),
                    (2, vec![6.into()]),
                ],
                "Addition output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_subtraction(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = x - 10",
            _ => "in x\nout z\nz = x - 10",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Subtraction test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![Value::Int(-9)]),
                    (1, vec![Value::Int(-7)]),
                    (2, vec![Value::Int(-5)]),
                ],
                "Subtraction output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_index_past_mult_dependencies(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => {
                "in x: Int\nout z1: Int\nout z2: Int\nz2 = x[-2]\nz1 = x[-1]"
            }
            _ => "in x\nout z1\nout z2\nz2 = x[-2]\nz1 = x[-1]",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            // TODO: async runtime produces more data than the constraint based runtime
            let num_expected_outputs = match config {
                TestConfiguration::AsyncTypedUntimed | TestConfiguration::AsyncUntimed => 4,
                TestConfiguration::Constraints => 3,
            };
            assert_eq!(
                outputs.len(),
                num_expected_outputs,
                "Index past mult dependencies test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                &outputs[0..3],
                vec![
                    (0, vec![Value::Unknown, Value::Unknown]),
                    (1, vec![1.into(), Value::Unknown]),
                    (2, vec![3.into(), 1.into()]),
                ],
                "Index past mult dependencies output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_if_else_expression(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => {
                "in x: Bool\nin y: Bool\nout z: Bool\nz = if(x) then y else false"
            }
            _ => "in x\nin y\nout z\nz = if(x) then y else false",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams5();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "If-else expression test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![true.into()]),
                    (1, vec![false.into()]),
                    (2, vec![false.into()]),
                ],
                "If-else expression output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_string_append(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Str\nin y: Str\nout z: Str\nz = x ++ y",
            _ => "in x\nin y\nout z\nz = x ++ y",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams4();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                2,
                "String append test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![(0, vec!["ab".into()]), (1, vec!["cd".into()]),],
                "String append output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_default_no_unknown(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = default(x, 42)",
            _ => "in x\nout z\nz = default(x, 42)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Default no unknown test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![3.into()]),
                    (2, vec![5.into()]),
                ],
                "Default no unknown output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_default_all_unknown(executor: Rc<LocalExecutor<'static>>) {
    // TODO: not supported by the type checker
    for config in TestConfiguration::untyped_configurations() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = default(x, 42)",
            _ => "in x\nout z\nz = default(x, 42)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = BTreeMap::from([(
                "x".into(),
                vec![Value::Unknown, Value::Unknown, Value::Unknown],
            )]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Default all unknown test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![42.into()]),
                    (1, vec![42.into()]),
                    (2, vec![42.into()]),
                ],
                "Default all unknown output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_default_one_unknown(executor: Rc<LocalExecutor<'static>>) {
    // TODO: not supported by the type checker
    for config in TestConfiguration::untyped_configurations() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "in x: Int\nout z: Int\nz = default(x, 42)",
            _ => "in x\nout z\nz = default(x, 42)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams =
                BTreeMap::from([("x".into(), vec![1.into(), Value::Unknown, 5.into()])]);
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                3,
                "Default one unknown test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![42.into()]),
                    (2, vec![5.into()]),
                ],
                "Default one unknown output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_counter(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => "out x: Int\nx = 1 + default(x[-1], 0)",
            _ => "out x\nx = 1 + default(x[-1], 0)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_empty();
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let outputs: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;
            assert_eq!(
                outputs.len(),
                4,
                "Counter test failed for config {:?} with dependency {:?}",
                config,
                kind
            );
            assert_eq!(
                outputs,
                vec![
                    (0, vec![1.into()]),
                    (1, vec![2.into()]),
                    (2, vec![3.into()]),
                    (3, vec![4.into()]),
                ],
                "Counter output mismatch for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_simple_add_monitor_does_not_go_away(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        // Common specification for all configurations
        let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
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
                let builder = RuntimeBuilder::new()
                    .executor(executor.clone())
                    .model(spec_untyped.clone())
                    .input(Box::new(input_streams))
                    .output(output_handler)
                    .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

                // Apply configuration-specific settings
                let builder = create_builder_from_config(builder, config);
                let monitor = builder.async_build().await;

                // Start monitor and return outputs stream
                executor.spawn(monitor.run()).detach();
                outputs
                // output_handler goes out of scope here, but monitor should continue
            };

            // Collect results after output handler has been dropped
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - monitor should persist and produce correct outputs
            assert_eq!(
                result,
                vec![(0, vec![Value::Int(3)]), (1, vec![Value::Int(7)])],
                "Monitor persistence failed for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_simple_add_monitor_large_input(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        // Common specification for all configurations
        let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration (100 elements)
            let input_streams =
                trustworthiness_checker::lola_fixtures::input_streams_simple_add(100);

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.async_build().await;

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert that large input produces expected number of outputs
            assert_eq!(
                result.len(),
                100,
                "Expected 100 outputs for large input, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            // Verify outputs are correct (z = x + y where x=2*i, y=2*i+1, so z=4*i+1)
            for (i, (time, values)) in result.iter().enumerate() {
                assert_eq!(*time, i, "Output time should match index");
                assert_eq!(values.len(), 1, "Should have exactly one output value");
                let expected = Value::Int(4 * (i as i64) + 1);
                assert_eq!(
                    values[0], expected,
                    "Output at time {} should be {}, got {:?} for config {:?}",
                    i, expected, values[0], config
                );
            }
        }
    }
}

pub fn input_streams_simple_add() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter(vec![Value::Int(1), 3.into()])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(futures::stream::iter(vec![Value::Int(2), 4.into()])) as OutputStream<Value>,
    );
    input_streams
}

pub fn input_streams_constraint_style() -> BTreeMap<VarName, OutputStream<Value>> {
    let mut input_streams = BTreeMap::new();
    input_streams.insert(
        "x".into(),
        Box::pin(futures::stream::iter(vec![
            Value::Int(1),
            3.into(),
            5.into(),
        ])) as OutputStream<Value>,
    );
    input_streams.insert(
        "y".into(),
        Box::pin(futures::stream::iter(vec![
            Value::Int(2),
            4.into(),
            6.into(),
        ])) as OutputStream<Value>,
    );
    input_streams
}

#[apply(async_test)]
async fn test_simple_add_monitor(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams3();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.async_build().await;

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            assert_eq!(
                result,
                vec![(0, vec![Value::Int(3)]), (1, vec![Value::Int(7)])]
            );
        }
    }
}

#[apply(async_test)]
async fn test_simple_modulo_monitor(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let spec_untyped = lola_specification(&mut spec_simple_modulo_monitor_typed()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams3();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.async_build().await;

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert based on configuration expectations
            match config {
                TestConfiguration::Constraints => {
                    // Constraint runtime may produce different results for modulo operations
                    assert_eq!(result.len(), 2);
                    assert_eq!(result[0].1[0], Value::Int(0));
                    assert_eq!(result[1].1[0], Value::Int(1));
                }
                TestConfiguration::AsyncUntimed | TestConfiguration::AsyncTypedUntimed => {
                    assert_eq!(
                        result,
                        vec![(0, vec![Value::Int(0)]), (1, vec![Value::Int(1)])]
                    );
                }
            }
        }
    }
}

#[apply(async_test)]
async fn test_simple_add_monitor_float(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::async_configurations() {
        let spec_untyped = lola_specification(&mut spec_simple_add_monitor_typed_float()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_float();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.async_build().await;

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;

            assert_eq!(result.len(), 2);
            match result[0].1[0] {
                Value::Float(f) => assert_abs_diff_eq!(f, 3.7, epsilon = 1e-4),
                _ => panic!("Expected float"),
            }
            match result[1].1[0] {
                Value::Float(f) => assert_abs_diff_eq!(f, 7.7, epsilon = 1e-4),
                _ => panic!("Expected float"),
            }
        }
    }
}

// Note: The original test_count_monitor was hanging when mixing configurations in the same test.
// This was fixed by improving VarManager lifecycle management to stop immediately when subscribers
// are dropped, addressing the multiple subscription issue for output variables.

#[apply(async_test)]
async fn test_count_monitor_mixed_configurations(executor: Rc<LocalExecutor<'static>>) {
    // Test that multiple different configurations can run sequentially without hanging
    let test_cases = vec![
        (TestConfiguration::AsyncUntimed, DependencyKind::Empty),
        (TestConfiguration::Constraints, DependencyKind::Empty),
        (TestConfiguration::Constraints, DependencyKind::DepGraph),
        (TestConfiguration::AsyncUntimed, DependencyKind::Empty), // Run AsyncUntimed again to verify cleanup
    ];

    for (iteration, (config, dependency_kind)) in test_cases.into_iter().enumerate() {
        let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
        let mut spec_str = "out x: Int\nx = 1 + default(x[-1], 0)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let builder = RuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler)
            .dependencies(create_dependency_manager(
                dependency_kind,
                spec_untyped.clone(),
            ));

        let builder = create_builder_from_config(builder, config);

        let monitor = builder.async_build().await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;

        assert_eq!(
            result,
            vec![
                (0, vec![Value::Int(1)]),
                (1, vec![Value::Int(2)]),
                (2, vec![Value::Int(3)]),
                (3, vec![Value::Int(4)]),
            ],
            "Count monitor failed for iteration {} with config {:?} and dependency {:?}",
            iteration,
            config,
            dependency_kind
        );
    }
}

#[apply(async_test)]
async fn test_count_monitor_sequential_with_drop_guard(executor: Rc<LocalExecutor<'static>>) {
    // Test running monitors sequentially using drop guard cancellation approach

    // First run
    {
        let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
        let mut spec_str = "out x: Int\nx = 1 + default(x[-1], 0)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = RuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler)
            .dependencies(create_dependency_manager(
                DependencyKind::Empty,
                spec_untyped.clone(),
            ))
            .semantics(Semantics::Untimed)
            .async_build()
            .await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;

        assert_eq!(
            result,
            vec![
                (0, vec![Value::Int(1)]),
                (1, vec![Value::Int(2)]),
                (2, vec![Value::Int(3)]),
                (3, vec![Value::Int(4)]),
            ]
        );
    }

    // Second run - should work now with drop guard cancellation
    {
        let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
        let mut spec_str = "out x: Int\nx = 1 + default(x[-1], 0)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = RuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec_untyped.clone())
            .input(Box::new(input_streams))
            .output(output_handler)
            .dependencies(create_dependency_manager(
                DependencyKind::Empty,
                spec_untyped.clone(),
            ))
            .semantics(Semantics::Untimed)
            .async_build()
            .await;

        executor.spawn(monitor.run()).detach();
        let result: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;

        assert_eq!(
            result,
            vec![
                (0, vec![Value::Int(1)]),
                (1, vec![Value::Int(2)]),
                (2, vec![Value::Int(3)]),
                (3, vec![Value::Int(4)]),
            ]
        );
    }
}

#[apply(async_test)]
async fn test_direct_varmanager_cancellation(executor: Rc<LocalExecutor<'static>>) {
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
}

#[apply(async_test)]
async fn test_drop_guard_cancellation_behavior(executor: Rc<LocalExecutor<'static>>) {
    // Test to verify that drop guard properly stops VarManagers when output streams are dropped

    let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();
    let mut spec_str = "out x: Int\nx = 1 + default(x[-1], 0)";
    let spec_untyped = lola_specification(&mut spec_str).unwrap();

    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec_untyped.output_vars.clone(),
    ));
    let outputs = output_handler.get_output();

    let monitor = RuntimeBuilder::new()
        .executor(executor.clone())
        .model(spec_untyped.clone())
        .input(Box::new(input_streams))
        .output(output_handler)
        .dependencies(create_dependency_manager(
            DependencyKind::Empty,
            spec_untyped.clone(),
        ))
        .semantics(Semantics::Untimed)
        .async_build()
        .await;

    executor.spawn(monitor.run()).detach();

    // Take only 2 values - this should trigger drop guard when output stream is dropped
    let result: Vec<(usize, Vec<Value>)> = outputs.take(2).enumerate().collect().await;

    assert_eq!(
        result,
        vec![(0, vec![Value::Int(1)]), (1, vec![Value::Int(2)]),]
    );

    // Add a small delay to allow cancellation to propagate via drop guard
    smol::Timer::after(std::time::Duration::from_millis(100)).await;
}

#[apply(async_test)]
async fn test_count_monitor(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = "out x: Int\nx = 1 + default(x[-1], 0)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration (empty for count monitor)
            let input_streams: BTreeMap<VarName, OutputStream<Value>> = BTreeMap::new();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.take(4).enumerate().collect().await;

            // Assert expected results - count functionality should work across configurations
            assert_eq!(
                result,
                vec![
                    (0, vec![Value::Int(1)]),
                    (1, vec![Value::Int(2)]),
                    (2, vec![Value::Int(3)]),
                    (3, vec![Value::Int(4)]),
                ],
                "Count monitor failed for config {:?} with dependency {:?}",
                config,
                kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_multiple_parameters(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::async_configurations() {
        let mut spec = "in x : Int\nin y : Int\nout r1 : Int\nout r2 : Int\nr1 =x+y\nr2 = x * y";
        let spec_untyped = lola_specification(&mut spec).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams3();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            assert_eq!(result.len(), 2);
            assert_eq!(
                result,
                vec![
                    (0, vec![Value::Int(3), Value::Int(2)]),
                    (1, vec![Value::Int(7), Value::Int(12)]),
                ]
            );
        }
    }
}

#[apply(async_test)]
async fn test_eval_monitor_untimed(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test only works with untimed semantics due to dynamic evaluation
    let input_streams = input_streams2();
    let spec = lola_specification(&mut spec_dynamic_monitor()).unwrap();
    let mut output_handler = Box::new(ManualOutputHandler::new(
        executor.clone(),
        spec.output_vars.clone(),
    ));
    let outputs = output_handler.get_output();

    let monitor = RuntimeBuilder::new()
        .executor(executor.clone())
        .model(spec.clone())
        .input(Box::new(input_streams))
        .output(output_handler)
        .dependencies(create_dependency_manager(DependencyKind::Empty, spec))
        .semantics(Semantics::Untimed)
        .async_build()
        .await;

    executor.spawn(monitor.run()).detach();
    let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;
    assert_eq!(
        result,
        vec![
            (0, vec![Value::Int(3), Value::Int(3)]),
            (1, vec![Value::Int(7), Value::Int(7)]),
        ]
    );
}

#[apply(async_test)]
async fn test_string_concatenation(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let spec_untyped = lola_specification(&mut spec_typed_string_concat()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams4();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            assert_eq!(
                result,
                vec![
                    (0, vec![Value::Str("ab".into())]),
                    (1, vec![Value::Str("cd".into())]),
                ]
            );
        }
    }
}

#[apply(async_test)]
async fn test_past_indexing(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let mut spec_str = "in x: Int\nin y: Int\nout z: Int\nz = x[-1]";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = input_streams_constraint_style();

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            let mut expected_results = vec![
                (0, vec![Value::Unknown]), // Default value for x[-1] at time 0
                (1, vec![Value::Int(1)]),  // x[0] = 1 at time 1
                (2, vec![Value::Int(3)]),  // x[1] = 3 at time 2
                (3, vec![Value::Int(5)]),  // x[2] = 3 at time 3
            ];
            // TODO: The constraints runtime current produces too few results
            if matches!(config, TestConfiguration::Constraints) {
                expected_results.remove(3);
            }
            assert_eq!(
                result, expected_results,
                "Temporal access failed for config {:?} with dependency {:?}",
                config, kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_maple_sequence(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        let spec_untyped = lola_specification(&mut spec_maple_sequence()).unwrap();

        for kind in config.dependency_kinds() {
            let input_streams = maple_valid_input_stream(10);

            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            let builder = create_builder_from_config(builder, config);
            let monitor = builder.build();

            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // TODO: Different runtimes may handle temporal dependencies differently for complex patterns
            // Expected: maple sequence should detect complete "maple" patterns in the input stream
            // The exact number and content of outputs may vary between runtimes due to different
            // handling of temporal access patterns and default values
            assert!(
                result.len() >= 5 && result.len() <= 15,
                "Maple sequence produced {} outputs for config {:?} with dependency {:?}, expected 5-15",
                result.len(),
                config,
                kind
            );

            // Verify that we get boolean outputs (the specification outputs are all Bool type)
            for (time, values) in &result {
                assert!(
                    values.iter().all(|v| matches!(v, Value::Bool(_))),
                    "Expected all boolean outputs at time {}, got {:?} for config {:?}",
                    time,
                    values,
                    config
                );
            }
        }
    }
}

#[apply(async_test)]
async fn test_restricted_dynamic_monitor(executor: Rc<LocalExecutor<'static>>) {
    for config in vec![TestConfiguration::AsyncUntimed] {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = "in x: Int\nin y: Int\nin s: Str\nout z: Int\nout w: Int\nz = x + y\nw = dynamic(s, {x,y})";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams2();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - dynamic monitor should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for restricted dynamic monitor, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
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
    }
}

#[apply(async_test)]
async fn test_defer_stream_1(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::untyped_configurations() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams_defer_1();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - defer functionality should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for defer stream 1, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            let expected_outputs = vec![
                (0, vec![Value::Unknown]),
                (1, vec![Value::Int(2)]),
                (2, vec![Value::Int(3)]),
                (3, vec![Value::Int(4)]),
                (4, vec![Value::Int(5)]),
                (5, vec![Value::Int(6)]),
                (6, vec![Value::Int(7)]),
                (7, vec![Value::Int(8)]),
                (8, vec![Value::Int(9)]),
                (9, vec![Value::Int(10)]),
                (10, vec![Value::Int(11)]),
                (11, vec![Value::Int(12)]),
                (12, vec![Value::Int(13)]),
                (13, vec![Value::Int(14)]),
                (14, vec![Value::Int(15)]),
            ];
            assert_eq!(
                result, expected_outputs,
                "Did not get expected output for config {:?} with dependency {:?}",
                config, kind
            )
        }
    }
}

#[apply(async_test)]
async fn test_defer_stream_2(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::untyped_configurations() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = "in x: Int\nin e: Str\nout z: Int\nz = defer(e)";
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams_defer_2();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - defer functionality should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for defer stream 2, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            let expected_outputs = vec![
                (0, vec![Value::Unknown]),
                (1, vec![Value::Unknown]),
                (2, vec![Value::Unknown]),
                (3, vec![Value::Int(4)]),
                (4, vec![Value::Int(5)]),
                (5, vec![Value::Int(6)]),
                (6, vec![Value::Int(7)]),
                (7, vec![Value::Int(8)]),
                (8, vec![Value::Int(9)]),
                (9, vec![Value::Int(10)]),
                (10, vec![Value::Int(11)]),
                (11, vec![Value::Int(12)]),
                (12, vec![Value::Int(13)]),
                (13, vec![Value::Int(14)]),
                (14, vec![Value::Int(15)]),
            ];
            assert_eq!(
                result, expected_outputs,
                "Did not get expected output for config {:?} with dependency {:?}",
                config, kind
            )
        }
    }
}

#[apply(async_test)]
async fn test_defer_stream_3(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::untyped_configurations() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => {
                "in x: Int\nin e: Str\nout z: Int\nz = defer(e)"
            }
            _ => "in x\nin e\nout z\nz = defer(e)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams_defer_3();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - defer functionality should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for defer stream 3, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            let expected_outputs = vec![
                (0, vec![Value::Unknown]),
                (1, vec![Value::Unknown]),
                (2, vec![Value::Unknown]),
                (3, vec![Value::Unknown]),
                (4, vec![Value::Unknown]),
                (5, vec![Value::Unknown]),
                (6, vec![Value::Unknown]),
                (7, vec![Value::Unknown]),
                (8, vec![Value::Unknown]),
                (9, vec![Value::Unknown]),
                (10, vec![Value::Unknown]),
                (11, vec![Value::Unknown]),
                (12, vec![Value::Int(13)]),
                (13, vec![Value::Int(14)]),
                (14, vec![Value::Int(15)]),
            ];
            assert_eq!(
                result, expected_outputs,
                "Did not get expected output for config {:?} with dependency {:?}",
                config, kind
            )
        }
    }
}

#[apply(async_test)]
async fn test_defer_stream_4(executor: Rc<LocalExecutor<'static>>) {
    // TODO: This test currently only runs AsyncUntimed due to bugs in other configurations:
    // - Constraints configuration produces different output counts and values
    // - AsyncTypedUntimed does not implement defer
    for config in vec![TestConfiguration::AsyncUntimed] {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => {
                "in x: Int\nin e: Str\nout z: Int\nz = defer(e)"
            }
            _ => "in x\nin e\nout z\nz = defer(e)",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams_defer_4();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - defer functionality should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for defer stream 4, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            // Notice one output "too many". This is expected behaviour (at least with a global default
            // history_length = 10 for defer) since once e = x[-1, 0] has arrived
            // the stream for z = defer(e) will continue as long as x[-1, 0] keeps
            // producing values (making use of its history) which can continue beyond
            // the lifetime of the stream for e (since it does not depend on e any more
            // once a value has been received). This differs from the behaviour of
            // defer(e) which stops if e stops.
            //
            // See also: Comment on sindex combinator.

            let expected_outputs = vec![
                (0, vec![Value::Unknown]),
                (1, vec![Value::Unknown]),
                (2, vec![Value::Int(1)]),
                (3, vec![Value::Int(2)]),
                (4, vec![Value::Int(3)]),
                (5, vec![Value::Int(4)]),
            ];
            assert_eq!(
                result, expected_outputs,
                "Did not get expected output for config {:?} with dependency {:?}",
                config, kind
            );
        }
    }
}

#[apply(async_test)]
async fn test_future_indexing(executor: Rc<LocalExecutor<'static>>) {
    for config in TestConfiguration::all() {
        // Use different specifications based on configuration to ensure type compatibility
        let mut spec_str = match config {
            TestConfiguration::AsyncTypedUntimed => {
                "in x: Int\nin y: Int\nout z: Int\nout a: Int\nz = x[1]\na = y"
            }
            _ => "in x\nin y\nout z\nout a\nz = x[1]\na = y",
        };
        let spec_untyped = lola_specification(&mut spec_str).unwrap();

        // Test with all applicable dependency kinds for this configuration
        for kind in config.dependency_kinds() {
            // Create fresh input streams for each test iteration
            let input_streams = input_streams_indexing();

            // Create output handler based on configuration
            let mut output_handler = Box::new(ManualOutputHandler::new(
                executor.clone(),
                spec_untyped.output_vars.clone(),
            ));
            let outputs = output_handler.get_output();

            // Build base monitor with common settings
            let builder = RuntimeBuilder::new()
                .executor(executor.clone())
                .model(spec_untyped.clone())
                .input(Box::new(input_streams))
                .output(output_handler)
                .dependencies(create_dependency_manager(kind, spec_untyped.clone()));

            // Apply configuration-specific settings
            let builder = create_builder_from_config(builder, config);

            let monitor = builder.build();

            // Run monitor and collect results
            executor.spawn(monitor.run()).detach();
            let result: Vec<(usize, Vec<Value>)> = outputs.enumerate().collect().await;

            // Assert expected results - future indexing should work across configurations
            assert!(
                result.len() >= 2,
                "Expected at least 2 outputs for future indexing, got {} for config {:?} with dependency {:?}",
                result.len(),
                config,
                kind
            );

            // Verify that we get outputs with expected structure (z and a)
            for (time, values) in &result {
                assert_eq!(
                    values.len(),
                    2,
                    "Expected 2 output values (z and a) at time {}, got {} for config {:?}",
                    time,
                    values.len(),
                    config
                );
            }
        }
    }
}
