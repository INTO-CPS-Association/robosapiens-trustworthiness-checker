//! The code the documentation shows, compiled and run.
//!
//! Every `rust` block in `docs/` that demonstrates a call is included from this file by an
//! `ANCHOR` region, so the book cannot show an API that no longer exists. Editing a region here
//! changes the rendered documentation; renaming one breaks `mdbook build`.
//!
//! Each anchored region is a complete top-level function, including its imports, because the
//! include copies the region verbatim: a region inside a function body renders with that body's
//! indentation and without the paths a reader needs. The `#[test]` wrappers at the end run them.
//!
//! Most examples use the guide's running example, so the values a reader sees on the page are the
//! values these assertions check. `group_updates_into_ticks` does not: it needs two variables to
//! show a tick, and the running example has one input.

// ANCHOR: monitor_evaluate
fn evaluate_two_ticks() -> anyhow::Result<()> {
    use trustworthiness_checker::dataflow::DataflowMonitor;
    use trustworthiness_checker::{DsrvSpecification, Value, VarName};

    let source = "in x: Int\n\
        out alert: Bool\n\
        out total: Int\n\
        out scaled: Int\n\
        alert = total > 20\n\
        total = default(total[1], 0) + scaled\n\
        scaled = x * 2";
    let spec = source.parse::<DsrvSpecification>()?;
    let mut monitor = DataflowMonitor::compile_untyped(spec)?;
    let outputs = monitor.output_vars().to_vec();
    let output_index = |name: &str| {
        outputs
            .iter()
            .position(|variable| variable == &VarName::new(name))
            .expect("declared output")
    };
    let mut row = vec![Value::NoVal; outputs.len()];

    monitor.evaluate(&[Value::Int(4)], &mut row)?;
    assert_eq!(row[output_index("total")], Value::Int(8));
    assert_eq!(row[output_index("alert")], Value::Bool(false));

    monitor.evaluate(&[Value::Int(8)], &mut row)?;
    assert_eq!(row[output_index("total")], Value::Int(24));
    assert_eq!(row[output_index("alert")], Value::Bool(true));
    Ok(())
}
// ANCHOR_END: monitor_evaluate

// ANCHOR: dataflow_program_lifecycle
fn reuse_a_compiled_program_for_independent_traces() -> anyhow::Result<()> {
    use trustworthiness_checker::dataflow::{DataflowMonitor, DataflowProgram};
    use trustworthiness_checker::{DsrvSpecification, Value};

    let source = "in x: Int\n\
        out total: Int\n\
        total = default(total[1], 0) + x";
    let spec = source.parse::<DsrvSpecification>()?;
    let program = DataflowProgram::compile_untyped(spec)?;

    let mut first = DataflowMonitor::from_program(program.clone());
    let mut second = DataflowMonitor::from_program(program);
    let mut first_rows = Vec::new();
    first.evaluate_trace([[Value::Int(2)], [Value::Int(3)]], &mut first_rows)?;
    assert_eq!(first_rows, vec![vec![Value::Int(2)], vec![Value::Int(5)]]);

    let mut second_rows = Vec::new();
    second.evaluate_trace([[Value::Int(10)]], &mut second_rows)?;
    assert_eq!(second_rows, vec![vec![Value::Int(10)]]);

    first.reset();
    let mut restarted_rows = Vec::new();
    first.evaluate_trace([[Value::Int(3)]], &mut restarted_rows)?;
    assert_eq!(restarted_rows, vec![vec![Value::Int(3)]]);
    Ok(())
}
// ANCHOR_END: dataflow_program_lifecycle

// ANCHOR: reconfiguration_request
fn build_reconfiguration_request() -> anyhow::Result<()> {
    use trustworthiness_checker::io::ReconfigurationRequest;

    let replacement = "in x: Int\n\
        out alert: Bool\n\
        out total: Int\n\
        out scaled: Int\n\
        alert = total > 40\n\
        total = default(total[1], 0) + scaled\n\
        scaled = x * 2";

    let request = ReconfigurationRequest::new(replacement);
    request.validate()?;
    assert_eq!(request.specification, replacement);
    Ok(())
}
// ANCHOR_END: reconfiguration_request

// ANCHOR: input_batch_from_ticks
fn group_updates_into_ticks() -> anyhow::Result<()> {
    use trustworthiness_checker::{InputBatch, InputUpdate};

    let batch = InputBatch::from_ticks(vec![
        vec![InputUpdate::new("x".into(), 1)],
        vec![
            InputUpdate::new("x".into(), 2),
            InputUpdate::new("y".into(), 3),
        ],
    ])?;

    assert_eq!(batch.tick_count(), 2);
    assert_eq!(batch.update_count(), 3);
    Ok(())
}
// ANCHOR_END: input_batch_from_ticks

// ANCHOR: input_pipeline_build
fn read_configured_input_batches() -> anyhow::Result<()> {
    use std::collections::BTreeSet;

    use futures::StreamExt;
    use trustworthiness_checker::io::{InputPipeline, InputSource};
    use trustworthiness_checker::{InputBatch, Value, VarName};

    smol::block_on(async {
        let source = InputSource::in_memory_ticks([
            InputBatch::update("x", Value::Int(4)),
            InputBatch::update("x", Value::Int(8)),
        ]);
        let pipeline = InputPipeline::new(source);
        let mut input = pipeline.build(BTreeSet::from([VarName::new("x")])).await?;

        let first = input.next().await.expect("first configured batch")?;
        let second = input.next().await.expect("second configured batch")?;
        assert_eq!((first.tick_count(), second.tick_count()), (1, 1));
        assert!(input.next().await.is_none());
        Ok(())
    })
}
// ANCHOR_END: input_pipeline_build

// ANCHOR: input_window_batch
fn collect_ticks_without_merging_them() -> anyhow::Result<()> {
    use std::{collections::BTreeSet, num::NonZeroUsize};

    use futures::StreamExt;
    use trustworthiness_checker::io::{InputPipeline, InputPolicy, InputSource, InputWindow};
    use trustworthiness_checker::{InputBatch, InputUpdate, Value, VarName};

    smol::block_on(async {
        let source = InputSource::in_memory_ticks([
            InputBatch::tick(vec![
                InputUpdate::new("x".into(), Value::Int(1)),
                InputUpdate::new("y".into(), Value::Int(2)),
            ])?,
            InputBatch::update("x", Value::Int(3)),
        ]);
        let limits = InputWindow::new(None, NonZeroUsize::new(3))?;
        let pipeline = InputPipeline::new(source).with_policy(InputPolicy::Batch(limits))?;
        let mut input = pipeline
            .build(BTreeSet::from([VarName::new("x"), VarName::new("y")]))
            .await?;

        let batch = input.next().await.expect("collected batch")?;
        assert_eq!(batch.tick_count(), 2);
        assert_eq!(batch.update_count(), 3);
        assert!(input.next().await.is_none());
        Ok(())
    })
}
// ANCHOR_END: input_window_batch

// ANCHOR: output_pipeline_open
fn write_one_output_row() -> anyhow::Result<()> {
    use trustworthiness_checker::io::output::{
        OutputBackendConfig, OutputDestination, OutputPipeline,
    };
    use trustworthiness_checker::{OutputBatch, Value, VarName};

    smol::block_on(async {
        let destination =
            OutputDestination::<Value>::new("local-null", OutputBackendConfig::null());
        let pipeline = OutputPipeline::from_destination(destination)?;

        let resolved = pipeline.resolve(
            [
                VarName::new("alert"),
                VarName::new("total"),
                VarName::new("scaled"),
            ],
            std::iter::empty::<VarName>(),
            None,
        )?;
        let mut writer = pipeline.open(resolved).await?;

        writer
            .feed(OutputBatch::update("total", Value::Int(8)))
            .await?;
        writer.flush().await?;
        writer.close().await?;
        Ok(())
    })
}
// ANCHOR_END: output_pipeline_open

// ANCHOR: dsrv_operator_syntax
fn evaluate_revised_operator_syntax() -> anyhow::Result<()> {
    use trustworthiness_checker::dataflow::DataflowMonitor;
    use trustworthiness_checker::{DsrvSpecification, Value, VarName};

    let source = "in base: Int\n\
        in exponent: Int\n\
        out different: Bool\n\
        out signed_power: Int\n\
        out both_positive: Bool\n\
        different = base != exponent\n\
        signed_power = (-base) ** exponent\n\
        both_positive = base > 0 and exponent > 0";
    let spec = source.parse::<DsrvSpecification>()?;
    let mut monitor = DataflowMonitor::compile_untyped(spec)?;
    let outputs = monitor.output_vars().to_vec();
    let output_index = |name: &str| {
        outputs
            .iter()
            .position(|variable| variable == &VarName::new(name))
            .expect("declared output")
    };
    let mut row = vec![Value::NoVal; outputs.len()];

    monitor.evaluate(&[Value::Int(2), Value::Int(3)], &mut row)?;
    assert_eq!(row[output_index("different")], Value::Bool(true));
    assert_eq!(row[output_index("signed_power")], Value::Int(-8));
    assert_eq!(row[output_index("both_positive")], Value::Bool(true));
    Ok(())
}
// ANCHOR_END: dsrv_operator_syntax

// ANCHOR: dsrv_numeric_literals
fn parse_numeric_literal_syntax() -> anyhow::Result<()> {
    use trustworthiness_checker::lang::dsrv::ast::ExprView;
    use trustworthiness_checker::lang::dsrv::parser::parse_expr;
    use trustworthiness_checker::{CheckedDsrvSpecification, Value};

    let literals = [
        ("42", Value::Int(42)),
        ("0.5", Value::Float(0.5)),
        ("1.", Value::Float(1.0)),
        ("1e6", Value::Float(1_000_000.0)),
        ("1E-6", Value::Float(1e-6)),
        ("1.5e+3", Value::Float(1_500.0)),
    ];
    for (source, expected) in literals {
        let expression = parse_expr(source)?;
        let matches_expected = match (&expected, expression.as_ref().view()) {
            (Value::Int(expected), ExprView::Val(Value::Int(actual))) => *actual == *expected,
            (Value::Float(expected), ExprView::Val(Value::Float(actual))) => {
                actual.to_bits() == expected.to_bits()
            }
            _ => false,
        };
        assert!(matches_expected, "{source} parsed to an unexpected value");
    }

    for literal in ["1e2.3", "1.2e3.4"] {
        let source = format!("out result: Float\nresult = {literal}");
        assert!(
            source.parse::<CheckedDsrvSpecification>().is_err(),
            "{literal} must be rejected as a malformed numeric specification"
        );
    }
    Ok(())
}
// ANCHOR_END: dsrv_numeric_literals

// ANCHOR: dsrv_trailing_commas_and_list_get
fn evaluate_trailing_commas_and_list_get() -> anyhow::Result<()> {
    use trustworthiness_checker::dataflow::DataflowMonitor;
    use trustworthiness_checker::{DsrvSpecification, Value, VarName};

    let source = "in values: List<Int,>\n\
        out first: Int\n\
        out count: Int\n\
        first = List.get(values, 0,)\n\
        count = List.len(values,)";
    let spec = source.parse::<DsrvSpecification>()?;
    let mut monitor = DataflowMonitor::compile_untyped(spec)?;
    let outputs = monitor.output_vars().to_vec();
    let output_index = |name: &str| {
        outputs
            .iter()
            .position(|variable| variable == &VarName::new(name))
            .expect("declared output")
    };
    let mut row = vec![Value::NoVal; outputs.len()];

    monitor.evaluate(
        &[Value::List(vec![Value::Int(7), Value::Int(9)].into())],
        &mut row,
    )?;
    assert_eq!(row[output_index("first")], Value::Int(7));
    assert_eq!(row[output_index("count")], Value::Int(2));
    Ok(())
}
// ANCHOR_END: dsrv_trailing_commas_and_list_get

// ANCHOR: dsrv_else_if_chain
fn evaluate_else_if_chain() -> anyhow::Result<()> {
    use trustworthiness_checker::dataflow::DataflowMonitor;
    use trustworthiness_checker::{DsrvSpecification, Value, VarName};

    let source = "in temperature: Int\n\
        out level: Int\n\
        level = if temperature > 90 then 2\n\
                else if temperature > 70 then 1\n\
                else 0";
    let spec = source.parse::<DsrvSpecification>()?;
    let mut monitor = DataflowMonitor::compile_untyped(spec)?;
    let output = VarName::new("level");
    let mut row = vec![Value::NoVal];

    for (temperature, expected) in [(95, 2), (80, 1), (40, 0)] {
        monitor.evaluate(&[Value::Int(temperature)], &mut row)?;
        assert_eq!(
            row[monitor
                .output_vars()
                .iter()
                .position(|variable| variable == &output)
                .expect("declared output")],
            Value::Int(expected)
        );
    }
    Ok(())
}
// ANCHOR_END: dsrv_else_if_chain

#[test]
fn dataflow_monitor_evaluates_rows() {
    evaluate_two_ticks().expect("documented monitor example should run");
}

#[test]
fn dataflow_program_lifecycle_reuses_compilation_and_resets_state() {
    reuse_a_compiled_program_for_independent_traces()
        .expect("documented lifecycle example should run");
}

#[test]
fn reconfiguration_request_validates_structure() {
    build_reconfiguration_request().expect("documented request example should run");
}

#[test]
fn input_batch_groups_updates_into_ticks() {
    group_updates_into_ticks().expect("documented batch example should run");
}

#[test]
fn input_pipeline_yields_one_batch_per_configured_tick() {
    read_configured_input_batches().expect("documented input example should run");
}

#[test]
fn input_window_batches_ticks_without_merging_them() {
    collect_ticks_without_merging_them().expect("documented input-window example should run");
}

#[test]
fn output_pipeline_writes_a_row_and_closes() {
    write_one_output_row().expect("documented output example should run");
}

#[test]
fn revised_operator_reference_example_runs() {
    evaluate_revised_operator_syntax().expect("documented operator example should run");
}

#[test]
fn numeric_literal_reference_example_runs() {
    parse_numeric_literal_syntax().expect("documented literal example should run");
}

#[test]
fn trailing_comma_reference_example_runs() {
    evaluate_trailing_commas_and_list_get().expect("documented trailing-comma example should run");
}

#[test]
fn else_if_reference_example_runs() {
    evaluate_else_if_chain().expect("documented conditional example should run");
}
