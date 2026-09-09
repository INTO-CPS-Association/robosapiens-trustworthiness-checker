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

#[test]
fn dataflow_monitor_evaluates_rows() {
    evaluate_two_ticks().expect("documented monitor example should run");
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
