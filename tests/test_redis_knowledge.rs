//! API-level Redis knowledge-state tests.
//!
//! These tests use the real `InputSource`/`InputSources`/`InputPipeline` path.
//! They never launch the Trustworthiness Checker executable.

#![cfg(feature = "testcontainers")]

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};
use std::rc::Rc;

use futures::StreamExt;
use macro_rules_attribute::apply;
use redis::AsyncCommands;
use smol::LocalExecutor;

use tc_testutils::redis::start_redis;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::DsrvSpecification;
use trustworthiness_checker::async_test;
use trustworthiness_checker::core::REDIS_HOSTNAME;
use trustworthiness_checker::io::{
    InputPipeline, InputPolicy, InputReduction, InputSource, InputSources, InputWindow,
    OutputBackendConfig, OutputPipeline, RedisKnowledgeConfig, RetryPolicy, Route,
};

use trustworthiness_checker::runtime::builder::GeneralRuntimeBuilder;
use trustworthiness_checker::{InputBatch, InputUpdate, Runtime, Value, VarName};

fn key(prefix: &str, name: &str) -> String {
    format!("tc:redis-knowledge:{prefix}:{name}")
}

async fn redis_connection(port: u16) -> anyhow::Result<redis::aio::MultiplexedConnection> {
    let client = redis::Client::open(format!("redis://{REDIS_HOSTNAME}:{port}/2"))?;
    Ok(client.get_multiplexed_async_connection().await?)
}

async fn enable_keyspace_notifications(
    connection: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<()> {
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("KEA")
        .query_async(connection)
        .await?;
    Ok(())
}

async fn next_batch(
    input: &mut (
             impl futures::Stream<Item = Result<InputBatch<Value>, trustworthiness_checker::InputError>>
             + Unpin
         ),
    label: &str,
) -> anyhow::Result<InputBatch<Value>> {
    with_timeout(input.next(), 5, label)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{label} ended"))?
        .map_err(Into::into)
}

fn knowledge_config(
    port: u16,
    publish_initial: bool,
    mappings: impl IntoIterator<Item = (VarName, String)>,
) -> RedisKnowledgeConfig {
    RedisKnowledgeConfig {
        host: REDIS_HOSTNAME.to_owned(),
        port: Some(port),
        database: 2,
        publish_initial,
        keys: mappings.into_iter().collect(),
        retry: RetryPolicy::new(
            trustworthiness_checker::io::RetryLimit::Attempts(NonZeroU32::new(3).unwrap()),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
        )
        .unwrap(),
    }
}

fn singleton_update(batch: &InputBatch<Value>) -> anyhow::Result<InputUpdate<Value>> {
    anyhow::ensure!(
        batch.tick_count() == 1,
        "expected one logical tick, got {batch:?}"
    );
    let tick = batch
        .ticks()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected a logical tick"))?;
    anyhow::ensure!(tick.len() == 1, "expected an independent singleton tick");
    Ok(tick
        .to_updates()
        .into_iter()
        .next()
        .expect("length checked"))
}

#[apply(async_test)]
async fn redis_knowledge_initial_snapshot_is_ordered_singleton_ticks(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    let prefix = uuid::Uuid::new_v4().to_string();
    let mode_key = key(&prefix, "mode");
    let plan_key = key(&prefix, "plan");
    let missing_key = key(&prefix, "missing");
    let ignored_key = key(&prefix, "ignored");
    let _: () = connection.set(&mode_key, "{mode: 'active'}").await?;
    let _: () = connection.set(&plan_key, "['inspect', 'repair']").await?;
    let _: () = connection.set(&ignored_key, "true").await?;

    let variables = BTreeSet::from([
        VarName::new("knowledge.mode"),
        VarName::new("knowledge.plan"),
        VarName::new("knowledge.missing"),
    ]);
    let config = knowledge_config(
        port,
        true,
        [
            (VarName::new("knowledge.mode"), mode_key),
            (VarName::new("knowledge.plan"), plan_key),
            (VarName::new("knowledge.missing"), missing_key),
        ],
    );
    let mut input = InputPipeline::new(InputSource::redis_knowledge(config))
        .with_executor(Rc::clone(&executor))
        .build(variables)
        .await?;

    let batch = next_batch(&mut input, "Redis knowledge initial snapshot").await?;
    assert_eq!(batch.tick_count(), 3);
    assert_eq!(batch.update_count(), 3);
    assert!(batch.ticks().all(|tick| tick.len() == 1));
    let updates = batch
        .updates()
        .map(|update| (update.variable.clone(), update.value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        updates
            .iter()
            .map(|(variable, _)| variable.clone())
            .collect::<Vec<_>>(),
        vec![
            VarName::new("knowledge.mode"),
            VarName::new("knowledge.plan"),
            VarName::new("knowledge.missing"),
        ]
    );
    let values = updates.into_iter().collect::<BTreeMap<_, _>>();
    assert_eq!(values[&VarName::new("knowledge.missing")], Value::NoVal);
    assert_eq!(
        values[&VarName::new("knowledge.mode")],
        Value::Map(BTreeMap::from([(
            "mode".into(),
            Value::Str("active".into())
        ),]))
    );
    assert_eq!(
        values[&VarName::new("knowledge.plan")],
        Value::List(vec![Value::Str("inspect".into()), Value::Str("repair".into())].into(),)
    );
    assert!(values.get(&VarName::new("knowledge.ignored")).is_none());
    Ok(())
}

#[apply(async_test)]
async fn redis_knowledge_changes_suppress_equivalent_and_deletion_state(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    let prefix = uuid::Uuid::new_v4().to_string();
    let mode_key = key(&prefix, "mode");
    let marker_key = key(&prefix, "marker");
    let _: () = connection.set(&mode_key, "{mode: 'active'}").await?;
    let _: () = connection.set(&marker_key, "0").await?;

    let mode = VarName::new("knowledge.mode");
    let marker = VarName::new("knowledge.marker");
    let config = knowledge_config(
        port,
        true,
        [
            (mode.clone(), mode_key.clone()),
            (marker.clone(), marker_key.clone()),
        ],
    );
    let mut input = InputPipeline::new(InputSource::redis_knowledge(config))
        .with_executor(Rc::clone(&executor))
        .build(BTreeSet::from([mode.clone(), marker.clone()]))
        .await?;
    let _ = next_batch(&mut input, "initial knowledge state").await?;

    let _: () = connection.set(&mode_key, "{mode:'active'}").await?;
    let _: () = connection.set(&marker_key, "1").await?;
    let marker_update = singleton_update(&next_batch(&mut input, "changed marker").await?)?;
    assert_eq!(marker_update.variable, marker);
    assert_eq!(marker_update.value, Value::Int(1));

    let _: () = connection.del(&mode_key).await?;
    let missing = singleton_update(&next_batch(&mut input, "deleted knowledge key").await?)?;
    assert_eq!(missing.variable, mode);
    assert_eq!(missing.value, Value::NoVal);

    let _: () = connection.del(&mode_key).await?;
    let _: () = connection.set(&marker_key, "2").await?;
    let marker_update = singleton_update(&next_batch(&mut input, "marker after deletion").await?)?;
    assert_eq!(marker_update.variable, marker);
    assert_eq!(marker_update.value, Value::Int(2));
    Ok(())
}

#[apply(async_test)]
async fn redis_knowledge_selects_exact_keys_only(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    let prefix = uuid::Uuid::new_v4().to_string();
    let selected_key = key(&prefix, "selected");
    let unselected_key = key(&prefix, "unselected");
    let similarly_named_key = format!("{selected_key}:suffix");
    let selected = VarName::new("knowledge.selected");
    let _: () = connection.set(&selected_key, "0").await?;

    let config = knowledge_config(port, true, [(selected.clone(), selected_key.clone())]);
    let mut input = InputPipeline::new(InputSource::redis_knowledge(config))
        .with_executor(Rc::clone(&executor))
        .build(BTreeSet::from([selected.clone()]))
        .await?;
    let initial = next_batch(&mut input, "exact-key initial snapshot").await?;
    assert_eq!(singleton_update(&initial)?.value, Value::Int(0));

    let _: () = connection.set(&unselected_key, "100").await?;
    let _: () = connection.set(&similarly_named_key, "200").await?;
    let _: () = connection.set(&selected_key, "1").await?;
    let update = singleton_update(&next_batch(&mut input, "selected key update").await?)?;
    assert_eq!(update.variable, selected);
    assert_eq!(update.value, Value::Int(1));
    Ok(())
}

#[apply(async_test)]
async fn redis_knowledge_notification_burst_exposes_latest_state_without_duplicate_final_value(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    let prefix = uuid::Uuid::new_v4().to_string();
    let burst_key = key(&prefix, "burst");
    let marker_key = key(&prefix, "marker");
    let burst = VarName::new("knowledge.burst");
    let marker = VarName::new("knowledge.marker");
    let _: () = connection.set(&burst_key, "0").await?;
    let _: () = connection.set(&marker_key, "0").await?;

    let config = knowledge_config(
        port,
        true,
        [
            (burst.clone(), burst_key.clone()),
            (marker.clone(), marker_key.clone()),
        ],
    );
    let mut input = InputPipeline::new(InputSource::redis_knowledge(config))
        .with_executor(Rc::clone(&executor))
        .build(BTreeSet::from([burst.clone(), marker.clone()]))
        .await?;
    let _ = next_batch(&mut input, "burst initial snapshot").await?;

    for value in 1..=20 {
        let _: () = connection.set(&burst_key, value).await?;
    }

    let mut saw_latest = false;
    for _ in 0..20 {
        let update = singleton_update(&next_batch(&mut input, "burst state").await?)?;
        if update.variable == burst && update.value == Value::Int(20) {
            saw_latest = true;
            break;
        }
    }
    assert!(saw_latest, "burst did not expose its latest state");

    // This controlled marker update is the readiness barrier for the negative
    // assertion: an equivalent final SET must not appear before the marker.
    let _: () = connection.set(&burst_key, 20).await?;
    let _: () = connection.set(&marker_key, 1).await?;
    let update = singleton_update(&next_batch(&mut input, "burst marker barrier").await?)?;
    assert_eq!(update.variable, marker);
    assert_eq!(update.value, Value::Int(1));
    Ok(())
}

#[apply(async_test)]
async fn redis_knowledge_composes_with_pubsub_and_window_stage(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    let prefix = uuid::Uuid::new_v4().to_string();
    let event_channel = key(&prefix, "phase:analyze:completed");
    let state_key = key(&prefix, "state");
    let event = VarName::new("analyze.completed");
    let state = VarName::new("knowledge.state");
    let _: () = connection.set(&state_key, "0").await?;

    let knowledge = InputSource::redis_knowledge(knowledge_config(
        port,
        true,
        [(state.clone(), state_key.clone())],
    ));
    let events = InputSource::redis_with_host_routes(
        REDIS_HOSTNAME,
        Some(BTreeMap::from([(
            event.clone(),
            Route::new(event_channel.clone().into_boxed_str(), None)?,
        )])),
        Some(port),
    );
    let sources = InputSources::new()
        .insert("phase-events", events)
        .insert("knowledge", knowledge);
    let mut input = InputPipeline::from_sources(sources)
        .with_executor(Rc::clone(&executor))
        .build(BTreeSet::from([event.clone(), state.clone()]))
        .await?;
    let _ = next_batch(&mut input, "mixed initial knowledge state").await?;

    let _: usize = connection.publish(&event_channel, "true").await?;
    let _: () = connection.set(&state_key, "1").await?;
    let first = singleton_update(&next_batch(&mut input, "mixed phase event").await?)?;
    let second = singleton_update(&next_batch(&mut input, "mixed knowledge update").await?)?;
    assert_ne!(first.variable, second.variable);
    assert!(
        BTreeSet::from([first.variable, second.variable])
            == BTreeSet::from([event.clone(), state.clone()])
    );

    let staged_knowledge = InputSource::redis_knowledge(knowledge_config(
        port,
        false,
        [(state.clone(), state_key.clone())],
    ));
    let staged_events = InputSource::redis_with_host_routes(
        REDIS_HOSTNAME,
        Some(BTreeMap::from([(
            event.clone(),
            Route::new(event_channel.clone().into_boxed_str(), None)?,
        )])),
        Some(port),
    );
    let policy = InputPolicy::WindowToStep {
        window: InputWindow::new(None, NonZeroUsize::new(2))?,
        reduction: InputReduction::LastUpdateWins,
    };
    let mut atomic = InputPipeline::from_sources(
        InputSources::new()
            .insert("phase-events", staged_events)
            .insert("knowledge", staged_knowledge),
    )
    .with_policy(policy)?
    .with_executor(Rc::clone(&executor))
    .build(BTreeSet::from([event.clone(), state.clone()]))
    .await?;

    let _: usize = connection.publish(&event_channel, "false").await?;
    let _: () = connection.set(&state_key, "2").await?;
    let atomic_batch = next_batch(&mut atomic, "atomic mixed window").await?;
    assert_eq!(atomic_batch.tick_count(), 1);
    assert_eq!(atomic_batch.update_count(), 2);
    let atomic_vars = atomic_batch
        .updates()
        .map(|update| update.variable.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(atomic_vars, BTreeSet::from([event, state]));
    Ok(())
}

#[apply(async_test)]
async fn redis_knowledge_multi_phase_maple_k_inputs_drive_observable_runtime(
    executor: Rc<LocalExecutor<'static>>,
) -> anyhow::Result<()> {
    let container = start_redis().await;
    let port = container.get_host_port_ipv4(6379).await?;
    let mut connection = redis_connection(port).await?;
    enable_keyspace_notifications(&mut connection).await?;

    // These names are representative fixture names. The inspected Adaptive
    // Platform does not publish one universal Redis channel/key registry.
    let prefix = uuid::Uuid::new_v4().to_string();
    let analyze_channel = key(&prefix, "maple:analyse:completed");
    let execute_channel = key(&prefix, "maple:execute:completed");
    let plan_key = key(&prefix, "maple:plan:current");
    let analyze = VarName::new("analyse_completed");
    let execute = VarName::new("execute_completed");
    let plan = VarName::new("current_plan");
    let _: () = connection.set(&plan_key, "inspect").await?;

    let events = InputSource::redis_with_host_routes(
        REDIS_HOSTNAME,
        Some(BTreeMap::from([
            (
                analyze.clone(),
                Route::new(analyze_channel.clone().into_boxed_str(), None)?,
            ),
            (
                execute.clone(),
                Route::new(execute_channel.clone().into_boxed_str(), None)?,
            ),
        ])),
        Some(port),
    );
    let knowledge = InputSource::redis_knowledge(knowledge_config(
        port,
        true,
        [(plan.clone(), plan_key.clone())],
    ));
    let variables = BTreeSet::from([analyze.clone(), execute.clone(), plan.clone()]);
    let policy = InputPolicy::WindowToStep {
        window: InputWindow::new(None, NonZeroUsize::new(3))?,
        reduction: InputReduction::LastUpdateWins,
    };
    let input = InputPipeline::from_sources(
        InputSources::new()
            .insert("maple-k-events", events)
            .insert("knowledge", knowledge),
    )
    .with_policy(policy)?
    .with_executor(Rc::clone(&executor))
    .build(variables)
    .await?;

    let spec = "in analyse_completed\nin execute_completed\nin current_plan\nout observed_plan\nobserved_plan = if analyse_completed && execute_completed then current_plan else \"waiting\""
        .parse::<DsrvSpecification>()?;
    let (output_sender, output_receiver) =
        async_unsync::bounded::channel::<BTreeMap<VarName, Value>>(1024).into_split();
    let mut outputs = Box::pin(futures::stream::unfold(
        output_receiver,
        |mut receiver| async move { receiver.recv().await.map(|row| (row, receiver)) },
    ));
    let builder = GeneralRuntimeBuilder::<DsrvSpecification, Value>::new()
        .executor(executor.clone())
        .model(spec.clone())
        .input(input)
        .output_pipeline(OutputPipeline::from_backend(OutputBackendConfig::channel(
            output_sender,
        )));
    let runtime = builder.build().await?;
    let runtime_task = executor.spawn(Runtime::run(runtime));

    // The initial knowledge update is held by the update-limit window. The
    // first complete Analyse+Execute pair makes startup readiness observable.
    let _: () = connection.set(&plan_key, "inspect").await?;
    let _: usize = connection.publish(&analyze_channel, "true").await?;
    let _: usize = connection.publish(&execute_channel, "true").await?;
    let first = with_timeout(outputs.next(), 5, "MAPLE-K runtime first output")
        .await?
        .ok_or_else(|| anyhow::anyhow!("MAPLE-K runtime output ended"))?;
    assert_eq!(
        first[&VarName::new("observed_plan")],
        Value::Str("inspect".into())
    );

    // A changed Knowledge value is the first update in the next window; the
    // next two phase events close that window and observe the new value.
    let _: () = connection.set(&plan_key, "repair").await?;
    let _: usize = connection.publish(&analyze_channel, "true").await?;
    let _: usize = connection.publish(&execute_channel, "true").await?;
    let second = with_timeout(outputs.next(), 5, "MAPLE-K runtime second output")
        .await?
        .ok_or_else(|| anyhow::anyhow!("MAPLE-K runtime output ended"))?;
    assert_eq!(
        second[&VarName::new("observed_plan")],
        Value::Str("repair".into())
    );

    // The same plan write above is intentionally equivalent to the initial
    // state. If it had produced a knowledge update, the three-update window
    // would have flushed before the phase pair and the first result would be
    // the `waiting` branch rather than the selected plan.
    drop(runtime_task);
    Ok(())
}
