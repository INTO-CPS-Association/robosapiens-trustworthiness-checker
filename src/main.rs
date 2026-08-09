use std::collections::BTreeSet;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

// #![deny(warnings)]
use anyhow::{self, Context};
use clap::{CommandFactory, FromArgMatches, error::ErrorKind, parser::ValueSource};
use mstlo::Variables;
use smol::LocalExecutor;
use tracing::{debug, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::cli::adapters::{
    DistributionModeBuilder, input_source, output_handler_spec, route_mappings,
};
use trustworthiness_checker::core::{Runtime, RuntimeSpec};
use trustworthiness_checker::distributed::scheduling::dist_constraint_evaluator::dist_constraint_input_vars;
use trustworthiness_checker::io::{
    InputConfigFile, InputPipeline, InputReduction, InputSources, InputStage, InputWindow,
    OutputHandlerBuilder,
};
use trustworthiness_checker::lang::dsrv::parser::parse_file as lalr_parse_file;
use trustworthiness_checker::lang::mstlo::MstloSpecification;
use trustworthiness_checker::runtime::builder::{DistributionMode, LangSpecification};
use trustworthiness_checker::runtime::mstlo::MstloTimedValue;
use trustworthiness_checker::runtime::{GeneralRuntimeBuilder, RuntimeBuilder};
use trustworthiness_checker::semantics::distributed::localisation::Localisable;
use trustworthiness_checker::{self as tc, Specification};
use trustworthiness_checker::{Value, VarName};

use macro_rules_attribute::apply;
use smol_macros::main as smol_main;
use trustworthiness_checker::cli::args::{
    Cli, InputWindowMode, Language, OutputMode, resolve_runtime,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[apply(smol_main)]
async fn main(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    let mut cmd = Cli::command();
    let matches = cmd.clone().get_matches_from(std::env::args_os());
    let cli = Cli::from_arg_matches(&matches)
        .map_err(|e| anyhow::anyhow!(e.to_string()))
        .context("Failed to parse CLI arguments")?;
    cli.validate().context("Invalid CLI combination")?;

    let _log_guard = init_tracing(cli.log_file.as_deref())?;
    debug!("CLI arguments: {:?}", cli);

    let runtime_was_explicit = matches
        .value_source("runtime")
        .is_some_and(|source| source == ValueSource::CommandLine);
    let runtime = resolve_runtime(
        cli.language,
        cli.runtime,
        cli.execution_policy,
        runtime_was_explicit,
    )
    .unwrap_or_else(|error| {
        cmd.error(ErrorKind::ArgumentConflict, error.to_string())
            .exit()
    });

    if matches!(cli.language, Language::MSTLO) {
        return run_mstlo(executor, cli, runtime).await;
    }

    let builder = <GeneralRuntimeBuilder<LangSpecification, Value> as RuntimeBuilder<
        LangSpecification,
        Value,
    >>::new();

    let mqtt_port = cli.mqtt_port;
    let redis_port = cli.redis_port;

    let builder = builder.executor(executor.clone());

    let builder = builder.semantics(cli.semantics);

    let builder = builder.runtime(runtime);

    let builder = if let Some(topic) = cli.reconf_topic.clone() {
        builder.reconf_topic(topic)
    } else {
        builder
    };

    let builder = builder.use_context_transfer(!cli.no_context_transfer);

    let builder = builder.scheduler_mode(cli.scheduler_communication());

    debug!("Choosing distribution mode");
    let dist_constraints = cli.distribution_constraints.clone();
    let distribution_mode_builder = DistributionModeBuilder::new(cli.distribution_mode.clone())
        .maybe_mqtt_port(mqtt_port)
        .maybe_local_node(cli.local_node.clone())
        .runtime(runtime)
        .maybe_dist_constraints(dist_constraints.clone())
        .dist_constraint_solver(cli.dist_constraint_solver)
        .ros_dist_graph_topic(cli.ros_dist_graph_topic.clone());
    debug!("Building distribution mode");
    let distribution_mode = distribution_mode_builder.build().await?;
    debug!(?distribution_mode, "Distribution mode built");
    let builder = builder.distribution_mode(distribution_mode);

    let model = lalr_parse_file(cli.model.as_str())
        .await
        .map(LangSpecification::from)
        .context("Model file could not be parsed")?;
    info!(%model, "Parsed model");

    // Localise the model to contain only the local variables (if needed)
    let model = match (&builder.distribution_mode, model) {
        (DistributionMode::LocalMonitor(locality_mode), LangSpecification::Dsrv(model)) => {
            debug!(?locality_mode, "Localising model");
            let model = model.localise(locality_mode);
            info!(?model, output_vars=?model.output_vars(), input_vars=?model.input_vars(), "Localised model");
            LangSpecification::Dsrv(model)
        }
        (_, model) => model,
    };

    // Filtered output variable names excluding distribution constraints
    let output_var_names: BTreeSet<_> = model
        .output_vars()
        .into_iter()
        .filter(|var_name| {
            !dist_constraints
                .clone()
                .map_or(false, |c| c.contains(&var_name.into()))
        })
        .collect();
    let aux_info = model.aux_vars().into_iter().collect();
    let builder = builder.model(model.clone());

    // Restrict distributed input subscriptions to constraint variables and their
    // true input dependencies without changing the language specification.
    let subscribed_input_vars = if matches!(runtime, RuntimeSpec::Distributed) {
        match (&dist_constraints, &model) {
            (Some(constraints), LangSpecification::Dsrv(model)) if !constraints.is_empty() => {
                let localized_constraint_vars: Vec<VarName> =
                    constraints.iter().cloned().map(VarName::from).collect();
                let localized = model.localise(&localized_constraint_vars);
                let mut input_vars = localized.input_vars().clone();
                input_vars.extend(dist_constraint_input_vars(
                    model,
                    &localized_constraint_vars,
                ));
                input_vars
            }
            _ => model.input_vars(),
        }
    } else {
        model.input_vars()
    };

    info!(
        input_vars = ?subscribed_input_vars,
        "Input variables selected for subscription"
    );

    // Configure the reusable input pipeline. Resource acquisition happens in
    // `build`, after the model's requested variables are known.
    let input_pipeline = configure_input_pipeline::<Value>(
        cli.input_mode.clone(),
        executor.clone(),
        mqtt_port,
        redis_port,
        cli.mqtt_input_backend(),
        &cli,
    )?;
    let builder = if matches!(runtime, RuntimeSpec::ReconfSemiSync) {
        builder.input_pipeline(input_pipeline)?
    } else {
        builder.input(
            input_pipeline
                .build(subscribed_input_vars)
                .await
                .context("Input stream could not be built")?,
        )
    };

    // Create the output handler
    let output_handler_builder = OutputHandlerBuilder::new(cli.output_mode.clone())
        .executor(executor.clone())
        .output_var_names(output_var_names)
        .mqtt_port(mqtt_port)
        .redis_port(redis_port)
        .aux_info(aux_info);

    // Keep the compact ROS route catalog available to the output/runtime
    // builder without reparsing the legacy nested format.
    let (var_msg_types, topic_mapping) = match &cli.output_mode {
        OutputMode {
            output_ros_file: Some(path),
            ..
        } => {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("Output route catalog {path:?} could not be read"))?;
            let routes =
                trustworthiness_checker::io::config::deserialisation::json_to_routes(&contents)?;
            let (topics, codecs) = route_mappings(routes, true)?;
            (Some(codecs), Some(topics))
        }
        _ => (None, None),
    };
    let builder = builder.maybe_var_msg_types(var_msg_types);
    let builder = builder.maybe_topic_mapping(topic_mapping);

    let builder = builder.output_handler_builder(output_handler_builder);

    // Create the runtime
    let monitor = builder.build().await;

    monitor.run().await
}

async fn run_mstlo(
    executor: Rc<LocalExecutor<'static>>,
    cli: Cli,
    runtime: RuntimeSpec,
) -> anyhow::Result<()> {
    let RuntimeSpec::Mstlo(execution_policy) = runtime else {
        anyhow::bail!("MSTLO CLI configuration requires RuntimeSpec::Mstlo")
    };

    let model: MstloSpecification = tc::lang::mstlo::parse_file(cli.model.as_str())
        .await
        .context("MSTLO model file could not be parsed")?;
    info!(%model, "Parsed MSTLO model");

    let input_pipeline = configure_input_pipeline::<MstloTimedValue>(
        cli.input_mode.clone(),
        executor.clone(),
        cli.mqtt_port,
        cli.redis_port,
        cli.mqtt_input_backend(),
        &cli,
    )?;

    let input = input_pipeline
        .build(model.input_vars())
        .await
        .context("MSTLO input stream could not be built")?;
    let output_vars = model.output_vars();
    let aux_info = model.aux_vars().into_iter().collect::<Vec<_>>();
    let output_spec = output_handler_spec::<MstloTimedValue>(cli.output_mode.clone())?;
    let output_handler = OutputHandlerBuilder::<MstloTimedValue>::new(output_spec)
        .executor(executor.clone())
        .output_var_names(output_vars)
        .mqtt_port(cli.mqtt_port)
        .redis_port(cli.redis_port)
        .aux_info(aux_info)
        .build()
        .await
        .context("MSTLO output handler could not be built")?;

    let builder = <GeneralRuntimeBuilder<MstloSpecification, MstloTimedValue> as RuntimeBuilder<
        MstloSpecification,
        MstloTimedValue,
    >>::new()
    .executor(executor)
    .model(model)
    .input(input)
    .output(output_handler)
    .runtime(RuntimeSpec::Mstlo(execution_policy))
    .semantics(cli.semantics)
    .mstlo_algorithm(cli.mstlo_algorithm)
    .mstlo_synchronization_strategy(cli.mstlo_synchronization)
    .mstlo_variables(parse_mstlo_variables(cli.mstlo_vars.as_deref())?);

    let monitor = builder.build().await;
    monitor.run().await
}

fn configure_input_pipeline<V>(
    input_mode: trustworthiness_checker::cli::args::InputMode,
    executor: Rc<LocalExecutor<'static>>,
    mqtt_port: Option<u16>,
    redis_port: Option<u16>,
    mqtt_backend: trustworthiness_checker::io::mqtt::MqttInputBackend,
    cli: &Cli,
) -> anyhow::Result<InputPipeline<V>>
where
    V: trustworthiness_checker::core::FileInputValue
        + trustworthiness_checker::core::RosStreamValue
        + 'static,
{
    let mut pipeline = if let Some(path) = &input_mode.input_config {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("input config {path:?} could not be read"))?;
        let config: InputConfigFile =
            serde_json5::from_str(&contents).context("input config could not be parsed")?;
        let sources =
            InputSources::<V>::from_config(config, executor, mqtt_port, redis_port, mqtt_backend)?;
        InputPipeline::from_sources(sources)
    } else {
        InputPipeline::new(input_source(
            input_mode,
            executor,
            mqtt_port,
            redis_port,
            mqtt_backend,
        )?)
    };

    if let Some(window_ms) = cli.input_window_ms {
        let window = InputWindow::new(
            Some(Duration::from_millis(window_ms)),
            cli.input_window_update_limit,
        )?;
        pipeline = pipeline.with_stage(
            match cli.input_window_mode.unwrap_or(InputWindowMode::Batch) {
                InputWindowMode::Batch => InputStage::Batch(window),
                InputWindowMode::AtomicStep => InputStage::WindowToStep {
                    window,
                    reduction: InputReduction::LastUpdateWins,
                },
            },
        )?;
    } else if cli.input_window_update_limit.is_some() {
        let window = InputWindow::new(None, cli.input_window_update_limit)?;
        pipeline = pipeline.with_stage(
            match cli.input_window_mode.unwrap_or(InputWindowMode::Batch) {
                InputWindowMode::Batch => InputStage::Batch(window),
                InputWindowMode::AtomicStep => InputStage::WindowToStep {
                    window,
                    reduction: InputReduction::LastUpdateWins,
                },
            },
        )?;
    }
    Ok(pipeline)
}

fn parse_mstlo_variables(bindings: Option<&[String]>) -> anyhow::Result<Variables> {
    let variables = Variables::new();
    for binding in bindings.unwrap_or(&[]) {
        let (name, value) = binding.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("MSTLO variable binding `{binding}` must have format name=value")
        })?;
        anyhow::ensure!(
            !name.trim().is_empty(),
            "MSTLO variable name cannot be empty"
        );
        let value = value.trim().parse::<f64>().with_context(|| {
            format!(
                "MSTLO variable `{}` value `{}` is not a valid float",
                name.trim(),
                value.trim()
            )
        })?;
        let name = Box::leak(name.trim().to_string().into_boxed_str()) as &'static str;
        variables.set(name, value);
    }
    Ok(variables)
}

fn init_tracing(log_file: Option<&str>) -> anyhow::Result<WorkerGuard> {
    let (writer, guard) = match log_file {
        Some(path) => {
            let path = Path::new(path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    anyhow::ensure!(
                        parent.exists(),
                        "Log directory does not exist: {}",
                        parent.display()
                    );
                    anyhow::ensure!(
                        parent.is_dir(),
                        "Log path parent is not a directory: {}",
                        parent.display()
                    );
                }
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            tracing_appender::non_blocking(file)
        }
        None => tracing_appender::non_blocking(std::io::stderr()),
    };

    let fmt_layer = if cfg!(feature = "span-tracing") {
        fmt::layer()
            .with_writer(writer)
            .with_span_events(FmtSpan::FULL)
            .with_file(true)
            .with_line_number(true)
    } else {
        fmt::layer()
            .with_writer(writer)
            .with_file(true)
            .with_line_number(true)
    };

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(EnvFilter::from_default_env())
        .init();

    Ok(guard)
}
