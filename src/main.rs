use std::collections::BTreeMap;
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
use trustworthiness_checker::cli::adapters::{DistributionModeBuilder, input_factory};
use trustworthiness_checker::core::{Runtime, RuntimeSpec};
use trustworthiness_checker::distributed::scheduling::dist_constraint_evaluator::dist_constraint_input_vars;
use trustworthiness_checker::io::{AggregationSemantics, InputAggregation, OutputHandlerBuilder};
use trustworthiness_checker::lang::dsrv::lalr_parser::parse_file as lalr_parse_file;
use trustworthiness_checker::runtime::builder::{DistributionMode, LangSpecification};
use trustworthiness_checker::runtime::{GeneralRuntimeBuilder, RuntimeBuilder};
use trustworthiness_checker::semantics::distributed::localisation::Localisable;
use trustworthiness_checker::{self as tc, Specification, io::file::parse_file};
use trustworthiness_checker::{Value, VarName};

use macro_rules_attribute::apply;
use smol_macros::main as smol_main;
use trustworthiness_checker::cli::args::{
    Cli, InputAggregationMode, Language, OutputMode, ParserMode, resolve_runtime,
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

    let builder = <GeneralRuntimeBuilder<LangSpecification, Value> as RuntimeBuilder<
        LangSpecification,
        Value,
    >>::new();

    let mqtt_port = cli.mqtt_port;
    let redis_port = cli.redis_port;

    let builder = builder.executor(executor.clone());

    let builder = builder.semantics(cli.semantics);

    let builder = builder.runtime(runtime);

    let parser_was_explicit = matches
        .value_source("parser")
        .is_some_and(|source| source == ValueSource::CommandLine);

    let effective_parser =
        if matches!(cli.language, Language::DSRV) && matches!(runtime, RuntimeSpec::Distributed) {
            if parser_was_explicit && !matches!(cli.parser, ParserMode::Combinator) {
                cmd.error(
                    ErrorKind::ArgumentConflict,
                    "--parser combinator is required when --runtime distributed is used",
                )
                .exit();
            }
            ParserMode::Combinator
        } else {
            cli.parser
        };

    let builder = builder.parser(effective_parser);

    let builder = builder.reconf_topic(cli.reconf_topic.clone());

    let builder = builder.use_context_transfer(!cli.no_context_transfer);

    let builder = builder
        .mstlo_algorithm(cli.mstlo_algorithm)
        .mstlo_synchronization_strategy(cli.mstlo_synchronization)
        .mstlo_variables(parse_mstlo_variables(cli.mstlo_vars.as_deref())?);

    let builder = builder.scheduler_mode(cli.scheduler_communication());

    debug!("Choosing distribution mode");
    let dist_constraints = cli.distribution_constraints.clone();
    let distribution_mode = if matches!(cli.language, Language::DSRV) {
        let distribution_mode_builder = DistributionModeBuilder::new(cli.distribution_mode.clone())
            .maybe_mqtt_port(mqtt_port)
            .maybe_local_node(cli.local_node.clone())
            .runtime(runtime)
            .maybe_dist_constraints(dist_constraints.clone())
            .dist_constraint_solver(cli.dist_constraint_solver)
            .ros_dist_graph_topic(cli.ros_dist_graph_topic.clone());
        debug!("Building distribution mode");
        distribution_mode_builder.build().await?
    } else {
        DistributionMode::CentralMonitor
    };
    debug!(?distribution_mode, "Distribution mode built");
    let builder = builder.distribution_mode(distribution_mode);

    let model: LangSpecification = match cli.language {
        Language::DSRV => match effective_parser {
            ParserMode::Combinator => parse_file(
                tc::lang::dsrv::parser::dsrv_specification,
                cli.model.as_str(),
            )
            .await
            .map(LangSpecification::from)
            .context("Model file could not be parsed")?,
            ParserMode::Lalr => lalr_parse_file(cli.model.as_str())
                .await
                .map(LangSpecification::from)
                .context("Model file could not be parsed")?,
        },
        Language::MSTLO => tc::lang::mstlo::parse_file(cli.model.as_str())
            .await
            .map(LangSpecification::from)
            .context("MSTLO model file could not be parsed")?,
    };
    info!(%model, "Parsed model");

    // Localise the model to contain only the local variables (if needed)
    let model = match (&builder.distribution_mode, model) {
        (DistributionMode::LocalMonitor(locality_mode), LangSpecification::Dsrv(model)) => {
            debug!(?locality_mode, "Localising model");
            let model = model.localise(locality_mode);
            info!(?model, output_vars=?model.output_vars, input_vars=?model.input_vars, "Localised model");
            LangSpecification::Dsrv(model)
        }
        (_, model) => model,
    };

    // Filtered output variable names excluding distribution constraints
    let output_var_names = model
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

    // For distributed runtime with distribution constraints, create a localised model
    // to restrict input subscriptions the constraint variables only (and their true input
    // dependencies).
    let localized_model = if matches!(runtime, RuntimeSpec::Distributed) {
        match (&dist_constraints, &model) {
            (Some(constraints), LangSpecification::Dsrv(model)) if !constraints.is_empty() => {
                let localized_constraint_vars: Vec<VarName> =
                    constraints.iter().cloned().map(VarName::from).collect();
                let mut localized = model.localise(&localized_constraint_vars);
                for var in dist_constraint_input_vars(model, &localized_constraint_vars) {
                    localized.add_input_var(var);
                }
                LangSpecification::Dsrv(localized)
            }
            _ => model.clone(),
        }
    } else {
        model.clone()
    };

    info!(
        input_vars = ?localized_model.input_vars(),
        "Localized model selected for input stream"
    );

    // Configure the input stream factory.
    let mut input_factory = input_factory(
        cli.input_mode.clone(),
        executor.clone(),
        mqtt_port,
        redis_port,
    )?;
    if let Some(delay_ms) = cli.input_aggregation_delay_ms {
        let semantics = match cli
            .input_aggregation_mode
            .unwrap_or(InputAggregationMode::PreserveTicks)
        {
            InputAggregationMode::PreserveTicks => AggregationSemantics::PreserveTicks,
            InputAggregationMode::AtomicStep => AggregationSemantics::CoalesceToAtomicStep,
        };
        let mut aggregation = InputAggregation::new(Duration::from_millis(delay_ms), semantics);
        if let Some(event_limit) = cli.input_aggregation_event_limit {
            aggregation = aggregation.with_event_limit(event_limit);
        }
        input_factory = input_factory.input_aggregation(aggregation)?;
    }
    let builder = if matches!(runtime, RuntimeSpec::ReconfSemiSync) {
        builder.input_factory(input_factory)?
    } else {
        builder.input(
            input_factory
                .open(localized_model.input_vars())
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

    // Get variable message types mapping and ROS topic mapping
    let (var_msg_types, topic_mapping): (
        Option<BTreeMap<VarName, String>>,
        Option<BTreeMap<VarName, String>>,
    ) = match &cli.output_mode {
        OutputMode {
            output_ros_file: Some(_output_ros_file),
            ..
        } => {
            // TODO: use cfg-if feature in next Rust version instead of this
            // more verbose syntax
            #[cfg(feature = "ros")]
            {
                // TODO: refactor to avoid reading the file twice (not done
                // currently as this would couple the output handler building
                // and the runtime building)

                use trustworthiness_checker::io::config::deserialisation::json_to_topic_msg_type_mapping;

                let output_json = std::fs::read_to_string(_output_ros_file)
                    .expect("Output mapping file could not be read");
                let (output_topics, output_types) = json_to_topic_msg_type_mapping(&output_json)
                    .expect("Output mapping file could not be parsed");

                let (input_types, input_topics) = match &cli.input_mode.input_ros_file {
                    Some(input_ros_file) => {
                        let input_json = std::fs::read_to_string(input_ros_file)
                            .expect("Input mapping file could not be read");
                        let (input_topics, input_types) =
                            json_to_topic_msg_type_mapping(&input_json)
                                .expect("Input mapping file could not be parsed");
                        (input_types, input_topics)
                    }
                    None => (BTreeMap::new(), BTreeMap::new()),
                };

                let merged_types = input_types.into_iter().chain(output_types).collect();
                let merged_topics = input_topics.into_iter().chain(output_topics).collect();
                (Some(merged_types), Some(merged_topics))
            }
            #[cfg(not(feature = "ros"))]
            {
                unimplemented!("Attempted to set a ROS topic mapping when ROS support not enabled")
            }
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
