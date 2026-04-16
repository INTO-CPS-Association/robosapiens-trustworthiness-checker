use std::rc::Rc;

// #![deny(warnings)]
use anyhow::{self, Context};
use clap::{CommandFactory, FromArgMatches, error::ErrorKind, parser::ValueSource};
use smol::LocalExecutor;
use tracing::{debug, info};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{fmt, prelude::*};
use trustworthiness_checker::cli::adapters::DistributionModeBuilder;
use trustworthiness_checker::core::{AbstractMonitorBuilder, Runnable};
use trustworthiness_checker::io::InputProviderBuilder;
use trustworthiness_checker::io::builders::OutputHandlerBuilder;
use trustworthiness_checker::lang::dsrv::lalr_parser::parse_file as lalr_parse_file;
use trustworthiness_checker::runtime::RuntimeBuilder;
use trustworthiness_checker::runtime::builder::DistributionMode;
use trustworthiness_checker::semantics::distributed::localisation::Localisable;
use trustworthiness_checker::{self as tc, io::file::parse_file};

use macro_rules_attribute::apply;
use smol_macros::main as smol_main;
use trustworthiness_checker::cli::args::{Cli, Language, ParserMode};

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[apply(smol_main)]
async fn main(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
    if cfg!(feature = "span-tracing") {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_span_events(FmtSpan::FULL)
                    .with_file(true)
                    .with_line_number(true),
            )
            .with(EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_file(true)
                    .with_line_number(true),
            )
            .with(EnvFilter::from_default_env())
            .init();
    }

    let mut cmd = Cli::command();
    let matches = cmd.clone().get_matches_from(std::env::args_os());
    let cli = Cli::from_arg_matches(&matches)
        .map_err(|e| anyhow::anyhow!(e.to_string()))
        .context("Failed to parse CLI arguments")?;
    debug!("CLI arguments: {:?}", cli);

    let builder = RuntimeBuilder::new();

    let mqtt_port = cli.mqtt_port;
    let redis_port = cli.redis_port;

    let builder = builder.executor(executor.clone());

    let builder = builder.semantics(cli.semantics);

    let builder = builder.runtime(cli.runtime);

    let parser_was_explicit = matches
        .value_source("parser")
        .is_some_and(|source| source == ValueSource::CommandLine);

    let effective_parser = if matches!(
        cli.runtime,
        trustworthiness_checker::core::Runtime::Distributed
    ) {
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

    let model_parser = match cli.language {
        Language::DSRV => tc::lang::dsrv::parser::dsrv_specification,
    };

    let builder = builder.scheduler_mode(cli.scheduler_communication());

    debug!("Choosing distribution mode");
    let distribution_mode_builder = DistributionModeBuilder::new(cli.distribution_mode)
        .maybe_mqtt_port(mqtt_port)
        .maybe_local_node(cli.local_node)
        .runtime(cli.runtime)
        .maybe_dist_constraints(cli.distribution_constraints)
        .ros_dist_graph_topic(cli.ros_dist_graph_topic.clone());
    debug!("Building distribution mode");
    let distribution_mode = distribution_mode_builder.build().await?;
    debug!(?distribution_mode, "Distribution mode built");
    let builder = builder.distribution_mode(distribution_mode);

    let model = match effective_parser {
        ParserMode::Combinator => parse_file(model_parser, cli.model.as_str())
            .await
            .context("Model file could not be parsed")?,
        ParserMode::Lalr => lalr_parse_file(cli.model.as_str())
            .await
            .context("Model file could not be parsed")?,
    };
    info!(
        "Parsed model: {}",
        serde_json::to_string_pretty(&model).expect("Failed to pretty-print model")
    );

    // Localise the model to contain only the local variables (if needed)
    let model = match &builder.distribution_mode {
        DistributionMode::LocalMonitor(locality_mode) => {
            debug!(?locality_mode, "Localising model");
            let model = model.localise(locality_mode);
            info!(?model, output_vars=?model.output_vars, input_vars=?model.input_vars, "Localised model");
            model
        }
        _ => model,
    };

    let output_var_names = model.output_vars.clone();
    let aux_info = model.aux_info.clone();
    let builder = builder.model(model.clone());

    // Create the input provider builder
    let input_provider_builder = InputProviderBuilder::new(cli.input_mode)
        .executor(executor.clone())
        .model(model)
        .lang(cli.language)
        .runtime(cli.runtime)
        .mqtt_port(mqtt_port)
        .redis_port(redis_port);
    let builder = builder.input_provider_builder(input_provider_builder);

    // Create the output handler
    let output_handler_builder = OutputHandlerBuilder::new(cli.output_mode)
        .executor(executor.clone())
        .output_var_names(output_var_names)
        .mqtt_port(mqtt_port)
        .redis_port(redis_port)
        .aux_info(aux_info);

    let builder = builder.output_handler_builder(output_handler_builder);

    // Create the runtime
    let monitor = builder.async_build().await;

    monitor.run().await
}
