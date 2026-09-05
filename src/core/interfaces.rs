use crate::core::StreamType;
use async_trait::async_trait;
use clap::ValueEnum;

#[cfg(feature = "ros")]
use smol::LocalExecutor;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
#[cfg(feature = "ros")]
use std::rc::Rc;
use strum_macros::Display;

#[cfg(feature = "ros")]
use super::{OutputError, OutputInterface, OutputWriter};
use super::{StreamData, VarName};

/* Enum specifying which semantics is to be used */
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Semantics {
    Untimed,
    TypedUntimed,
    GradualTypedUntimed,
    DelayedQuantitative,
    DelayedQualitative,
    EagerQualitative,
    RobustnessInterval,
}

/// Controls when a runtime may accept the next logical input tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum ExecutionPolicy {
    /// Permit runtime-specific pipelining and output batching.
    #[default]
    Buffered,
    /// Complete the current tick's downstream computation before accepting the
    /// next. This requires a runtime with a global logical-tick boundary.
    Synchronous,
}

/* Runtime and supported execution policy. */
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeSpec {
    #[default]
    Async,
    Dataflow(ExecutionPolicy),
    Mstlo(ExecutionPolicy),
    Distributed,
    SemiSync,
    /// Independent reference runtime with semisynchronous reconfiguration.
    ReconfSemiSync,
    /// Reconfigurable dataflow runtime with the selected input policy.
    ReconfDataflow(ExecutionPolicy),
}

pub type LocalStream<T> = futures::stream::LocalBoxStream<'static, T>;

/// A stream value that can be decoded from JSON5 and encoded for MQTT, Redis, and stdout.
pub trait JsonStreamValue: StreamData + Sized {
    fn decode_json(payload: &[u8]) -> anyhow::Result<Self>;
    fn encode_json(&self) -> anyhow::Result<String>;

    fn decode_mqtt_payload(payload: &[u8]) -> anyhow::Result<Self>;

    fn encode_stdout(&self) -> anyhow::Result<String> {
        self.encode_json()
    }
}

/// A stream value accepted by file input.
pub trait FileInputValue: JsonStreamValue {
    fn decode_file_value(payload: &str) -> anyhow::Result<Self> {
        Self::decode_json(payload.as_bytes())
    }

    fn missing_value() -> Self;
}

#[cfg(feature = "ros")]
/// A stream value accepted by ROS input and output handlers.
pub trait RosStreamValue: StreamData + Sized {
    fn open_ros_input(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<(
        crate::core::InputStream<Self>,
        crate::io::ros::RosInputControl,
    )>;

    fn open_ros_output(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        interface: OutputInterface,
    ) -> futures::future::LocalBoxFuture<'static, Result<OutputWriter<Self>, OutputError>>;
}

#[cfg(not(feature = "ros"))]
pub trait RosStreamValue: StreamData {}

#[cfg(not(feature = "ros"))]
impl<T: StreamData> RosStreamValue for T {}

pub trait Specification: Debug + std::fmt::Display + Clone + 'static {
    type Expr;

    fn input_vars(&self) -> BTreeSet<VarName>;

    fn output_vars(&self) -> BTreeSet<VarName>;

    fn aux_vars(&self) -> BTreeSet<VarName>;

    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.output_vars()
            .into_iter()
            .chain(self.aux_vars())
            .collect()
    }

    fn var_names(&self) -> BTreeSet<VarName> {
        self.input_vars()
            .into_iter()
            .chain(self.stream_vars())
            .collect()
    }

    fn var_expr(&self, var: &VarName) -> Option<Self::Expr>;

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType>;
}

/*
 * A runtime, implementing a runtime monitor for a model/specification.
 */
#[async_trait(?Send)]
pub trait Runtime {
    // Should usually wait on the output provider
    async fn run(mut self) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        Box::new(self).run_boxed().await
    }

    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()>;
}

/* Allow using a boxed runtime as a runtime */
#[async_trait(?Send)]
impl Runtime for Box<dyn Runtime> {
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        Runtime::run_boxed(self).await
    }

    async fn run(mut self: Self) -> anyhow::Result<()> {
        Runtime::run_boxed(self).await
    }
}
