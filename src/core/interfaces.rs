use crate::core::StreamType;
use async_trait::async_trait;
use clap::ValueEnum;
use futures::future::LocalBoxFuture;
#[cfg(feature = "ros")]
use smol::LocalExecutor;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
#[cfg(feature = "ros")]
use std::rc::Rc;
use strum_macros::Display;

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
    ReconfSemiSync,
}

pub type OutputStream<T> = futures::stream::LocalBoxStream<'static, T>;

/// A stream value that can be encoded for MQTT, Redis, and stdout.
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
    fn ros_input_stream(
        executor: Rc<LocalExecutor<'static>>,
        mapping: BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<crate::core::InputStream<Self>>;

    fn ros_output_handler(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        mapping: BTreeMap<String, (String, String)>,
        aux_info: Vec<VarName>,
    ) -> anyhow::Result<Box<dyn OutputHandler<Val = Self>>>;
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

// This could alternatively implement Sink
// The constructor (which is not specified by the trait) should provide any
// configuration details needed by the output handler (e.g. host, port,
// output file name, etc.) whilst provide_streams is called by the runtime to
// finish the setup of the output handler by providing the streams to be output,
// and finally run is called to start the output handler.
pub trait OutputHandler {
    type Val: StreamData;

    // async fn handle_output(&mut self, var: &VarName, value: V);
    // This should only be called once by the runtime to provide the streams
    fn provide_streams(&mut self, streams: BTreeMap<VarName, OutputStream<Self::Val>>);

    // Essentially this is of type
    // async fn run(&mut self);
    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>>;
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
