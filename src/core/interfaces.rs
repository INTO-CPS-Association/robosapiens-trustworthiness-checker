use crate::core::StreamType;
use crate::io::replay_history::ReplayHistory;
use async_trait::async_trait;
use clap::ValueEnum;
use futures::future::LocalBoxFuture;
use std::collections::BTreeSet;
use std::{collections::BTreeMap, fmt::Debug};
use strum_macros::Display;

use super::{StreamData, Value, VarName};

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

/* Enum specifying which runtime is to be used */
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
#[strum(serialize_all = "kebab-case")]
pub enum RuntimeSpec {
    Async,
    Distributed,
    SemiSync,
    ReconfSemiSync,
}

pub type OutputStream<T> = futures::stream::LocalBoxStream<'static, T>;

#[async_trait(?Send)]
pub trait InputProvider {
    type Val;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>>;

    /// Returns an OutputStream of Result<()> indicating if the InputProvider has encountered an
    /// error (Err) or has successfully provided one batch of values (Ok).
    /// Awaiting the control_stream attempts to progress the InputProvider by one step.
    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>>;

    /// Optional replay snapshot accessor.
    ///
    /// Input providers that can expose deterministic historical values for replay
    /// (e.g. file-backed providers) may override this to return a full
    /// time-indexed snapshot map.
    ///
    /// The default implementation returns `None`, indicating replay history is
    /// not available.
    fn replay_history(&self) -> Option<BTreeMap<usize, BTreeMap<VarName, Value>>> {
        None
    }

    /// Optional live replay-history handle accessor.
    fn replay_history_handle(&self) -> Option<ReplayHistory> {
        None
    }
}

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

    fn add_input_var(&mut self, var: VarName);

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
