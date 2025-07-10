use std::rc::Rc;

use async_trait::async_trait;
use ecow::EcoVec;
use smol::LocalExecutor;

use crate::{OutputStream, VarName, core::StreamData};

/// Abstract builder of contexts
pub trait AbstractContextBuilder {
    type Val: StreamData;
    type Ctx: StreamContext<Self::Val>;

    fn new() -> Self;

    fn executor(self, executor: Rc<LocalExecutor<'static>>) -> Self;

    fn var_names(self, var_names: Vec<VarName>) -> Self;

    fn history_length(self, history_length: usize) -> Self;

    fn input_streams(self, streams: Vec<OutputStream<Self::Val>>) -> Self;

    fn partial_clone(&self) -> Self;

    fn build(self) -> Self::Ctx;
}

#[async_trait(?Send)]
pub trait StreamContext<Val: StreamData>: 'static {
    type Builder: AbstractContextBuilder<Val = Val, Ctx = Self>;

    fn var(&self, x: &VarName) -> Option<OutputStream<Val>>;

    fn subcontext(&self, history_length: usize) -> Self;

    fn restricted_subcontext(&self, vs: EcoVec<VarName>, history_length: usize) -> Self;

    /// Advance the clock used by the context by one step, letting all
    /// streams to progress (blocking)
    async fn tick(&mut self);

    /// Set the clock to automatically advance, allowing all substreams
    /// to progress freely (limited only by buffering)
    async fn run(&mut self);

    /// Check if the clock is currently started
    fn is_clock_started(&self) -> bool;

    /// Get the current value of the clock (this may not guarantee
    /// that all stream have reached this time)
    fn clock(&self) -> usize;

    /// Get the cancellation token for this context
    fn cancellation_token(&self) -> crate::utils::cancellation_token::CancellationToken;

    /// Cancel all var managers in this context
    fn cancel(&self);
}

pub trait MonitoringSemantics<Expr, Val, Ctx, CVal = Val>: Clone + 'static
where
    Val: StreamData,
    CVal: StreamData,
    Ctx: StreamContext<CVal>,
{
    fn to_async_stream(expr: Expr, ctx: &Ctx) -> OutputStream<Val>;
}
