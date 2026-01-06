use std::rc::Rc;

use async_trait::async_trait;
use ecow::EcoVec;
use smol::LocalExecutor;

use crate::{OutputStream, VarName, core::StreamData};

/// Abstract builder of contexts
pub trait AbstractContextBuilder {
    type Val: StreamData;
    type Ctx: StreamContext<Val = Self::Val>;

    fn new() -> Self;

    fn executor(self, executor: Rc<LocalExecutor<'static>>) -> Self;

    fn var_names(self, var_names: Vec<VarName>) -> Self;

    fn history_length(self, history_length: usize) -> Self;

    fn input_streams(self, streams: Vec<OutputStream<Self::Val>>) -> Self;

    fn partial_clone(&self) -> Self;

    fn build(self) -> Self::Ctx;
}

#[async_trait(?Send)]
pub trait StreamContext: 'static {
    type Val: StreamData;
    type Builder: AbstractContextBuilder<Val = Self::Val, Ctx = Self>;

    fn var(&self, x: &VarName) -> Option<OutputStream<Self::Val>>;

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

pub trait MonitoringSemantics<Expr, AC, Ctx>: Clone + 'static
where
    AC: AsyncConfig,
    Ctx: StreamContext,
{
    fn to_async_stream(expr: Expr, ctx: &Ctx) -> OutputStream<AC::Val>;
}

pub trait AsyncConfig: 'static {
    type Val: StreamData;
    type CtxVal: StreamData;
    type Expr: 'static;
    // type Sem: MonitoringSemantics<Self::Expr, Self::Val, Self::Ctx, Self::CtxVal>;
    // type Ctx: StreamContext<Self::CtxVal>;
    // type CtxBuilder: AbstractContextBuilder<Val = Self::Val, Ctx = Self::Ctx>;
}
