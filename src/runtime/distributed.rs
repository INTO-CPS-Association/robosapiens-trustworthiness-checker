use crate::{
    MonitoringSemantics, Specification, core::StreamData,
    semantics::distributed::combinators::DistributedContext,
};

use super::asynchronous::AsyncMonitorRunner;

/// A Monitor instance implementing the Async Runtime.
///
/// This runtime uses async actors to keep track of dependencies between
/// channels and to distribute data between them, pass data around via async
/// streams, and automatically perform garbage collection of the data contained
/// in the streams.
///
///  - The Expr type parameter is the type of the expressions in the model.
///  - The Val type parameter is the type of the values used in the channels.
///  - The S type parameter is the monitoring semantics used to evaluate the
///    expressions as streams.
///  - The M type parameter is the model/specification being monitored.
pub struct DistributedMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val, DistributedContext<Val>>,
    M: Specification<Expr = Expr>,
{
    async_monitor: AsyncMonitorRunner<Expr, Val, S, M, DistributedContext<Val>>,
}
