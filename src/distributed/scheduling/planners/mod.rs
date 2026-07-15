pub mod constrained;
#[cfg(feature = "sat")]
pub mod constrained_sat;
#[cfg(not(feature = "sat"))]
pub mod constrained_sat {
    use std::{marker::PhantomData, rc::Rc};

    use async_trait::async_trait;

    use crate::distributed::{
        distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
        scheduling::{
            planners::core::SchedulerPlanner,
            planning_context::PlanningContext,
        },
        solvers::sat_solver::SatMonitoredAtDistConstraintSolver,
    };
    use crate::semantics::AsyncConfig;

    pub struct StaticOptimizedSchedulerPlannerSat<S, AC>
    where
        AC: AsyncConfig,
    {
        _phantom: PhantomData<(S, AC)>,
    }

    impl<S, AC> StaticOptimizedSchedulerPlannerSat<S, AC>
    where
        AC: AsyncConfig,
    {
        pub fn new(_solver: SatMonitoredAtDistConstraintSolver<S, AC>) -> Self {
            panic!("SAT solver support not enabled")
        }
    }

    #[async_trait(?Send)]
    impl<S, AC> SchedulerPlanner for StaticOptimizedSchedulerPlannerSat<S, AC>
    where
        S: 'static,
        AC: AsyncConfig + 'static,
    {
        async fn plan(
            &self,
            _graph: Rc<DistributionGraph>,
            _scheduler_tick: usize,
            _planning_context: Option<PlanningContext>,
        ) -> Option<Rc<LabelledDistributionGraph>> {
            unreachable!("SAT solver support not enabled")
        }
    }

    pub struct DynamicOptimizedSchedulerPlannerSat<S, AC>
    where
        AC: AsyncConfig,
    {
        _phantom: PhantomData<(S, AC)>,
    }

    impl<S, AC> DynamicOptimizedSchedulerPlannerSat<S, AC>
    where
        AC: AsyncConfig,
    {
        pub fn new(_solver: SatMonitoredAtDistConstraintSolver<S, AC>) -> Self {
            panic!("SAT solver support not enabled")
        }
    }

    #[async_trait(?Send)]
    impl<S, AC> SchedulerPlanner for DynamicOptimizedSchedulerPlannerSat<S, AC>
    where
        S: 'static,
        AC: AsyncConfig + 'static,
    {
        async fn plan(
            &self,
            _graph: Rc<DistributionGraph>,
            _scheduler_tick: usize,
            _planning_context: Option<PlanningContext>,
        ) -> Option<Rc<LabelledDistributionGraph>> {
            unreachable!("SAT solver support not enabled")
        }
    }
}
pub mod core;
pub mod random;
