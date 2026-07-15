pub mod brute_solver;
#[cfg(feature = "sat")]
pub mod sat_solver;
#[cfg(not(feature = "sat"))]
pub mod sat_solver {
    use std::marker::PhantomData;

    use crate::{VarName, semantics::AsyncConfig};

    pub struct SatMonitoredAtDistConstraintSolver<S, AC>
    where
        AC: AsyncConfig,
    {
        pub localised_dist_spec: AC::Spec,
        _phantom: PhantomData<S>,
    }

    impl<S, AC> SatMonitoredAtDistConstraintSolver<S, AC>
    where
        AC: AsyncConfig,
    {
        pub fn new(
            _dist_constraints: Vec<VarName>,
            _output_vars: Vec<VarName>,
            spec: AC::Spec,
            _planning_context: Option<()>,
        ) -> Self {
            Self {
                localised_dist_spec: spec,
                _phantom: PhantomData,
            }
        }
    }
}
