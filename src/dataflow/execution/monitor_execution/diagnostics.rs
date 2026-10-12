//! Diagnostics hooks of the execution owner.
//!
//! The step loop and tier transitions call the methods here. They read the active plan and arena
//! but never change routes, authority or state. Without the `diagnostics` feature every method is
//! empty.

// Diagnostics only read state; none of this code may need `unsafe`.
#![forbid(unsafe_code)]

use super::MonitorExecution;
use crate::dataflow::diagnostics::ExecutionProbe;
#[cfg(not(feature = "diagnostics"))]
use crate::dataflow::diagnostics::Stamp;

#[cfg(feature = "diagnostics")]
mod enabled {
    use serde_json::json;

    use super::super::plan::{ExecutionPlan, ExecutionStep};
    use super::super::{AuthoritativeTier, MonitorExecution, RegionAuthority};
    use crate::dataflow::diagnostics::{
        DataflowRecorder, MemoryVisitor, PlanDescriptor, Stamp, StepDescriptor,
    };
    use crate::dataflow::environment::EnvironmentSlot;
    use crate::dataflow::execution::scalar_region::ScalarRegion;
    use crate::diagnostics::MemoryEstimate;

    fn describe(plan: &ExecutionPlan, quickening: bool) -> PlanDescriptor {
        let step = |step: &ExecutionStep| match step {
            ExecutionStep::Graph(graph) => StepDescriptor::Graph {
                stream: graph.stream.index(),
                islands: graph
                    .segments
                    .iter()
                    .filter(|segment| {
                        matches!(
                            segment,
                            super::super::plan::ExecutableSegment::Island { .. }
                        )
                    })
                    .count(),
            },
            ExecutionStep::ScalarRegion(region) => StepDescriptor::Region {
                region: *region,
                members: match &plan.regions[*region] {
                    ScalarRegion::Streams(streams) => {
                        streams.iter().map(|member| member.stream.index()).collect()
                    }
                    ScalarRegion::Graph(graph) => vec![graph.stream.index()],
                },
            },
        };
        PlanDescriptor {
            quickening,
            source: plan.source_steps.iter().map(step).collect(),
            main: plan.main_steps.iter().map(step).collect(),
            streams: plan
                .semantic
                .streams
                .iter()
                .map(|planned| planned.stream.index())
                .collect(),
        }
    }

    impl MonitorExecution {
        pub(in crate::dataflow) fn recorder(&self) -> Option<&DataflowRecorder> {
            self.diagnostics.recorder()
        }

        pub(in crate::dataflow) fn recorder_mut(&mut self) -> Option<&mut DataflowRecorder> {
            self.diagnostics.recorder_mut()
        }

        pub(in crate::dataflow) fn install_recorder(&mut self, recorder: Box<DataflowRecorder>) {
            self.diagnostics.install(recorder);
        }

        /// Point step records at the active plan, describing it on first sight.
        #[inline]
        pub(in crate::dataflow) fn observe_active_plan(&mut self) {
            let Some(recorder) = self.diagnostics.recorder_mut() else {
                return;
            };
            let plan = &self.engine.active_plan;
            let quickening = self.engine.quickening;
            recorder.observe_plan(plan.identity.id.0, plan.identity.generation, || {
                describe(plan, quickening)
            });
        }

        pub(in crate::dataflow::execution::monitor_execution) fn diag_plan_switch(
            &mut self,
            stamp: Stamp,
            outcome: &'static str,
        ) {
            let identity = self.engine.active_plan.identity;
            if let (Some(start), Some(recorder)) = (stamp, self.diagnostics.recorder_mut()) {
                recorder.record_plan_switch(start, outcome, identity.id.0, identity.generation);
            }
        }

        pub(in crate::dataflow::execution::monitor_execution) fn diag_tier_activation(
            &mut self,
            stamp: Stamp,
        ) {
            let Some(start) = stamp else {
                return;
            };
            #[cfg(feature = "jit")]
            let outcome = self.engine.jit.report().map(|report| {
                json!({
                    "plan": format!("{:?}", report.plan()),
                    "compiled_artifacts": report.compiled_artifacts(),
                    "backend_error": report.backend_error(),
                })
            });
            #[cfg(not(feature = "jit"))]
            let outcome: Option<serde_json::Value> = None;
            if let Some(recorder) = self.diagnostics.recorder_mut() {
                recorder.record_tier_activation(start, || outcome.unwrap_or_else(|| json!({})));
            }
        }

        pub(in crate::dataflow::execution::monitor_execution) fn diag_event(
            &mut self,
            name: &'static str,
            args: serde_json::Value,
        ) {
            if let Some(recorder) = self.diagnostics.recorder_mut() {
                recorder.event(name, || args);
            }
        }

        /// The current dependency slots of every `dynamic`/`defer` body of `stream`.
        pub(in crate::dataflow) fn active_dependency_slots(
            &self,
            stream: usize,
        ) -> Vec<EnvironmentSlot> {
            let evaluator = &self.evaluators.evaluators[stream];
            self.expression_locations
                .iter()
                .filter(|location| location.stream.index() == stream)
                .flat_map(|location| evaluator.expression_dependency_slots(location.node))
                .copied()
                .collect()
        }

        /// Native JIT state as JSON, when native execution is configured.
        pub(in crate::dataflow) fn jit_summary(&self) -> Option<serde_json::Value> {
            #[cfg(feature = "jit")]
            {
                self.engine.jit.report().map(|report| {
                    json!({
                        "plan": format!("{:?}", report.plan()),
                        "compiled_artifacts": report.compiled_artifacts(),
                        "unsupported_streams": report.unsupported_streams(),
                        "complete_temporal_kernel_streams": report.complete_temporal_kernel_streams(),
                        "backend_error": report.backend_error(),
                    })
                })
            }
            #[cfg(not(feature = "jit"))]
            {
                None
            }
        }

        /// Charge evaluator, region, plan and cache storage to `visitor`. Read-only: natively owned
        /// state is not materialized, and is reported as opaque instead.
        pub(in crate::dataflow) fn visit_memory<'a>(
            &'a self,
            visitor: &mut MemoryVisitor<'a>,
            stream_names: &[String],
        ) {
            let whole_native = matches!(self.authoritative_tier, AuthoritativeTier::WholeNative);
            let mut native_streams = vec![whole_native; self.evaluators.evaluators.len()];
            if let Some(regions) = &self.evaluators.region_states {
                for (index, authority) in regions.authority.iter().enumerate() {
                    if matches!(authority, RegionAuthority::Native)
                        && let Some(region) = self.engine.active_plan.regions.get(index)
                    {
                        match region {
                            ScalarRegion::Streams(streams) => {
                                for member in streams.iter() {
                                    native_streams[member.stream.index()] = true;
                                }
                            }
                            ScalarRegion::Graph(graph) => {
                                native_streams[graph.stream.index()] = true
                            }
                        }
                    }
                }
            }
            for (index, evaluator) in self.evaluators.evaluators.iter().enumerate() {
                let name = stream_names
                    .get(index)
                    .map_or_else(|| format!("stream {index}"), Clone::clone);
                visitor.stream_evaluator(&name, evaluator, native_streams[index]);
            }
            visitor.push(
                "execution/published_scalars",
                "scratch",
                None,
                MemoryEstimate::fixed(
                    self.evaluators.published_scalars.len()
                        * std::mem::size_of::<
                            Option<crate::dataflow::execution::quickening::ScalarValue>,
                        >(),
                ),
            );
            if let Some(regions) = &self.evaluators.region_states {
                let bytes = regions
                    .states
                    .iter()
                    .map(|state| state.diagnostic_bytes())
                    .sum::<usize>();
                visitor.push(
                    "execution/quickened_regions",
                    "accelerator",
                    None,
                    MemoryEstimate::fixed(bytes),
                );
            }
            let plan_bytes = |plan: &ExecutionPlan| {
                std::mem::size_of::<ExecutionPlan>()
                    + (plan.source_steps.len() + plan.main_steps.len())
                        * std::mem::size_of::<ExecutionStep>()
                    + plan.semantic.streams.len()
                        * std::mem::size_of::<
                            crate::dataflow::execution::scheduled_plan::PlannedStream,
                        >()
                    + plan.regions.len() * std::mem::size_of::<ScalarRegion>()
            };
            let cached = self
                .engine
                .cached_plans
                .iter()
                .map(plan_bytes)
                .sum::<usize>();
            visitor.push(
                format!(
                    "program/execution_plans ({} cached)",
                    self.engine.cached_plans.len()
                ),
                "program",
                None,
                MemoryEstimate::fixed(plan_bytes(&self.engine.active_plan) + cached),
            );
            visitor.push(
                format!(
                    "program/dynamic_template_cache ({} shared entries)",
                    self.shared_reconfigurable_expression_cache.entry_count()
                ),
                "program",
                None,
                MemoryEstimate::default(),
            );
            if whole_native || native_streams.iter().any(|native| *native) {
                visitor.push_opaque(
                    "execution/native_artifacts",
                    "accelerator",
                    None,
                    MemoryEstimate::default(),
                    "native code and native-owned temporal state are not inspected",
                );
            }
        }
    }
}

#[cfg(not(feature = "diagnostics"))]
impl MonitorExecution {
    #[inline(always)]
    pub(super) fn observe_active_plan(&mut self) {}

    #[inline(always)]
    pub(super) fn diag_plan_switch(&mut self, _stamp: Stamp, _outcome: &'static str) {}

    #[inline(always)]
    pub(super) fn diag_tier_activation(&mut self, _stamp: Stamp) {}
}

impl MonitorExecution {
    #[inline(always)]
    pub(in crate::dataflow) fn probe(&self) -> &ExecutionProbe {
        &self.diagnostics
    }

    /// Move the probe out, leaving a disabled one, so a replacement execution can adopt it.
    #[inline(always)]
    pub(in crate::dataflow) fn take_probe(&mut self) -> ExecutionProbe {
        self.diagnostics.take()
    }

    #[inline(always)]
    pub(in crate::dataflow) fn adopt_probe(&mut self, probe: ExecutionProbe) {
        self.diagnostics = probe;
    }
}
