use super::super::super::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use super::super::super::{ContextTransferPolicy, StreamMapping};
use super::super::dynamic_expressions::{
    DynamicExpressionActivation, SharedDynamicExpressionCache,
    prepare_active_expression_with_change,
};
use super::super::evaluator_state::{ActiveExpression, DynamicExpressionState};
use super::Evaluator;
use crate::core::Value;
use crate::dataflow::DataflowEvaluationError;
use std::rc::Rc;

struct DynamicBodyDonor {
    active_expression: ActiveExpression,
    environment_values: Vec<Value>,
    last_source_value: Option<Value>,
}

impl DynamicBodyDonor {
    fn take_from(dynamic: &mut DynamicExpressionState) -> Self {
        Self {
            active_expression: dynamic
                .active_expression
                .take()
                .expect("an active dynamic body must have a donor"),
            environment_values: std::mem::take(&mut dynamic.environment_values),
            last_source_value: dynamic.last_source_value.take(),
        }
    }
}

enum PreparedNestedBodyTransfer {
    Exact,
}

impl PreparedNestedBodyTransfer {
    fn between(target: &ActiveExpression, source: &ActiveExpression) -> Option<Self> {
        if target.evaluator.program.graph.nodes.is_empty()
            || source.evaluator.program.graph.nodes.is_empty()
        {
            return None;
        }

        if target.evaluator.program.state_key() == source.evaluator.program.state_key() {
            return target
                .evaluator
                .validate_exact_nested_transfer(&source.evaluator)
                .then_some(Self::Exact);
        }

        None
    }

    fn apply(&self, target: &mut ActiveExpression, source: &mut ActiveExpression) {
        match self {
            Self::Exact => target
                .evaluator
                .rewrite_exact_nested_from(&mut source.evaluator),
        }
    }
}

impl Evaluator {
    fn validate_exact_nested_transfer(&self, source: &Self) -> bool {
        self.program.state_key() == source.program.state_key()
            && self.tier_states.canonical.validate_exact_rewrite(
                source.tier_states.canonical.as_ref(),
                &self.program.graph,
                &source.program.graph,
                &self.program.environment_layout,
                &source.program.environment_layout,
            )
    }

    pub(super) fn rewrite_exact_nested_from(&mut self, source: &mut Self) {
        debug_assert!(
            self.validate_exact_nested_transfer(source),
            "exact nested context transfer requires a prepared mapping"
        );
        self.tier_states.move_exact_from(&mut source.tier_states);
    }

    /// Validate the structural part of an exact context transfer without changing either evaluator.
    pub(in crate::dataflow) fn validate_context_mapping(
        &self,
        source: &Self,
        mapping: &StreamMapping,
    ) -> bool {
        match mapping {
            StreamMapping::Exact(_) => {
                self.program.state_key() == source.program.state_key()
                    && self
                        .tier_states
                        .canonical
                        .as_ref()
                        .validate_context_mapping(
                            source.tier_states.canonical.as_ref(),
                            &self.program.graph,
                            &source.program.graph,
                            mapping,
                        )
            }
            StreamMapping::Unmapped => true,
        }
    }

    /// Validate the full mapped state after any native representation has been materialized.
    pub(in crate::dataflow) fn validate_context_rewrite(
        &self,
        source: &Self,
        mapping: &StreamMapping,
    ) -> bool {
        match mapping {
            StreamMapping::Exact(_) => {
                self.program.state_key() == source.program.state_key()
                    && self
                        .tier_states
                        .canonical
                        .as_ref()
                        .validate_context_rewrite(
                            source.tier_states.canonical.as_ref(),
                            &self.program.graph,
                            &source.program.graph,
                            &self.program.environment_layout,
                            &source.program.environment_layout,
                            mapping,
                        )
            }
            StreamMapping::Unmapped => true,
        }
    }

    /// Apply a root context transfer from a mapping prepared for exact stream matches.
    ///
    /// The root preparation path owns release-mode validation. Application relies on that prepared
    /// mapping and keeps only debug assertions instead of recovering from an invalid mapping.
    pub(in crate::dataflow) fn rewrite_context_from(
        &mut self,
        source: &mut Self,
        mapping: &StreamMapping,
        policy: ContextTransferPolicy,
    ) {
        if policy == ContextTransferPolicy::None {
            self.reset();
            return;
        }

        match mapping {
            StreamMapping::Unmapped => self.reset(),
            StreamMapping::Exact(_) => {
                debug_assert!(
                    self.validate_context_rewrite(source, mapping),
                    "exact root context transfer requires a prepared and validated mapping"
                );
                self.move_exact_context(source);
            }
        }
    }

    fn move_exact_context(&mut self, source: &mut Self) {
        self.tier_states.move_exact_from(&mut source.tier_states);
    }

    /// Reconfigure one nested expression atomically at its source barrier.
    #[inline]
    pub(in crate::dataflow) fn reconfigure_expression(
        &mut self,
        node: NodeId,
        source_value: Value,
        transfer: ContextTransferPolicy,
        shared_template_cache: &mut SharedDynamicExpressionCache,
    ) -> Result<(DynamicExpressionActivation, bool), DataflowEvaluationError> {
        let program = &self.program;
        let StreamOp::Dynamic(spec) = &program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-dynamic node")
        };
        let source_text = match source_value {
            Value::Str(source_text) => source_text,
            Value::Deferred | Value::NoVal => {
                return Ok((DynamicExpressionActivation::Unchanged, false));
            }
            other => {
                return Err(DataflowEvaluationError::InvalidExpressionSource(
                    other.to_string(),
                ));
            }
        };

        let (had_active_expression, prepared) = {
            let dynamic = self
                .tier_states
                .canonical
                .as_mut()
                .dynamic_expression_state_mut(node);
            let had_active_expression = dynamic.active_expression.is_some();
            let previous_dependency_slots = dynamic
                .active_expression
                .as_ref()
                .map_or(&[][..], |active| {
                    active.environment_projection.outer_dependency_slots()
                });
            let prepared = prepare_active_expression_with_change(
                source_text,
                spec,
                &dynamic.template_cache,
                Some(&*shared_template_cache),
                &program.environment_layout,
                had_active_expression,
                previous_dependency_slots,
            )?
            .expect("the source barrier requested a changed nested body");
            (had_active_expression, prepared)
        };
        let activation = prepared.activation;
        let template = prepared.template;
        let mut active_expression = ActiveExpression {
            evaluator: Evaluator::new(Rc::clone(&template.program)),
            template: Rc::clone(&template),
            environment_projection: prepared.environment_projection,
        };
        let has_donor = spec.kind == ReconfigurableExpressionKind::Dynamic && had_active_expression;
        let prepared_transfer = if has_donor && transfer != ContextTransferPolicy::None {
            let source = self
                .tier_states
                .canonical
                .as_ref()
                .dynamic_expression_state(node)
                .active_expression
                .as_ref()
                .expect("an active dynamic body must have a donor");
            PreparedNestedBodyTransfer::between(&active_expression, source)
        } else {
            None
        };

        let (mut donor, last_defer_result) = {
            let dynamic = self
                .tier_states
                .canonical
                .as_mut()
                .dynamic_expression_state_mut(node);
            let donor = if has_donor {
                Some(DynamicBodyDonor::take_from(dynamic))
            } else {
                dynamic.active_expression = None;
                dynamic.environment_values.clear();
                dynamic.last_source_value = None;
                None
            };
            (donor, dynamic.last_defer_result.take())
        };

        // The shadow allocation is reusable storage, not context. Every target-used slot is refreshed
        // from the current or retained outer environment before the body executes.
        let environment_values = donor.as_mut().map_or_else(Vec::new, |donor| {
            std::mem::take(&mut donor.environment_values)
        });
        let state_preserved = match (donor.as_mut(), prepared_transfer.as_ref()) {
            (Some(donor), Some(prepared_transfer)) => {
                prepared_transfer.apply(&mut active_expression, &mut donor.active_expression);
                true
            }
            _ => false,
        };
        let last_source_value = donor.and_then(|donor| donor.last_source_value);
        let dynamic = self
            .tier_states
            .canonical
            .as_mut()
            .dynamic_expression_state_mut(node);
        shared_template_cache.insert(
            spec,
            &program.environment_layout,
            Rc::clone(&active_expression.template),
        );
        dynamic.cache_template(Rc::clone(&active_expression.template));
        dynamic.active_expression = Some(active_expression);
        dynamic.last_source_value = last_source_value;
        dynamic.last_defer_result =
            if spec.kind == ReconfigurableExpressionKind::Deferred && !had_active_expression {
                None
            } else {
                last_defer_result
            };
        dynamic.environment_values = environment_values;
        Ok((activation, state_preserved))
    }
}
