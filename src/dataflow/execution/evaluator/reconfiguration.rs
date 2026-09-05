use super::super::super::ir::{NodeId, ReconfigurableExpressionKind, StreamOp};
use super::super::super::{ContextTransferPolicy, StreamMapping};
use super::super::evaluator_state::{ActiveExpression, ReconfigurableExpressionState};
use super::super::reconfigurable_expressions::{
    ReconfigurableExpressionActivation, SharedReconfigurableExpressionCache,
    prepare_active_expression_with_change,
};
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
    fn take_from(expression: &mut ReconfigurableExpressionState) -> Self {
        Self {
            active_expression: expression
                .active_expression
                .take()
                .expect("an active dynamic body must have a donor"),
            environment_values: std::mem::take(&mut expression.environment_values),
            last_source_value: expression.last_source_value.take(),
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
            && self.canonical.validate_exact_rewrite(
                source.canonical.as_ref(),
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
        self.move_exact_state(source);
    }

    /// Moves the canonical arena without cloning it, then clears the per-tick node values.
    fn move_exact_state(&mut self, source: &mut Self) {
        std::mem::swap(&mut self.canonical, &mut source.canonical);
        self.canonical.node_values.fill(Value::NoVal);
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
                    && self.canonical.as_ref().validate_context_mapping(
                        source.canonical.as_ref(),
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
                    && self.canonical.as_ref().validate_context_rewrite(
                        source.canonical.as_ref(),
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
        self.move_exact_state(source);
    }

    /// Reconfigure one nested expression atomically at its source barrier.
    #[inline]
    pub(in crate::dataflow) fn reconfigure_expression(
        &mut self,
        node: NodeId,
        source_value: Value,
        transfer: ContextTransferPolicy,
        shared_template_cache: &mut SharedReconfigurableExpressionCache,
    ) -> Result<(ReconfigurableExpressionActivation, bool), DataflowEvaluationError> {
        let program = &self.program;
        let StreamOp::Reconfigurable(spec) = &program.graph.nodes[node.index()] else {
            unreachable!("reconfigurable expression referenced a non-reconfigurable node")
        };
        let source_text = match source_value {
            Value::Str(source_text) => source_text,
            Value::Deferred | Value::NoVal => {
                return Ok((ReconfigurableExpressionActivation::Unchanged, false));
            }
            other => {
                return Err(DataflowEvaluationError::InvalidExpressionSource(
                    other.to_string(),
                ));
            }
        };

        let (had_active_expression, prepared) = {
            let expression = self
                .canonical
                .as_mut()
                .reconfigurable_expression_state_mut(node);
            let had_active_expression = expression.active_expression.is_some();
            let previous_dependency_slots = expression
                .active_expression
                .as_ref()
                .map_or(&[][..], |active| {
                    active.environment_projection.outer_dependency_slots()
                });
            let prepared = prepare_active_expression_with_change(
                source_text,
                spec,
                &expression.template_cache,
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
                .canonical
                .as_ref()
                .reconfigurable_expression_state(node)
                .active_expression
                .as_ref()
                .expect("an active dynamic body must have a donor");
            PreparedNestedBodyTransfer::between(&active_expression, source)
        } else {
            None
        };

        let (mut donor, last_defer_result) = {
            let expression = self
                .canonical
                .as_mut()
                .reconfigurable_expression_state_mut(node);
            let donor = if has_donor {
                Some(DynamicBodyDonor::take_from(expression))
            } else {
                expression.active_expression = None;
                expression.environment_values.clear();
                expression.last_source_value = None;
                None
            };
            (donor, expression.last_defer_result.take())
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
        let expression = self
            .canonical
            .as_mut()
            .reconfigurable_expression_state_mut(node);
        shared_template_cache.insert(
            spec,
            &program.environment_layout,
            Rc::clone(&active_expression.template),
        );
        expression.cache_template(Rc::clone(&active_expression.template));
        expression.active_expression = Some(active_expression);
        expression.last_source_value = last_source_value;
        expression.last_defer_result =
            if spec.kind == ReconfigurableExpressionKind::Deferred && !had_active_expression {
                None
            } else {
                last_defer_result
            };
        expression.environment_values = environment_values;
        Ok((activation, state_preserved))
    }
}
