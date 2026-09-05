//! Mutable per-tick bookkeeping for `dynamic`/`defer` activation.
//!
//! [`ExpressionActivationState`] tracks which deferred expressions have sealed, which source
//! streams are still needed by a live expression, and which releases are pending. It is the one
//! piece of the expression subsystem that changes as ticks run; the immutable descriptions it
//! reads live in [`super::monitor_plan`].
//!
//! Note the deliberate distinction from *root* reconfiguration (`super::reconfiguration`), which
//! replaces a whole monitor definition. This module never does that: it only tracks the lifecycle
//! of expression occurrences inside one fixed definition.

use super::ir::ReconfigurableExpressionKind;
use super::monitor_plan::{ReconfigurableExpressionId, ReconfigurableExpressionPlan};
use super::reconfiguration::ExpressionStateKey;
use super::stream_id::{StreamId, StreamSet};

#[derive(Clone)]
/// Which reconfigurable expressions are live, and which sources they still require.
///
/// Sources are reference counted because several expressions may share one. A `defer` that has
/// activated seals, and its prerequisite sources are released — but only through
/// [`ExpressionActivationState::apply_pending_releases`], run after a successful tick, so a tick
/// that fails cannot drop a source another expression still needs.
pub(super) struct ExpressionActivationState {
    sealed_deferred_expressions: Vec<bool>,
    pending_releases: Vec<ReconfigurableExpressionId>,
    release_prerequisites: Vec<Option<StreamSet>>,
    expression_streams: Vec<StreamId>,
    source_user_refcounts: Vec<usize>,
    live_expression_refcounts: Vec<usize>,
    resolution_streams: StreamSet,
    source_streams: StreamSet,
    source_order: Vec<StreamId>,
}

impl ExpressionActivationState {
    pub(super) fn new(plan: &ReconfigurableExpressionPlan, stream_count: usize) -> Self {
        let mut source_user_refcounts = vec![0usize; stream_count];
        let mut live_expression_refcounts = vec![0usize; stream_count];
        let mut release_prerequisites = Vec::with_capacity(plan.expressions.len());
        let mut expression_streams = Vec::with_capacity(plan.expressions.len());
        for expression in &plan.expressions {
            debug_assert_eq!(expression.id.index(), release_prerequisites.len());
            expression_streams.push(expression.stream);
            live_expression_refcounts[expression.stream.index()] += 1;
            for stream in expression.source_prerequisites.iter() {
                source_user_refcounts[stream.index()] += 1;
            }
            release_prerequisites.push(
                matches!(expression.kind, ReconfigurableExpressionKind::Deferred)
                    .then(|| expression.source_prerequisites.clone()),
            );
        }

        let resolution_streams = StreamSet::from_streams(
            live_expression_refcounts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count != 0).then(|| StreamId::new(index))),
        );
        let source_streams = StreamSet::from_streams(
            source_user_refcounts
                .iter()
                .enumerate()
                .filter_map(|(index, count)| (*count != 0).then(|| StreamId::new(index))),
        );
        debug_assert_eq!(&source_streams, plan.initial_source_streams());

        Self {
            sealed_deferred_expressions: vec![false; plan.expressions.len()],
            pending_releases: Vec::new(),
            release_prerequisites,
            expression_streams,
            source_user_refcounts,
            live_expression_refcounts,
            resolution_streams,
            source_streams,
            source_order: plan.initial_source_order().to_vec(),
        }
    }

    #[inline]
    pub(super) fn resolution_stream(&self, index: usize) -> Option<StreamId> {
        self.resolution_streams.as_slice().get(index).copied()
    }

    #[inline]
    pub(super) fn source_order(&self) -> &[StreamId] {
        &self.source_order
    }

    #[inline]
    pub(super) fn source_streams(&self) -> &StreamSet {
        &self.source_streams
    }

    #[inline]
    pub(super) fn is_sealed(&self, expression_id: ReconfigurableExpressionId) -> bool {
        self.sealed_deferred_expressions
            .get(expression_id.index())
            .copied()
            .unwrap_or(false)
    }

    pub(super) fn mark_deferred_activated(
        &mut self,
        expression_id: ReconfigurableExpressionId,
    ) -> bool {
        let index = expression_id.index();
        let Some(prerequisites) = self.release_prerequisites.get(index) else {
            return false;
        };
        if prerequisites.is_none() || self.sealed_deferred_expressions[index] {
            return false;
        }

        self.sealed_deferred_expressions[index] = true;
        self.pending_releases.push(expression_id);
        true
    }

    pub(super) fn sealed_addresses(
        &self,
        plan: &ReconfigurableExpressionPlan,
    ) -> Vec<ExpressionStateKey> {
        plan.expressions
            .iter()
            .filter(|expression| self.is_sealed(expression.id()))
            .map(|expression| expression.address().clone())
            .collect()
    }

    /// Reconstruct lifecycle-derived routing for an imported root monitor. Deferred-source release
    /// remains on the pending path below so a failed tick cannot release sources early.
    pub(super) fn restore_sealed_addresses(
        &mut self,
        plan: &ReconfigurableExpressionPlan,
        sealed: &[ExpressionStateKey],
    ) {
        for expression in &plan.expressions {
            if sealed.iter().any(|address| address == expression.address()) {
                self.mark_deferred_activated(expression.id());
            }
        }
        self.apply_pending_releases();
    }

    pub(super) fn apply_pending_releases(&mut self) -> bool {
        if self.pending_releases.is_empty() {
            return false;
        }

        let mut membership_changed = false;
        let mut resolution_membership_changed = false;
        for expression_id in self.pending_releases.drain(..) {
            let expression_stream = self.expression_streams[expression_id.index()];
            let live_count = &mut self.live_expression_refcounts[expression_stream.index()];
            debug_assert!(*live_count != 0);
            *live_count -= 1;
            resolution_membership_changed |= *live_count == 0;

            let prerequisites = self.release_prerequisites[expression_id.index()]
                .as_ref()
                .expect("only deferred expressions can have pending source releases");
            for stream in prerequisites.iter() {
                let count = &mut self.source_user_refcounts[stream.index()];
                debug_assert!(*count != 0);
                *count -= 1;
                if *count == 0 {
                    membership_changed = true;
                }
            }
        }

        if resolution_membership_changed {
            self.resolution_streams
                .streams
                .retain(|stream| self.live_expression_refcounts[stream.index()] != 0);
        }
        if membership_changed {
            self.source_streams
                .streams
                .retain(|stream| self.source_user_refcounts[stream.index()] != 0);
            self.source_order
                .retain(|stream| self.source_user_refcounts[stream.index()] != 0);
        }
        membership_changed
    }
}

#[cfg(test)]
mod tests {
    use super::super::ir::ReconfigurableExpressionKind;
    use super::super::monitor_plan::test_support::{
        reconfigurable_expression as expression, reconfigurable_expression_plan as plan,
    };
    use super::super::stream_id::StreamId;
    use super::*;

    #[test]
    fn deferred_source_release_is_delayed_until_explicitly_applied() {
        let plan = plan(
            vec![expression(
                0,
                ReconfigurableExpressionKind::Deferred,
                &[0, 1],
            )],
            2,
        );
        let mut state = ExpressionActivationState::new(&plan, 2);
        let expression_id = plan.expressions()[0].id;

        assert!(state.mark_deferred_activated(expression_id));
        assert!(state.is_sealed(expression_id));
        assert_eq!(state.source_streams().as_slice().len(), 2);
        assert_eq!(state.source_order().len(), 2);

        assert!(state.apply_pending_releases());
        assert!(state.source_streams().as_slice().is_empty());
        assert!(state.source_order().is_empty());
        assert!(!state.apply_pending_releases());
    }

    #[test]
    fn shared_deferred_and_dynamic_sources_use_refcounts() {
        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Deferred, &[0, 1]),
                expression(1, ReconfigurableExpressionKind::Dynamic, &[1, 2]),
            ],
            3,
        );
        let mut state = ExpressionActivationState::new(&plan, 3);

        assert!(state.mark_deferred_activated(plan.expressions()[0].id));
        assert!(state.apply_pending_releases());
        assert_eq!(
            state
                .source_streams()
                .iter()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            state
                .source_order()
                .iter()
                .copied()
                .map(StreamId::index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(!state.mark_deferred_activated(plan.expressions()[0].id));
        assert!(!state.mark_deferred_activated(plan.expressions()[1].id));
    }

    #[test]
    fn one_shared_deferred_release_does_not_change_source_membership() {
        let plan = plan(
            vec![
                expression(0, ReconfigurableExpressionKind::Deferred, &[1]),
                expression(1, ReconfigurableExpressionKind::Deferred, &[1]),
            ],
            2,
        );
        let mut state = ExpressionActivationState::new(&plan, 2);

        state.mark_deferred_activated(plan.expressions()[0].id);
        assert!(!state.apply_pending_releases());
        assert!(state.source_streams().contains(StreamId::new(1)));

        state.mark_deferred_activated(plan.expressions()[1].id);
        assert!(state.apply_pending_releases());
        assert!(!state.source_streams().contains(StreamId::new(1)));
    }
}
