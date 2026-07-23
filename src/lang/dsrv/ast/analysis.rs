//! Semantic analyses over DSRV expression trees.

use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::TreeCursorExt;

use super::{Expr, ExprRef, ExprView};
use crate::core::{StreamType, VarName};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DependencyKind {
    Stream,
    Placement,
}

enum DependencyTraversalEvent<'arena> {
    Expression(ExprRef<'arena>),
    LeaveBindings(&'arena [(VarName, StreamType)]),
}

impl Expr {
    /// Syntactic variable occurrences, including lexically bound variables.
    pub fn variable_references(&self) -> impl Iterator<Item = &VarName> {
        self.as_ref().variable_references()
    }

    /// Variables referenced outside of any enclosing lambda binding.
    pub fn free_variables(&self) -> BTreeSet<VarName> {
        self.as_ref().free_variables()
    }

    pub fn visit_free_variables(&self, visit: impl FnMut(&VarName)) {
        self.as_ref().visit_free_variables(visit);
    }

    /// Stream values required to evaluate this expression.
    ///
    /// Unlike [`Self::free_variables`], placement references in `monitored_at`
    /// are not stream dependencies.
    pub fn stream_dependencies(&self) -> BTreeSet<VarName> {
        self.as_ref().stream_dependencies()
    }

    pub fn visit_stream_dependencies(&self, visit: impl FnMut(&VarName)) {
        self.as_ref().visit_stream_dependencies(visit);
    }
}

impl<'arena> ExprRef<'arena> {
    /// Syntactic variable occurrences, including lexically bound variables.
    pub fn variable_references(self) -> impl Iterator<Item = &'arena VarName> {
        self.postorder().filter_map(|expr| match expr.view() {
            ExprView::Var(var) => Some(var),
            _ => None,
        })
    }

    /// Variables referenced outside of any enclosing lambda binding.
    pub fn free_variables(self) -> BTreeSet<VarName> {
        let mut free = BTreeSet::new();
        self.visit_free_variables(|var| {
            free.insert(var.clone());
        });
        free
    }

    /// Visit variables referenced outside of any enclosing lambda binding.
    ///
    /// A variable is reported once per semantic occurrence; callers that need
    /// uniqueness can collect into a set without first allocating a temporary set.
    pub fn visit_free_variables(self, mut visit: impl FnMut(&VarName)) {
        self.visit_dependencies_with::<true>(|_, var| visit(var));
    }

    pub fn stream_dependencies(self) -> BTreeSet<VarName> {
        let mut dependencies = BTreeSet::new();
        self.visit_stream_dependencies(|var| {
            dependencies.insert(var.clone());
        });
        dependencies
    }

    pub fn visit_stream_dependencies(self, mut visit: impl FnMut(&VarName)) {
        self.visit_dependencies_with::<false>(|_, var| visit(var));
    }

    pub(crate) fn visit_dependencies(self, visit: impl FnMut(DependencyKind, &VarName)) {
        self.visit_dependencies_with::<true>(visit);
    }

    fn visit_dependencies_with<const INCLUDE_PLACEMENT: bool>(
        self,
        mut visit: impl FnMut(DependencyKind, &VarName),
    ) {
        use ExprView::*;

        let mut binding_depths = BTreeMap::<&VarName, usize>::new();
        let mut pending = vec![DependencyTraversalEvent::Expression(self)];

        while let Some(event) = pending.pop() {
            match event {
                DependencyTraversalEvent::Expression(expr) => match expr.view() {
                    Var(var) if !binding_depths.contains_key(var) => {
                        visit(DependencyKind::Stream, var);
                    }
                    MonitoredAt(var, _)
                        if INCLUDE_PLACEMENT && !binding_depths.contains_key(var) =>
                    {
                        visit(DependencyKind::Placement, var);
                    }
                    Dynamic(source, _, scope) | Defer(source, _, scope) => {
                        for var in scope
                            .iter()
                            .filter(|var| !binding_depths.contains_key(*var))
                        {
                            visit(DependencyKind::Stream, var);
                        }
                        pending.push(DependencyTraversalEvent::Expression(source));
                    }
                    Lambda(params, body) => {
                        for (name, _) in params {
                            *binding_depths.entry(name).or_default() += 1;
                        }
                        pending.push(DependencyTraversalEvent::LeaveBindings(params));
                        pending.push(DependencyTraversalEvent::Expression(body));
                    }
                    _ => pending.extend(
                        expr.children()
                            .rev()
                            .map(DependencyTraversalEvent::Expression),
                    ),
                },
                DependencyTraversalEvent::LeaveBindings(params) => {
                    for (name, _) in params {
                        let depth = binding_depths
                            .get_mut(name)
                            .expect("leaving an active lambda binding");
                        *depth -= 1;
                        if *depth == 0 {
                            binding_depths.remove(name);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::ast::Expr;

    #[test]
    fn free_variables_exclude_nested_and_shadowed_lambda_parameters() {
        let expr = Expr::Lambda(
            vec![("x".into(), StreamType::Int)].into(),
            Box::new(Expr::Tuple(
                vec![
                    Expr::Var("x".into()),
                    Expr::Var("free".into()),
                    Expr::Lambda(
                        vec![("x".into(), StreamType::Int)].into(),
                        Box::new(Expr::BinOp(
                            Box::new(Expr::Var("x".into())),
                            Box::new(Expr::Var("also_free".into())),
                            crate::lang::dsrv::ast::SBinOp::NOp(
                                crate::lang::dsrv::ast::NumericalBinOp::Add,
                            ),
                        )),
                    ),
                ]
                .into(),
            )),
        );

        assert_eq!(
            expr.free_variables(),
            BTreeSet::from(["also_free".into(), "free".into()])
        );
    }

    #[test]
    fn free_variables_include_semantic_runtime_and_monitor_references() {
        use ecow::eco_vec;

        let expr = Expr::Lambda(
            eco_vec![("bound".into(), StreamType::Int)],
            Box::new(Expr::Tuple(
                vec![
                    Expr::RestrictedDynamic(
                        Box::new(Expr::Val("source")),
                        crate::core::StreamTypeAscription::Ascribed(StreamType::Int),
                        eco_vec!["bound".into(), "runtime_input".into()],
                    ),
                    Expr::MonitoredAt("monitored".into(), "node".into()),
                ]
                .into(),
            )),
        );

        assert_eq!(
            expr.free_variables(),
            BTreeSet::from(["monitored".into(), "runtime_input".into()])
        );
    }

    #[test]
    fn free_variable_visitor_avoids_an_intermediate_collection() {
        let expr = Expr::Tuple(
            vec![
                Expr::Var("x".into()),
                Expr::Var("x".into()),
                Expr::Var("y".into()),
            ]
            .into(),
        );
        let mut visited = Vec::new();
        expr.visit_free_variables(|var| visited.push(var.clone()));

        assert_eq!(visited, vec!["x".into(), "x".into(), "y".into()]);
    }

    #[test]
    fn stream_dependencies_exclude_placement_references() {
        let expr = Expr::Tuple(
            vec![
                Expr::Var("input".into()),
                Expr::MonitoredAt("placed_stream".into(), "node".into()),
            ]
            .into(),
        );

        assert_eq!(expr.stream_dependencies(), BTreeSet::from(["input".into()]));
        assert_eq!(
            expr.free_variables(),
            BTreeSet::from(["input".into(), "placed_stream".into()])
        );
    }

    #[test]
    fn classified_dependencies_preserve_scopes_bindings_and_placement() {
        use crate::core::StreamTypeAscription;
        use ecow::eco_vec;

        let expr = Expr::Lambda(
            eco_vec![("bound".into(), StreamType::Int)],
            Box::new(Expr::Tuple(
                vec![
                    Expr::Var("free".into()),
                    Expr::MonitoredAt("placed".into(), "node".into()),
                    Expr::RestrictedDynamic(
                        Box::new(Expr::Var("dynamic_source".into())),
                        StreamTypeAscription::Ascribed(StreamType::Int),
                        eco_vec!["bound".into(), "dynamic_scope".into()],
                    ),
                    Expr::Defer(
                        Box::new(Expr::Var("defer_source".into())),
                        StreamTypeAscription::Ascribed(StreamType::Int),
                        eco_vec!["defer_scope".into(), "bound".into()],
                    ),
                ]
                .into(),
            )),
        );
        let mut dependencies = Vec::new();

        expr.as_ref()
            .visit_dependencies(|kind, var| dependencies.push((kind, var.clone())));

        assert_eq!(
            dependencies,
            vec![
                (DependencyKind::Stream, "free".into()),
                (DependencyKind::Placement, "placed".into()),
                (DependencyKind::Stream, "dynamic_scope".into()),
                (DependencyKind::Stream, "dynamic_source".into()),
                (DependencyKind::Stream, "defer_scope".into()),
                (DependencyKind::Stream, "defer_source".into()),
            ]
        );
    }
}
