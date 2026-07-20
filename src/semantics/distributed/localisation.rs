//! Localisation and auxiliary-expression expansion for distributed DSRV specifications.
//!
//! Transformations build new expression storage with [`ExprBuilder`] rather
//! than mutating trees that may be shared by several specification roots.

use static_assertions::assert_obj_safe;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Debug;

use tracing::debug;

use crate::lang::dsrv::ast::{DsrvSpecification, Expr, ExprBuilder, ExprId, ExprRef};

use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};
use crate::{ExprKind, VarName};

pub trait LocalitySpec: Debug {
    fn local_vars(&self) -> Vec<VarName>;
}

assert_obj_safe!(LocalitySpec);

impl LocalitySpec for Vec<VarName> {
    fn local_vars(&self) -> Vec<VarName> {
        self.clone()
    }
}
impl<W: Debug> LocalitySpec for (NodeName, &GenericLabelledDistributionGraph<W>) {
    /// Returns the local variables of the node.
    /// Panics if the node does not exist in the graph.
    fn local_vars(&self) -> Vec<VarName> {
        let node_index = self.1.get_node_index_by_name(&self.0).unwrap();
        self.1
            .monitors_at_node(node_index)
            .unwrap_or_else(|| panic!("Node index {:?} does not exist in the graph", node_index))
            .clone()
    }
}
impl<W: Debug + Clone> LocalitySpec for (NodeName, GenericLabelledDistributionGraph<W>) {
    fn local_vars(&self) -> Vec<VarName> {
        (self.0.clone(), &self.1).local_vars()
    }
}

impl LocalitySpec for Box<dyn LocalitySpec> {
    fn local_vars(&self) -> Vec<VarName> {
        self.as_ref().local_vars()
    }
}

pub trait Localisable {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self;
}

fn copy_inlining_aux(
    expression: ExprRef<'_>,
    spec: &DsrvSpecification,
    aux_vars: &BTreeSet<VarName>,
    visiting: &mut BTreeSet<VarName>,
    target: &mut ExprBuilder,
) -> ExprId {
    if let ExprKind::Var(var) = expression.kind() {
        if aux_vars.contains(var) {
            assert!(
                visiting.insert(var.clone()),
                "Recursive/cyclic aux definition detected while localising at aux variable {var:?}",
            );
            let expanded_expr = spec
                .exprs
                .get(var)
                .unwrap_or_else(|| panic!("Aux variable {var:?} does not have a definition"));
            let expanded =
                copy_inlining_aux(expanded_expr.as_ref(), spec, aux_vars, visiting, target);
            visiting.remove(var);
            return expanded;
        }
    }

    if matches!(expression.kind(), ExprKind::MonitoredAt(var, _) if aux_vars.contains(var)) {
        panic!("Localisation of monitored_at expression with aux variable is not allowed");
    }
    if matches!(expression.kind(), ExprKind::Dist(_, _)) {
        unimplemented!("Dist currently unsupported");
    }

    let node = contiguous_tree::map_child_ids(expression.kind(), |child| {
        copy_inlining_aux(expression.child(child), spec, aux_vars, visiting, target)
    });
    target.alloc(node, expression.span())
}

fn dependency_closure(
    spec: &DsrvSpecification,
    roots: impl IntoIterator<Item = VarName>,
) -> BTreeSet<VarName> {
    let mut reachable = BTreeSet::new();
    let mut pending = roots.into_iter().collect::<Vec<_>>();
    while let Some(var) = pending.pop() {
        if !reachable.insert(var.clone()) {
            continue;
        }
        if let Some(root) = spec.exprs.get(&var) {
            root.visit_stream_dependencies(|dependency| pending.push(dependency.clone()));
        }
    }
    reachable
}

fn prune_to_dependency_closure(
    mut spec: DsrvSpecification,
    roots: &[VarName],
) -> DsrvSpecification {
    let reachable = dependency_closure(&spec, roots.iter().cloned());
    let root_set = roots.iter().cloned().collect::<BTreeSet<_>>();
    spec.output_vars.retain(|var| root_set.contains(var));
    spec.aux_vars.retain(|var| reachable.contains(var));
    spec.exprs.retain(|var, _| reachable.contains(var));
    spec.type_annotations
        .retain(|var, _| reachable.contains(var));
    spec
}

fn inline_aux(spec: DsrvSpecification) -> DsrvSpecification {
    let aux_vars = spec.aux_vars.clone();
    for aux in &aux_vars {
        assert!(
            spec.exprs.contains_key(aux),
            "Aux variable {aux:?} does not have a definition"
        );
    }

    let capacity = spec.exprs.values().map(|expr| expr.arena().len()).sum();
    let mut builder = ExprBuilder::with_capacity(capacity);
    let mut exprs = BTreeMap::new();
    for (var, root) in &spec.exprs {
        if aux_vars.contains(var) {
            continue;
        }
        let root = copy_inlining_aux(
            root.as_ref(),
            &spec,
            &aux_vars,
            &mut BTreeSet::new(),
            &mut builder,
        );
        exprs.insert(var.clone(), root);
    }

    let output_vars = spec.output_vars.difference(&aux_vars).cloned().collect();
    let type_annotations = spec
        .type_annotations
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();

    DsrvSpecification::from_arena(
        spec.input_vars,
        output_vars,
        builder.finish_arena(),
        exprs,
        type_annotations,
        std::iter::empty(),
    )
}

impl Localisable for DsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        let local_vars = locality_spec.local_vars();
        let spec = inline_aux(prune_to_dependency_closure(self.clone(), &local_vars));
        let local_set = local_vars.into_iter().collect::<BTreeSet<_>>();

        let output_vars = spec
            .output_vars
            .intersection(&local_set)
            .cloned()
            .collect::<BTreeSet<_>>();
        let aux_vars = spec
            .aux_vars
            .intersection(&local_set)
            .cloned()
            .collect::<BTreeSet<_>>();
        let exprs = spec
            .exprs
            .iter()
            .filter(|(var, _)| local_set.contains(*var))
            .map(|(var, root)| (var.clone(), root.clone()))
            .collect::<BTreeMap<_, _>>();

        let needed_inputs = exprs
            .values()
            .flat_map(Expr::stream_dependencies)
            .collect::<HashSet<_>>();
        let input_vars = spec
            .input_vars
            .iter()
            .chain(
                self.output_vars
                    .iter()
                    .chain(self.aux_vars.iter())
                    .filter(|var| !local_set.contains(*var)),
            )
            .filter(|var| needed_inputs.contains(*var))
            .cloned()
            .collect();

        debug!("Local expression inputs: {:?}", needed_inputs);
        DsrvSpecification::new(
            input_vars,
            output_vars,
            exprs,
            spec.type_annotations.clone(),
            aux_vars,
        )
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use crate::dsrv_fixtures::spec_simple_add_decomposable;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::span::strip_span;
    use crate::{dsrv_specification, sexpr};
    use proptest::prelude::*;
    use test_log::test;
    use winnow::Parser;

    use super::*;
    use crate::lang::dsrv::ast::generation::arb_boolean_dsrv_spec;

    fn assert_specs_eq_ignoring_spans(actual: &DsrvSpecification, expected: &DsrvSpecification) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        assert_eq!(actual.type_annotations, expected.type_annotations);

        let actual_exprs = actual
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span(expr)))
            .collect::<BTreeMap<_, _>>();
        let expected_exprs = expected
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span(expr)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_exprs, expected_exprs);
    }

    #[test]
    fn test_localise_specification_1() {
        let spec = DsrvSpecification::new(
            BTreeSet::from(["a".into(), "b".into()]),
            BTreeSet::from(["c".into(), "d".into(), "e".into()]),
            vec![
                ("c".into(), sexpr!(Var("a"))),
                ("d".into(), sexpr!(Not(Var("a")))),
                ("e".into(), sexpr!(Not(Var("d")))),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec!["c".into(), "e".into()];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            DsrvSpecification::new(
                BTreeSet::from(["a".into(), "d".into()]),
                BTreeSet::from(["c".into(), "e".into()]),
                vec![
                    ("c".into(), sexpr!(Var("a"))),
                    ("e".into(), sexpr!(Not(Var("d")))),
                ]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_2() {
        let spec = DsrvSpecification::new(
            BTreeSet::from(["a".into()]),
            BTreeSet::from(["i".into()]),
            BTreeMap::<VarName, Expr>::new(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec![];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            DsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, Expr>::new(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = dsrv_specification
            .parse(spec_simple_add_decomposable())
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &DsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![("w".into(), sexpr!(BinOp(Var("x"), Add, Var("y"))))]
                    .into_iter()
                    .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &DsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![("v".into(), sexpr!(BinOp(Var("z"), Add, Var("w"))))]
                    .into_iter()
                    .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_localise_spec_with_aux() {
        // Tests that localisation correctly handles auxiliary variables
        // Note that these must be specified similarly to output variables
        let spec = dsrv_specification
            .parse(
                "   in x
                    in y
                    in z
                    out w
                    out v
                    aux tmp
                    w = x + y
                    tmp = z + w
                    v = tmp",
            )
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &DsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![("w".into(), sexpr!(BinOp(Var("x"), Add, Var("y"))))]
                    .into_iter()
                    .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &DsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![("v".into(), sexpr!(BinOp(Var("z"), Add, Var("w"))))]
                    .into_iter()
                    .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_inline_aux_single_aux() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();
        let tmp: VarName = "tmp".into();
        let z: VarName = "z".into();

        let spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from([tmp.clone(), z.clone()]),
            vec![
                (
                    tmp.clone(),
                    sexpr!(BinOp(Var(x.clone()), Add, Var(y.clone()))),
                ),
                (
                    z.clone(),
                    sexpr!(BinOp(Var(tmp.clone()), Mul, Var(x.clone()))),
                ),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![tmp.clone()],
        );

        let result = inline_aux(spec);

        let expected_exprs = vec![(
            z.clone(),
            sexpr!(BinOp(
                BinOp(Var(x.clone()), Add, Var(y.clone())),
                Mul,
                Var(x.clone())
            )),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            DsrvSpecification::new(
                BTreeSet::from([x, y]),
                BTreeSet::from([z]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    fn test_inline_aux_transitive_chain() {
        let i: VarName = "i".into();
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let h3: VarName = "h3".into();
        let out: VarName = "out".into();

        let spec = DsrvSpecification::new(
            BTreeSet::from([i.clone()]),
            BTreeSet::from([h1.clone(), h2.clone(), h3.clone(), out.clone()]),
            vec![
                (h1.clone(), sexpr!(Var(i.clone()))),
                (h2.clone(), sexpr!(BinOp(Var(h1.clone()), Add, Val(1)))),
                (h3.clone(), sexpr!(BinOp(Var(h2.clone()), Add, Val(2)))),
                (out.clone(), sexpr!(Var(h3.clone()))),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1.clone(), h2.clone(), h3.clone()],
        );

        let result = inline_aux(spec);

        // Auxiliary definitions are expanded transitively into the output expression.
        let expected_exprs = vec![(
            out.clone(),
            sexpr!(BinOp(BinOp(Var(i.clone()), Add, Val(1)), Add, Val(2))),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            DsrvSpecification::new(
                BTreeSet::from([i]),
                BTreeSet::from([out]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    #[should_panic(expected = "Recursive/cyclic aux definition detected")]
    fn test_inline_aux_cycle_panics() {
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let out: VarName = "out".into();

        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([h1.clone(), h2.clone(), out.clone()]),
            vec![
                (h1.clone(), sexpr!(Var(h2.clone()))),
                (h2.clone(), sexpr!(Var(h1.clone()))),
                (out.clone(), sexpr!(Var(h1.clone()))),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1, h2],
        );

        let _ = inline_aux(spec);
    }

    proptest! {
        #[test]
        fn test_localise_specification_prop(
            spec in arb_boolean_dsrv_spec(),
            restricted_vars in prop::collection::hash_set("[a-z]", 0..5)
        ) {
            let restricted_vars: Vec<VarName> = restricted_vars.into_iter().map(|s| s.into()).collect();
            let localised_spec = spec.localise(&restricted_vars);

            for var in localised_spec.output_vars.iter() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(spec.exprs.contains_key(var));
            }
            for var in localised_spec.input_vars.iter() {
                assert!(spec.input_vars.contains(var)
                    || spec.output_vars.contains(var));
            }
        }
    }
}
