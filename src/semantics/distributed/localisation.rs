//! Localisation and auxiliary-expression expansion for distributed DSRV specifications.

use static_assertions::assert_obj_safe;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Debug};

use tracing::debug;

use crate::lang::dsrv::ast::{DsrvSpecification, ExprView, RewriteForestError, rewrite_forest};
use crate::lang::dsrv::span::Span;

use crate::VarName;
use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};

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

/// A failure while localising a DSRV specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DsrvLocalisationError {
    MissingAuxDefinition { variable: VarName },
    MonitoredAtAux { variable: VarName, node: NodeName },
    Dist,
    CyclicReplacement { replacement_spans: Vec<Span> },
}

impl fmt::Display for DsrvLocalisationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuxDefinition { variable } => {
                write!(
                    formatter,
                    "aux variable `{variable}` does not have a definition"
                )
            }
            Self::MonitoredAtAux { variable, node } => write!(
                formatter,
                "localisation of monitored_at({variable}, {node}) is not allowed because `{variable}` is an aux variable"
            ),
            Self::Dist => formatter.write_str("dist(...) is unsupported during DSRV localisation"),
            Self::CyclicReplacement { replacement_spans } => write!(
                formatter,
                "cyclic aux replacement expansion through spans {replacement_spans:?}"
            ),
        }
    }
}

impl std::error::Error for DsrvLocalisationError {}

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
            // Placement references are not runtime stream dependencies, but aux variables used in
            // monitored_at must remain visible so localisation can reject them structurally.
            root.visit_free_variables(|dependency| {
                if spec.aux_vars.contains(dependency) {
                    pending.push(dependency.clone());
                }
            });
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

fn try_inline_aux(spec: DsrvSpecification) -> Result<DsrvSpecification, DsrvLocalisationError> {
    let aux_vars = spec.aux_vars.clone();
    for aux in &aux_vars {
        if !spec.exprs.contains_key(aux) {
            return Err(DsrvLocalisationError::MissingAuxDefinition {
                variable: aux.clone(),
            });
        }
    }

    let roots = spec
        .roots()
        .filter(|(var, _)| spec.output_vars.contains(*var) && !aux_vars.contains(*var))
        .map(|(var, root)| (var.clone(), root))
        .collect::<BTreeMap<_, _>>();
    let compact_forest = rewrite_forest(roots, |expression| match expression.view() {
        ExprView::Var(var) if aux_vars.contains(var) => Ok(spec.var_expr_ref(var)),
        ExprView::MonitoredAt(var, node) if aux_vars.contains(var) => {
            Err(DsrvLocalisationError::MonitoredAtAux {
                variable: var.clone(),
                node: node.clone(),
            })
        }
        ExprView::Dist(_, _) => Err(DsrvLocalisationError::Dist),
        _ => Ok(None),
    })
    .map_err(|error| match error {
        RewriteForestError::Policy(error) => error,
        RewriteForestError::ReplacementCycle { replacement_spans } => {
            DsrvLocalisationError::CyclicReplacement { replacement_spans }
        }
    })?;
    let (arena, exprs) = compact_forest.into_arena_and_roots();

    let output_vars = spec.output_vars.difference(&aux_vars).cloned().collect();
    let type_annotations = spec
        .type_annotations
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();

    Ok(DsrvSpecification::from_arena(
        spec.input_vars,
        output_vars,
        arena,
        exprs,
        type_annotations,
        std::iter::empty(),
    ))
}

#[cfg(test)]
fn inline_aux(spec: DsrvSpecification) -> DsrvSpecification {
    try_inline_aux(spec).unwrap_or_else(|error| panic!("Failed to inline aux variables: {error}"))
}

fn finish_localisation(
    mut spec: DsrvSpecification,
    original: &DsrvSpecification,
    local_set: &BTreeSet<VarName>,
) -> DsrvSpecification {
    debug_assert!(spec.exprs.keys().all(|var| local_set.contains(var)));
    let needed_inputs = spec
        .exprs
        .values()
        .flat_map(|expression| expression.stream_dependencies())
        .collect::<HashSet<_>>();
    spec.input_vars = original
        .input_vars
        .iter()
        .chain(
            original
                .output_vars
                .iter()
                .chain(original.aux_vars.iter())
                .filter(|var| !local_set.contains(*var)),
        )
        .filter(|var| needed_inputs.contains(*var))
        .cloned()
        .collect();
    spec.output_vars.retain(|var| local_set.contains(var));
    spec.aux_vars.retain(|var| local_set.contains(var));
    spec.stream_vars = spec
        .output_vars
        .iter()
        .cloned()
        .chain(spec.aux_vars.iter().cloned())
        .collect();

    debug!("Local expression inputs: {:?}", needed_inputs);
    spec
}

impl DsrvSpecification {
    /// Localise this specification without panicking on unsupported aux expansion.
    pub fn try_localise(
        &self,
        locality_spec: &impl LocalitySpec,
    ) -> Result<Self, DsrvLocalisationError> {
        let local_vars = locality_spec.local_vars();
        let spec = try_inline_aux(prune_to_dependency_closure(self.clone(), &local_vars))?;
        let local_set = local_vars.into_iter().collect::<BTreeSet<_>>();
        Ok(finish_localisation(spec, self, &local_set))
    }
}

impl Localisable for DsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        self.try_localise(locality_spec)
            .unwrap_or_else(|error| panic!("Failed to localise DSRV specification: {error}"))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use crate::dsrv_fixtures::spec_simple_add_decomposable;
    use crate::lang::dsrv::ast::{Expr, TreeCursorExt};
    use crate::lang::dsrv::span::strip_span;
    use crate::sexpr;
    use proptest::prelude::*;
    use test_log::test;

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
        let spec = crate::lang::dsrv::parser::parse_str(spec_simple_add_decomposable())
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
        let spec = crate::lang::dsrv::parser::parse_str(
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
    #[should_panic(expected = "cyclic aux replacement expansion")]
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

    #[test]
    fn try_localise_reports_missing_aux_definition() {
        let missing: VarName = "missing".into();
        let output: VarName = "output".into();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([missing.clone(), output.clone()]),
            BTreeMap::from([(output.clone(), sexpr!(Var(missing.clone())))]),
            BTreeMap::new(),
            [missing.clone()],
        );

        assert_eq!(
            spec.try_localise(&vec![output]).unwrap_err(),
            DsrvLocalisationError::MissingAuxDefinition { variable: missing }
        );
    }

    #[test]
    fn try_localise_reports_monitored_at_aux() {
        let helper: VarName = "helper".into();
        let output: VarName = "output".into();
        let spec = crate::lang::dsrv::parser::parse_str(
            "aux helper\nout output\nhelper = true\noutput = monitored_at(helper, A)",
        )
        .expect("spec should parse");

        assert!(matches!(
            spec.try_localise(&vec![output]),
            Err(DsrvLocalisationError::MonitoredAtAux { variable, .. }) if variable == helper
        ));
    }

    #[test]
    fn try_localise_reports_dist() {
        let output: VarName = "output".into();
        let spec = crate::lang::dsrv::parser::parse_str("out output\noutput = dist(A, B)")
            .expect("spec should parse");

        assert_eq!(
            spec.try_localise(&vec![output]).unwrap_err(),
            DsrvLocalisationError::Dist
        );
    }

    #[test]
    fn try_localise_reports_cyclic_aux_replacement() {
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let output: VarName = "output".into();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([first.clone(), second.clone(), output.clone()]),
            BTreeMap::from([
                (first.clone(), sexpr!(Var(second.clone()))),
                (second.clone(), sexpr!(Var(first.clone()))),
                (output.clone(), sexpr!(Var(first.clone()))),
            ]),
            BTreeMap::new(),
            [first, second],
        );

        assert!(matches!(
            spec.try_localise(&vec![output]),
            Err(DsrvLocalisationError::CyclicReplacement { .. })
        ));
    }

    #[test]
    fn localisation_finalisation_preserves_the_compact_rewrite_arena() {
        let helper: VarName = "helper".into();
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let spec = DsrvSpecification::new(
            BTreeSet::from(["input".into()]),
            BTreeSet::from([helper.clone(), first.clone(), second.clone()]),
            BTreeMap::from([
                (helper.clone(), sexpr!(Not(Var("input")))),
                (first.clone(), sexpr!(Var(helper.clone()))),
                (
                    second.clone(),
                    sexpr!(BinOp(Var(helper.clone()), And, Val(true))),
                ),
            ]),
            BTreeMap::new(),
            [helper],
        );

        let local_set = BTreeSet::from([first, second]);
        let pruned = prune_to_dependency_closure(
            spec.clone(),
            &local_set.iter().cloned().collect::<Vec<_>>(),
        );
        let rewritten = try_inline_aux(pruned).unwrap();
        let rewrite_arena = rewritten
            .exprs()
            .values()
            .next()
            .expect("local roots should exist")
            .arena() as *const _ as usize;

        let localised = finish_localisation(rewritten, &spec, &local_set);
        let final_arena = localised
            .exprs()
            .values()
            .next()
            .expect("local roots should exist")
            .arena() as *const _ as usize;
        assert_eq!(
            final_arena, rewrite_arena,
            "finalisation must not rebuild syntax"
        );

        let reachable_nodes = localised
            .exprs()
            .values()
            .map(|root| root.as_ref().postorder().len())
            .sum::<usize>();
        assert_eq!(localised.arena_len(), reachable_nodes);
        let roots = localised.exprs().values().collect::<Vec<_>>();
        assert!(
            roots
                .windows(2)
                .all(|roots| roots[0].shares_storage_with(roots[1]))
        );
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
